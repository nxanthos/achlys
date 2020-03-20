-module(path_with_counters).
-export([
    schedule/1,
    get_path/2,
    debug_layer/2,
    debug_cost/1
]).

-define(INFINITE, 1000000).

% Network configuration:

% Format:
% [
%   {Destination, Source, Cost},
%   {Destination, Source, Cost},
%   ...,
% ]
get_links() ->
    [
        {a, b, 3},
        {a, d, 7},
        {b, a, 3},
        {b, c, 4},
        {b, d, 2},
        {c, b, 4},
        {c, d, 5},
        {c, e, 6},
        {d, a, 7},
        {d, b, 2},
        {d, c, 5},
        {d, e, 4},
        {e, c, 6},
        {e, d, 4}
    ].
get_links(Destination) ->
    Links = lists:filter(fun({Dst, _, _}) ->
        not (Destination == Dst)
    end, get_links()),
    [{Destination, Destination, 0}|Links].

% Format:
% [
%   {Node, [Predecessor 1, Predecessor 2, ...]},
%   {Node, [Predecessor 1, Predecessor 2, ...]},
%   ...
% ]
get_nodes(Destination) ->
    Orddict = lists:foldl(fun({Dst, Src, _}, Acc) ->
        orddict:append(Dst, Src, Acc)
    end, orddict:new(), get_links(Destination)),
    orddict:to_list(Orddict).

% Format:
% Nodes = [
%   {Node, [Predecessor 1, Predecessor 2, ...]},
%   {Node, [Predecessor 1, Predecessor 2, ...]},
%   ...
% ]
get_predecessors(Nodes, Name) ->
    Predicate = fun({Node, _}) -> Node == Name end,
    case lists:search(Predicate, Nodes) of
        {value, {_, Predecessors}} -> Predecessors;
        _ -> []
    end.

% Helpers :

get_actor() -> 
    {
        partisan_remote_reference,
        partisan_peer_service_manager:mynode(),
        partisan_util:gensym(self())
    }.

get_min(Predicate, L) ->
    case L of [H|T] ->
        lists:foldl(fun(A, B) ->
            case Predicate(A, B) of true -> A; false -> B end
        end, H, T)
    end.

% Format:
% [
%   {Value, Cost},
%   {Value, Cost},
%   ...
% ]
partition(Values) ->
    N = erlang:length(Values),
    {L1, L2} = lists:split(erlang:trunc(N / 2), Values),
    lists:zip(L1, L2).

add_connection(Values, Costs, Destination) when length(Values) == length(Costs) ->
    achlys_process:start_dag_link(
        Values ++ Costs, Destination,
        fun(Results) ->
            List = lists:map(fun({Value, Cost}) ->
                state_gcounter_ext:sum(Value, Cost)
            end, partition(Results)),
            case List of [H|T] ->
                lists:foldl(fun(A, B) ->
                    state_gcounter_ext:min(A, B, min)
                end, H, T)
            end
        end
    ).

get_node_id(Name, Level) ->
    Identifier = erlang:list_to_binary(erlang:atom_to_list(Name) ++ erlang:integer_to_list(Level)), 
    {Identifier, state_gcounter}.

get_cost_id(A, B) ->
    Identifier = case A < B of
        true ->
            erlang:atom_to_list(A) ++ erlang:atom_to_list(B);
        false ->
            erlang:atom_to_list(B) ++ erlang:atom_to_list(A)
    end,
    {erlang:list_to_binary(Identifier), state_gcounter}.

% Format:
% [
%   {{Name, Type}, Cost},
%   {{Name, Type}, Cost},
%   ...
% ]
get_initial_costs(Destination) ->
    Links = get_links(Destination),
    Set = lists:foldl(fun(Link, Acc) ->
        case Link of
            {Dst, Src, _} when Dst >= Src ->
                sets:add_element(Link, Acc);
            _ -> Acc
        end
    end, sets:new(), Links),
    lists:map(fun({Dst, Src, Cost}) ->
        ID = get_cost_id(Dst, Src),
        {ID, Cost}
    end, sets:to_list(Set)).

init_dag(Destination) ->

    Nodes = get_nodes(Destination),

    % Initialize the input value of the first layer:

    lists:foreach(fun({Node, _}) ->
        ID = get_node_id(Node, 1),
        Value = case Node of
            _ when Node == Destination -> 1;
            _ -> ?INFINITE
        end,
        lasp:update(ID, {increment, Value}, get_actor())
    end, Nodes),

    % Initialize the cost :

    lists:foreach(fun({ID, Cost}) ->
        lasp:update(ID, {increment, Cost + 1}, get_actor())
    end, get_initial_costs(Destination)),

    % Add layers :

    N = erlang:length(Nodes),
    lists:foreach(fun(K) ->
        lists:foreach(fun({Node, Predecessors}) ->
            add_connection(
                lists:map(fun(Predecessor) -> % Values
                    get_node_id(Predecessor, K)
                end, Predecessors),
                lists:map(fun(Predecessor) -> % Costs
                    get_cost_id(Node, Predecessor)
                end, Predecessors),
                get_node_id(Node, K + 1)
            )
        end, Nodes)
    end, lists:seq(1, N - 1)),
    ok.

search_path(_, _, [], _, Path) -> Path;
search_path(_, _, _, Level, Path) when Level =< 0 -> Path;
search_path(Destination, Nodes, Candidates, Level, Path) ->
    Values = lists:map(fun(Name) ->
        ID = get_node_id(Name, Level),
        {ok, Value} = lasp:query(ID),
        {Name, Value}
    end, Candidates),
    Predicate = fun({_, V1}, {_, V2}) -> V1 < V2 end,
    case get_min(Predicate, Values) of
        {_, Value} when Value >= ?INFINITE ->
            no_solution;
        {_, Value} when Value == 0 ->
            no_solution;
        {Name, Value} when Name == Destination ->
            Path ++ [{Name, Value - Level}];
        {Name, Value} ->
            NextCandidates = get_predecessors(Nodes, Name),
            search_path(
                Destination,
                Nodes,
                NextCandidates,
                Level - 1,
                Path ++ [{Name, Value - Level}]
            )
    end.

get_path(Source, Destination) ->
    Nodes = get_nodes(Destination),
    Level = erlang:length(Nodes),
    Path = search_path(Destination, Nodes, [Source], Level, []).

% Debug :

debug_layer(Destination, N) ->
    lists:foreach(fun({Name, _}) ->
        ID = get_node_id(Name, N),
        {Identifier, _} = ID,
        io:format("~p=~w~n", [Identifier, lasp:query(ID)])
    end, get_nodes(Destination)).

debug_cost(Destination) ->
    lists:foreach(fun({ID, _}) ->
        {Identifier, _} = ID,
        io:format("~p=~w~n", [Identifier, lasp:query(ID)])
    end, get_initial_costs(Destination)).

schedule(Destination) ->
    init_dag(Destination),
    timer:sleep(2000),
    % lists:foreach(fun(K) ->
    %     debug_layer(Destination, K)
    % end, lists:seq(1, N)),
    N = 5,
    debug_layer(Destination, N),
    ok.

% Example:
% path_with_counters:schedule(a).
% path_with_counters:get_path(b, a).

-module(path_with_counters).
-export([
    schedule/0
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

% Helpers :

get_actor() -> 
    {
        partisan_remote_reference,
        partisan_peer_service_manager:mynode(),
        partisan_util:gensym(self())
    }.

partition(Values) ->
    N = erlang:length(Values),
    {L1, L2} = lists:split(erlang:trunc(N / 2), Values),
    lists:zip(L1, L2).

add_connection(Inputs, Costs, Destination) when length(Inputs) == length(Costs) ->
    achlys_process:start_dag_link(
        Inputs ++ Costs, Destination,
        fun(Results) ->
            get_min(partition(Results))
        end
    ).

sum({state_pncounter, LValue}, {state_pncounter, RValue}) ->
    {state_pncounter, orddict:merge(
        fun(_, {Inc1, Dec1}, {Inc2, Dec2}) ->
            {Inc1 + Inc2, Dec1 + Dec2}
        end,
        LValue,
        RValue
    )}.

get_min(L) ->
    case L of [{I1, C1}|T] ->
        S1 = sum(I1, C1),
        N1 = state_pncounter:query(S1),
        {S3, _} = lists:foldl(fun({I2, C2}, {_, N3}=Acc) ->
            S2 = sum(I2, C2),
            N2 = state_pncounter:query(S2),
            case N2 of
                _ when N2 < N3 -> {S2, N2};
                _ -> Acc
            end
        end, {S1, N1}, T),
        S3
    end.

get_node_id(Name, Level) ->
    erlang:list_to_binary(erlang:atom_to_list(Name) ++ erlang:integer_to_list(Level)).

get_cost_id(Dst, Src) ->
    erlang:list_to_binary(erlang:atom_to_list(Dst) ++ erlang:atom_to_list(Src)).

init_dag(Destination) ->

    Type = state_pncounter,
    Nodes = get_nodes(Destination),
    Links = get_links(Destination),

    % Initialize the input value:

    lists:foreach(fun({Node, _}) ->
        Name = get_node_id(Node, 1),
        ID = {Name, Type},
        case Node of
            Destination ->
                Value = 1,
                lasp:update(ID, {increment, Value}, get_actor());
            _ ->
                Value = ?INFINITE,
                lasp:update(ID, {increment, Value}, get_actor())
        end
    end, Nodes),

    % Initialize the cost :

    lists:foreach(fun({Dst, Src, Cost}) ->
        ID = {get_cost_id(Dst, Src), Type},
        lasp:declare(ID, Type),
        lasp:update(ID, {increment, Cost + 1}, get_actor())
    end, Links),

    % Add layers :

    N = erlang:length(Nodes),
    lists:foreach(fun(K) ->
        lists:foreach(fun({Node, Predecessors}) ->
            add_connection(
                lists:map(fun(Predecessor) -> % Inputs
                    {get_node_id(Predecessor, K), Type}
                end, Predecessors),
                lists:map(fun(Predecessor) -> % Costs
                    {get_cost_id(Node, Predecessor), Type}
                end, Predecessors),
                {get_node_id(Node, K + 1), Type}
            )
        end, Nodes)
    end, lists:seq(1, N - 1)),
    ok.

% Debug :

debug_layer(Destination, N) ->
    Type = state_pncounter,
    lists:foreach(fun({Name, _}) ->
        Identifier = get_node_id(Name, N),
        io:format("~p=~w~n", [Identifier, lasp:query({Identifier, Type})])
    end, get_nodes(Destination)).

debug_cost(Destination) ->
    Type = state_pncounter,
    lists:foreach(fun({Dst, Src, _}) ->
        Identifier = get_cost_id(Dst, Src),
        io:format("~p=~w~n", [Identifier, lasp:query({Identifier, Type})])
    end, get_links(Destination)).

schedule() ->
    Destination = a,
    N = 5,
    init_dag(Destination),
    timer:sleep(2000),
    % lists:foreach(fun(K) ->
    %     debug_layer(Destination, K)
    % end, lists:seq(1, N)),
    debug_layer(Destination, N),
    ok.

% path_with_counters:schedule().

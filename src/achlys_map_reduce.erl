-module(achlys_map_reduce).
-define(SERVER, ?MODULE).
-define(TIMEOUT, 1000).

-export([
    schedule/2,
    debug/1
]).

% ---------------------------------------------
% Map phase :
% ---------------------------------------------

% @pre -
% @post -
filter_pairs(Pairs) ->
    lists:filter(fun(Pair) -> 
        case Pair of
            {_, _} -> true;
            _ -> false
        end
    end, Pairs).

% @pre -
% @post -
get_values({IVar, Map}) ->
    Values = achlys_util:query(IVar),
    lists:foldl(fun(Value, Pairs) ->
        case erlang:apply(Map, [Value]) of
            Result when erlang:is_list(Result) ->
                Pairs ++ filter_pairs(Result);
            {_, _} = Result ->
                Pairs ++ [Result]
        end
    end, [], Values).

% @pre -
% @post -
sort_pairs(Pairs) when erlang:is_list(Pairs) ->
    lists:sort(Pairs);
sort_pairs(Pair) when erlang:is_tuple(Pair) ->
    [Pair].

% @pre -
% @post -
map_phase([], _) -> 0;
map_phase(Entries, OVar) ->
    P1 = lists:foldl(fun(Entry, Values) ->
        case Entry of
            {{_, Type}, Fun} when erlang:is_atom(Type), erlang:is_function(Fun) ->
                Values ++ get_values(Entry);
            _ -> Values
        end
    end, [], Entries),
    P2 = sort_pairs(P1),
    lists:foldl(fun(Pair, K) ->
        {Key, Value} = Pair,
        lasp:update(OVar, {add, #{
            id => K,
            key => Key,
            value => Value
        }}, self()),
        K + 1
    end, 0, P2).

% ---------------------------------------------
% Shuffle phase :
% ---------------------------------------------

% @pre -
% @post -
shuffle_phase(Pairs, GetOVar) ->
    shuffle_phase(Pairs, #{}, GetOVar).
shuffle_phase([], Dispatching, _) -> Dispatching;
shuffle_phase([#{id := _, key := Key, value := _} = Pair|Pairs], Dispatching, GetOVar) ->
    OVar = GetOVar(Key),
    lasp:update(OVar, {add, Pair}, self()),
    case maps:is_key(Key, Dispatching) of
        true ->
            shuffle_phase(Pairs, maps:update_with(Key, fun(Info) ->
                maps:update_with(n, fun(N) -> N + 1 end, Info)
            end, Dispatching), GetOVar);
        false ->
            shuffle_phase(Pairs, maps:put(Key, #{
                n => 1,
                variable => OVar
            }, Dispatching), GetOVar)
    end;
shuffle_phase([_|Pairs], Dispatching, GetOVar) ->
    shuffle_phase(Pairs, Dispatching, GetOVar).

% ---------------------------------------------
% Reduce phase :
% ---------------------------------------------

% @pre -
% @post -
convert_key(Key) when erlang:is_atom(Key) ->
    erlang:atom_to_list(Key);
convert_key(Key) when erlang:is_integer(Key) ->
    erlang:integer_to_list(Key).

% @pre -
% @post -
reduce_phase(_, [], _, _) -> {0, false};
reduce_phase(Group, Pairs, Reduce, OVar) ->
    Values = [Value || #{value := Value} <- Pairs],
    P1 = erlang:apply(Reduce, [Group, Values, false]),
    P2 = filter_pairs(P1),
    P3 = sort_pairs(P2),
    N = lists:foldl(fun(Pair, K) ->
        {Key, Value} = Pair,
        lasp:update(OVar, {add, #{
            id => {Group, K},
            key => Key,
            value => Value
        }}, self()),
        K + 1
    end, 0, P3),
    P4 = sort_pairs([{Key, Value} || #{key := Key, value := Value} <- Pairs]),
    {N, not (P3 =:= P4)}.

% ---------------------------------------------
% Helpers:
% ---------------------------------------------

% @pre -
% @post -
get_total_cardinality(Info) ->
    get_total_cardinality(Info, 0).
get_total_cardinality([], Total) -> Total;
get_total_cardinality([{_, Number, _}|T], Total) ->
    get_total_cardinality(T, Number + Total).

% @pre -
% @post -
is_irreductible([]) -> true;
is_irreductible([{_, _, Flag}|T]) ->
    case Flag of
        true -> false;
        false -> is_irreductible(T)
    end.

% @pre -
% @post -
choose_node() ->
    {ok, Members} = partisan_peer_service:members(),
    Length = erlang:length(Members),
    Index = rand:uniform(Length),
    [lists:nth(Index, Members)].

% @pre -
% @post -
give_task(Key, N, Vars, Reduce) ->
    {IVar, CVar, OVar} = Vars,
    Node = choose_node(),
    Task = achlys:declare(mytask, Node, single, fun() ->
        io:format("Starting the reduction~n"),
        {ok, {_, _, _, {_, Pairs}}} = lasp:read(IVar, {cardinality, N}),
        {Cardinality, Flag} = reduce_phase(Key, Pairs, Reduce, OVar),
        lasp:update(CVar, {add,
            {Key, Cardinality, Flag}
        }, self())
    end),
    achlys:bite(Task).

% @pre -
% @post -
dispatch_tasks(Keys, Dispatching, Vars, Reduce) ->
    {CVar, OVar} = Vars,
    lists:foreach(fun(Key) ->
        case maps:get(Key, Dispatching) of #{n := N, variable := IVar} ->
            give_task(Key, N, {IVar, CVar, OVar}, Reduce)
        end
    end, Keys).

% @pre -
% @post -
await(Fun, Args) ->
    Pid = self(),
    erlang:spawn(fun() ->
        Self = self(),
        erlang:spawn(fun() ->
            Self ! erlang:apply(Fun, Args)
        end),
        receive Result ->
            Pid ! {ok, Result}
        after ?TIMEOUT ->
            Pid ! {error, timeout}
        end
    end),
    receive Result -> Result end.

% @pre -
% @post -
retry(0, _, _) -> {error, max_attempts_reached};
retry(K, Action, Callback) ->
    case await(Action, []) of
        {error, timeout} ->
            Callback(),
            retry(K - 1, Action, Callback);
        Response -> Response
    end.

% @pre -
% @post -
read(ID, N) ->
    fun() ->
        {ok, {_, _, _, {_, Values}}} = lasp:read(ID, {cardinality, N}),
        Values
    end.

% @pre -
% @post -
get_cardinality_per_key(L) ->
    get_cardinality_per_key(L, #{}).
get_cardinality_per_key([], Acc) -> Acc;
get_cardinality_per_key([{Key, Number, _}|T], Acc) ->
    get_cardinality_per_key(T, maps:put(Key, Number, Acc)).

% @pre -
% @post -
get_groups_with_missing_pairs([], Acc) ->
    maps:keys(Acc);
get_groups_with_missing_pairs([Pair|Pairs], Acc) ->
    case Pair of #{id := {Group, _}} ->
        case maps:get(Group, Acc) of
            N when N > 1 ->
                get_groups_with_missing_pairs(
                    Pairs,
                    maps:update(Group, N - 1, Acc)
                );
            N when N =< 1 ->
                get_groups_with_missing_pairs(
                    Pairs,
                    maps:remove(Group, Acc)
                )
        end
    end.

% @pre -
% @post -
round(Round, Pairs, Vars, Reduce, Options) ->

    Dispatching = shuffle_phase(Pairs, fun(Key) ->
        Name = erlang:list_to_binary(
            erlang:integer_to_list(Round) ++ convert_key(Key)
        ), 
        {Name, state_gset}
    end),

    Keys = maps:keys(Dispatching),
    dispatch_tasks(Keys, Dispatching, Vars, Reduce),
    
    % Debug :
    lists:foreach(fun(Info) ->
        case Info of {_, #{variable := ID}} -> debug(ID) end
    end, maps:to_list(Dispatching)),
    
    {CVar, OVar} = Vars,
    I = maps:size(Dispatching),

    case retry(5, read(CVar, I), fun() ->
        io:format("Resending the task~n")
    end) of
        {error, _} ->
            io:format("Error: Could not get cardinality~n"),
            [];
        {ok, Info} ->
            J = get_total_cardinality(Info),
            case retry(5, read(OVar, J), fun() ->
                io:format("Resending the task~n")
            end) of
                {error, _} ->
                    io:format("Error: Could not get the output pairs~n"),
                    [];
                {ok, Result} ->
                    case is_irreductible(Info) of
                        true ->
                            io:format("OVar = ~w Result = ~w~n", [OVar, Result]),
                            Result;
                        false ->
                            NextCVar = {erlang:list_to_binary(
                                erlang:integer_to_list(Round) ++ "-cvar"
                            ), state_gset},
                            NextOVar = {erlang:list_to_binary(
                                erlang:integer_to_list(Round) ++ "-over"
                            ), state_gset},
                            start_round(
                                Round + 1,
                                Result,
                                {NextCVar, NextOVar},
                                Reduce,
                                Options
                            )
                    end
            end
    end.

% @pre -
% @post -
start_round(Round, Pairs, Vars, Reduce, Options) ->
    case maps:get(max_round, Options) of
        Max when Round > Max -> Pairs;
        _ -> round(Round, Pairs, Vars, Reduce, Options)
    end.

% @pre -
% @post -
schedule(Entries, Reduce) ->
    IVar = {<<"0-ivar">>, state_gset},
    CVar = {<<"0-cvar">>, state_gset},
    OVar = {<<"0-ovar">>, state_gset},
    N = map_phase(Entries, IVar),
    {ok, {_, _, _, {_, Pairs}}} = lasp:read(IVar, {cardinality, N}),
    % io:format("Pairs=~p Cardinality=~p~n", [Pairs, I]),
    start_round(1, Pairs, {CVar, OVar}, Reduce, #{
        max_round => 10
    }).

% ---------------------------------------------
% Debugging:
% ---------------------------------------------

% @pre -
% @post -
debug(ID) ->
    {ok, Set} = lasp:query(ID),
    Values = sets:to_list(Set),
    io:format("Variable (~w) -> ~w~n", [ID, Values]).

-module(relational_algebra).
-export([
    sum_test/0,
    count_test/0,
    filter_test/0,
    natural_join_test/0
]).

% @pre -
% @post -
split(Values) -> split(Values, {[], [], []}).
split([], Acc) -> Acc;
split([Value|Values], Acc) ->
    {L1, L2, L3} = Acc,
    case Value of
        #{a := _, c := _} ->
            split(Values, {L1, L2, [Value|L3]});
        #{a := _} ->
            split(Values, {[Value|L1], L2, L3});
        #{c := _} ->
            split(Values, {L1, [Value|L2], L3})
    end.

% @pre -
% @post -
natural_join(Key, L1, L2) ->
    natural_join(Key, L2, L1, L2).
natural_join(_, _, [], []) -> [];
natural_join(_, _, [], _) -> [];
natural_join(Key, L2, [_|T1], []) ->
    natural_join(Key, L2, T1, L2);
natural_join(Key, L2, [H1|T1], [H2|T2]) ->
    case {H1, H2} of {
        #{a := A},
        #{c := C}
    } -> [{Key, #{a => A, c => C}}|natural_join(Key, L2, [H1|T1], T2)] end.

% @pre -
% @post -
natural_join_test() ->
    {V1, V2} = get_tables(),
    Pairs = achlys_map_reduce:schedule([
        {V1, fun(Value) -> % R1
            case Value of #{
                a := A,
                b := B
            } -> [{B, #{a => A}}] end
        end},
        {V2, fun(Value) -> % R2
            case Value of #{
                b := B,
                c := C
            } -> [{B, #{c => C}}] end
        end}
    ], fun(Key, Values) -> % Join
        {L1, L2, L3} = split(Values),
        L = natural_join(Key, L1, L2) ++ lists:map(fun(E) ->
            {Key, E}
        end, L3),
        % io:format("Reduce -> Key=~p~nValues=~p~nNatural-join=~p~n~n", [Key, Values, L]),
        % io:format("~p~n", [L]),
        L
    end),

    % Expected result:

    % | A | B | C |
    % |---|---|---|
    % | 1 | a | x |
    % | 1 | a | y |
    % | 1 | b | y |
    % | 2 | b | y |
    % | 2 | c | y |

    lists:foreach(fun(Pair) ->
        case Pair of #{key := Key, value := Value} ->
            io:format("~p~n", [maps:put(b, Key, Value)])
        end
    end, Pairs).

% @pre -
% @post -
count_test() ->
    {V1, _} = get_tables(),
    Pairs = achlys_map_reduce:schedule([
        {V1, fun(Value) -> % R1
            case Value of #{b := B, a := _} ->
                [{B, 1}]
            end
        end}
    ], fun(Key, Values) ->
        [{Key, lists:sum(Values)}]
    end),

    % Expected result:

    % | a | 1 |
    % | b | 2 |
    % | c | 1 |
    
    lists:foreach(fun(Pair) ->
        case Pair of #{key := Key, value := Value} ->
            io:format("|~p|~p|~n", [Key, Value])
        end
    end, Pairs).

% @pre -
% @post -
sum_test() ->
    {V1, _} = get_tables(),
    Pairs = achlys_map_reduce:schedule([
        {V1, fun(Value) -> % R1
            case Value of #{b := B, a := A} ->
                [{B, A}]
            end
        end}
    ], fun(Key, Values) ->
        [{Key, lists:sum(Values)}]
    end),

    % Expected result:

    % | c | 2 |
    % | a | 1 |
    % | b | 3 |

    lists:foreach(fun(Pair) ->
        case Pair of #{key := Key, value := Value} ->
            io:format("|~p|~p|~n", [Key, Value])
        end
    end, Pairs).

% @pre -
% @post -
filter_test() ->
    {V1, _} = get_tables(),
    Pairs = achlys_map_reduce:schedule([
        {V1, fun(Value) -> % R1
            case Value of #{b := B, a := A} ->
                [{B, A}]
            end
        end}
    ], fun(Key, Values) ->
        lists:foldl(fun(Value, Acc) -> 
            case Value rem 2 == 0 of
                true -> [{Key, Value}|Acc];
                false -> Acc
            end
        end, [], Values)
    end),

    % Expected result:

    % | b | 2 |
    % | c | 2 |

    lists:foreach(fun(Pair) ->
        case Pair of #{key := Key, value := Value} ->
            io:format("|~p|~p|~n", [Key, Value])
        end
    end, Pairs).

% ---------------------------------------------
% Data :
% ---------------------------------------------

% @pre -
% @post -
get_R1() -> [
    #{b => a, a => 1},
    #{b => b, a => 1},
    #{b => b, a => 2},
    #{b => c, a => 2}
].

% @pre -
% @post -
get_R2() -> [
    #{b => a, c => x},
    #{b => a, c => y},
    #{b => b, c => y},
    #{b => c, c => y}
].

% @pre -
% @post -
get_tables() ->

    V1 = {<<"R1">>, state_gset},
    V2 = {<<"R2">>, state_gset},

    R1 = get_R1(),
    lists:foreach(fun(Row) ->
        lasp:update(V1, {add, Row}, self())
    end, R1),

    R2 = get_R2(),
    lists:foreach(fun(Row) ->
        lasp:update(V2, {add, Row}, self())        
    end, R2),

    {V1, V2}.
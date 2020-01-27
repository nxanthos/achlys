-module(achlys_db).
-export([
    natural_join_test/0
]).

% @pre -
% @post -
split(Values) -> split(Values, {[], [], []}).
split([], Acc) -> Acc;
split([Value|Values], Acc) ->
    {L1, L2, L3} = Acc,
    case Value of
        #{a := _} ->
            split(Values, {[Value|L1], L2, L3});
        #{c := _} ->
            split(Values, {L1, [Value|L2], L3});
        _ ->
            split(Values, {L1, L2, [Value|L3]})
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
format_result([]) -> [];
format_result([Pair|Pairs]) ->
    case Pair of #{key := Key, value := Value} ->
        [maps:put(b, Key, Value)|format_result(Pairs)]
    end.

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

    % Expected result :

    % | A | B | C |
    % |---|---|---|
    % | 1 | a | x |
    % | 1 | a | y |
    % | 1 | b | y |
    % | 2 | b | y |
    % | 2 | c | y |

    io:format("~p~n", [format_result(Pairs)]).

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

% ---------------------------------------------
% Test :
% ---------------------------------------------

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
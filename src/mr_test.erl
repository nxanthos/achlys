-module(mr_test).
-export([
    example_1/0,
    example_2/0
]).

% mr_test:example_1().
% mr_test:example_2().

% @pre -
% @post -
example_1() ->
    
    lasp_peer_service:join('achlys1@192.168.1.6'),
    timer:sleep(500),

    IVar1 = {<<"a">>, state_gset},
    IVar2 = {<<"b">>, state_gset},

    lasp:bind(IVar1, {state_gset, [5, 3]}),
    lasp:bind(IVar2, {state_gset, [2, 6]}),
    lasp:read(IVar1, {cardinality, 2}),
    lasp:read(IVar2, {cardinality, 2}),

    OVar = achlys_mr:schedule([
        {IVar1, fun(Value) -> % Map
            [{a, Value + 1}]
        end},
        {IVar2, fun(Value) -> % Map
            [{b, Value + 1}]
        end}
    ], fun(Key, Values) -> % Reduce
        case Key of
            a -> [{a, lists:foldl(fun(E, Acc) -> E + Acc end, 0, Values)}];
            b -> lists:map(fun(E) -> {a, 2 * E} end, Values);
        _ -> [] end
    end),

    case lasp:read(OVar, {strict, undefined}) of
        {ok, {_, _, _, VarState}} ->
            io:format("OVar=~p~n", [VarState])
    end.

% @pre -
% @post -
example_2() ->
    ok.
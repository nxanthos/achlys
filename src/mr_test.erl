-module(mr_test).
-export([
    example_1/0
]).

% mr_test:example_1().

% @pre -
% @post -
example_1() ->
    
    % lasp_peer_service:join('achlys1@192.168.1.6'),

    timer:sleep(500),
    io:format("The node has been linked~n"),
    io:format("The process id = ~p~n", [self()]),
    IVar1 = {<<"a">>, state_gset},
    IVar2 = {<<"b">>, state_gset},

    lasp:bind(IVar1, {state_gset, [5, 3]}),
    lasp:bind(IVar2, {state_gset, [2, 6]}),
    lasp:read(IVar1, {cardinality, 2}),
    lasp:read(IVar2, {cardinality, 2}),

    Pairs = achlys_mr:schedule([
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
    lists:foreach(fun(Pair) ->
        io:format("Pair -> ~w~n", [Pair])
    end, Pairs).

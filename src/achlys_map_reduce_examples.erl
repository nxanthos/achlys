-module(achlys_map_reduce_examples).
-export([
    example_1/0
]).

% @pre -
% @post -
example_1() ->
    
    IVar1 = {<<"a">>, state_gset},
    IVar2 = {<<"b">>, state_gset},

    lasp:update(IVar1, {add, 5}, self()),
    lasp:update(IVar1, {add, 3}, self()),
    lasp:update(IVar2, {add, 2}, self()),
    lasp:update(IVar2, {add, 6}, self()),

    lasp:read(IVar1, {cardinality, 2}),
    lasp:read(IVar2, {cardinality, 2}),

    Pairs = achlys_map_reduce:schedule([
        {IVar1, fun(Value) -> % Map
            [{a, Value + 1}]
        end},
        {IVar2, fun(Value) -> % Map
            [{b, Value + 1}]
        end}
    ], fun(Key, Values, _) -> % Reduce
        case Key of
            a -> [{a, lists:foldl(fun(E, Acc) -> E + Acc end, 0, Values)}];
            b -> lists:map(fun(E) -> {a, 2 * E} end, Values);
        _ -> [] end
    end),

    lists:foreach(fun(Pair) ->
        io:format("Pair -> ~w~n", [Pair])
    end, Pairs).

-module(achlys_map_reduce_examples).
-export([
    example_1/0,
    example_2/0,
    example_3/0
]).

% achlys_map_reduce_examples:example_1().
% achlys_map_reduce_examples:example_2().
% achlys_map_reduce_examples:example_3().

% @pre -
% @post -
example_1() ->
    
    IVar1 = {<<"a">>, state_gset},
    IVar2 = {<<"b">>, state_gset},

    lasp:bind(IVar1, {state_gset, [5, 3]}),
    lasp:bind(IVar2, {state_gset, [2, 6]}),
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
    end, Pairs),
    ok.

% @pre -
% @post -
example_2() ->

    IVar = {<<"table">>, state_gset},
    Path = "dataset/data.csv",
    file_reader:load_csv(Path, IVar),

    Pairs = achlys_map_reduce:schedule([
        {IVar, fun(Value) -> % Map
            case Value of {_ID, #{
                country := Country,
                temperature := Temperature
            }} -> [{Country, Temperature}];
            _ -> [] end
        end}
    ], fun(Key, Values, _) -> % Reduce
        [{Key, lists:max(Values)}]
    end),

    lists:foreach(fun(Pair) ->
        case Pair of #{key := Key, value := Value} ->
            io:format("Key=~p Value=~p~n", [Key, Value])
        end
    end, Pairs),
    ok.

% @pre -
% @post -
example_3() ->

    IVar = {<<"table">>, state_gset},
    Path = "dataset/data.csv",
    file_reader:load_csv(Path, IVar),

    Pairs = achlys_map_reduce:schedule([
        {IVar, fun(Value) -> % Map
            case Value of {_ID, #{
                country := Country
            }} -> [{Country, 1}];
            _ -> [] end
        end}
    ], fun(Key, Values, _) -> % Reduce
        [{Key, lists:sum(Values)}]
    end),

    lists:foreach(fun(Pair) ->
        case Pair of #{key := Key, value := Value} ->
            io:format("Key=~p Value=~p~n", [Key, Value])
        end
    end, Pairs),
    ok.
-module(mr_test).
-export([
    example_1/0,
    example_2/0,
    example_3/0
]).

% mr_test:example_1().
% mr_test:example_2().
% mr_test:example_3().

% @pre -
% @post -
example_1() ->

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
        {ok, {_, _, _, Var}} ->
            io:format("OVar=~p~n", [Var])
    end.

% @pre -
% @post -
example_2() ->

    IVar = {<<"table">>, state_gset},
    Path = "dataset/data.csv",
    file_reader:load_csv(Path, IVar),

    OVar = achlys_mr:schedule([
        {IVar, fun(Value) -> % Map
            case Value of {_ID, #{
                country := Country,
                temperature := Temperature
            }} -> [{Country, Temperature}];
            _ -> [] end
        end}
    ], fun(Key, Values) -> % Reduce
        [{Key, lists:max(Values)}]
    end),
    
    case lasp:read(OVar, {strict, undefined}) of
        {ok, {_, _, _, Var}} ->
            io:format("OVar=~p~n", [Var])
    end.

% @pre -
% @post -
example_3() ->

    IVar = {<<"documents">>, state_gset},
    lasp:bind(IVar, {state_gset, [
        "These are my cats",
        "The cats are over there",
        "I love cats"
    ]}),
    lasp:read(IVar, {cardinality, 3}),

    OVar = achlys_mr:schedule([
        {IVar, fun(Value) -> % Map
            L = string:tokens(Value, " "),
            orddict:to_list(lists:foldl(fun(Word, Counters) ->
                orddict:update_counter(Word, 1, Counters)
            end, orddict:new(), L))
        end}
    ], fun(Key, Values) -> % Reduce
        [{Key, lists:sum(Values)}]
    end),

    case lasp:read(OVar, {strict, undefined}) of
        {ok, {_, _, _, Var}} ->
            io:format("OVar=~p~n", [Var])
    end.

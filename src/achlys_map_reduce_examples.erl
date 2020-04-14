-module(achlys_map_reduce_examples).
-export([
    example_1/0,
    example_2/0
]).

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
readfile(Path) ->
    {ok, IOData} = file:read_file(Path),
    Binaries = binary:split(IOData, [<<"\n">>], [global]),
    lists:foldr(fun(Binary, Lines) ->
        Line = erlang:binary_to_list(Binary),
        case Line of [] -> Lines; _ -> [Line|Lines] end 
    end, [], Binaries).

% @pre -
% @post -
gen_tuple(Labels, Columns, Parser) ->
    L = lists:zip(Labels, Columns),
    lists:foldl(fun({Label, Column}, Tuple) ->
        Key = erlang:list_to_atom(Label),
        Value = erlang:apply(Parser, [Key, Column]),
        maps:put(Key, Value, Tuple)
    end, #{}, L).

% @pre -
% @post -
read_csv(Path, Separator, Parser) ->
    Lines = readfile(Path),
    case Lines of
        [] -> [];
        [Header|Body] ->
            Labels = string:tokens(Header, Separator),
            lists:map(fun(Line) ->
                Columns = string:tokens(Line, Separator),
                gen_tuple(Labels, Columns, Parser)
            end, Body)
    end.

% achlys_map_reduce_examples:example_2().

% @pre -
% @post -
give_ids(Tuples) ->
    lists:foldl(fun(Tuple, {I, List}) ->
        {I + 1, [maps:put(id, I, Tuple)|List]}
    end, {0, []}, Tuples).

add_data(IVar) ->
    Path = "dataset/data.csv",
    Separator = ";",
    Parser = fun(Label, Column) ->
        case Label of temperature ->
            erlang:list_to_integer(Column);
        _ -> Column end
    end,
    {N, Tuples} = give_ids(read_csv(Path, Separator, Parser)),
    lasp:bind(IVar, {state_gset, Tuples}),
    lasp:read(IVar, {cardinality, N}).

% @pre -
% @post -
example_2() ->

    IVar = {<<"table">>, state_gset},
    add_data(IVar),

    Pairs = achlys_map_reduce:schedule([
        {IVar, fun(Value) -> % Map
            case Value of #{
                country := Country,
                temperature := Temperature
            } -> [{Country, Temperature}];
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
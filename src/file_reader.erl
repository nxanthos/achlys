-module(file_reader).
-export([
    load_csv/2
]).

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

% @pre -
% @post -
give_ids(Tuples) ->
    lists:foldl(fun(Tuple, {I, List}) ->
        {I + 1, [{I, Tuple}|List]}
    end, {0, []}, Tuples).

% @pre -
% @post -
load_csv(Path, GSet) ->
    Separator = ";",
    Parser = fun(Label, Column) ->
        case Label of temperature ->
            erlang:list_to_integer(Column);
        _ -> Column end
    end,
    {N, Tuples} = give_ids(read_csv(Path, Separator, Parser)),
    % io:format("Tuples=~p~n", [Tuples]),
    lasp:bind(GSet, {state_gset, Tuples}),
    lasp:read(GSet, {cardinality, N}).
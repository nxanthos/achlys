-module(achlys_mr_csv_adapter).
-export([
    get_pairs/1
]).

% {achlys_mr_csv_adapter, [
%     {"a.csv", fun(Tuple) -> ... end},
%     {"b.csv", fun(Tuple) -> ... end}
% ]},

% @pre -
% @post -
get_pairs(Entries) ->
    Separator = ";",
    Parser = fun(_, Column) -> Column end,
    lists:flatmap(fun({Path, Map}) ->
        Tuples = file_reader:read_csv(Path, Separator, Parser),
        lists:flatmap(fun(Tuple) ->
            erlang:apply(Map, [Tuple])
        end, Tuples)
    end, Entries).

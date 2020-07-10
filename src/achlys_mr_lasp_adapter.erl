-module(achlys_mr_lasp_adapter).
-export([
    get_pairs/1
]).

% {achlys_mr_lasp_adapter, [
%     {IVar1, fun(Value) -> ... end},
%     {IVar2, fun(Value) -> ... end}
% ]}

% @pre -
% @post -
get_pairs(Entries) ->
    lists:flatmap(fun({IVar, Map}) ->
        Values = achlys_util:query(IVar),
        lists:flatmap(fun(Value) ->
            erlang:apply(Map, [Value])
        end, Values)
    end, Entries).

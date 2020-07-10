-module(achlys_mr_adapter).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    load/1 
]).

% @pre -
% @post -
load(Adapters) ->
    lists:flatmap(fun(Adapter) ->
        case Adapter of
            {none, Pairs} -> Pairs;
            {Module, Entries} ->
                Module:get_pairs(Entries)
        end
    end, Adapters).

% ---------------------------------------------
% EUnit tests:
% ---------------------------------------------

-ifdef(TEST).

% @pre -
% @post -
load_no_adapter_test() ->
    ?assertEqual(load([
        {none, [{key, 1}, {key, 3}]}
    ]), [{key, 1}, {key, 3}]),
    ok.

% @pre -
% @post -
load_lasp_adapter_test() ->

    {ok, Pid} = lasp_sup:start_link(),
    Var = {<<"var1">>, state_gset},
    lasp:bind(Var, {state_gset, [5, 3]}),
    lasp:read(Var, {cardinality, 2}),

    ?assertEqual(load([
        {achlys_mr_lasp_adapter, [
            {Var, fun(Value) ->
                [{key, Value}]
            end}
        ]}
    ]), [{key, 3}, {key, 5}]),
    ok.

% @pre -
% @post -
load_cvs_adapter_test() ->
    ?assertEqual(load([
        {achlys_mr_csv_adapter, [
            {"dataset/test.csv", fun(Tuple) ->
                case Tuple of #{
                    temperature := Temperature,
                    country := Country
                } -> [{Country, Temperature}];
                _ -> [] end
            end}
        ]}
    ]), [
        {"Belgium", "15"},
        {"France", "14"},
        {"Spain", "18"},
        {"Greece", "20"}
    ]).

% @pre -
% @post -
load_multiple_adapters_test() ->

    Var = {<<"var2">>, state_gset},
    lasp:bind(Var, {state_gset, [{"Germany", "12"}]}),
    lasp:read(Var, {cardinality, 1}),

    ?assertEqual(load([
        {achlys_mr_lasp_adapter, [
            {Var, fun(Value) -> [Value] end}
        ]},
        {achlys_mr_csv_adapter, [
            {"dataset/test.csv", fun(Tuple) ->
                case Tuple of #{
                    temperature := Temperature,
                    country := Country
                } -> [{Country, Temperature}];
                _ -> [] end
            end}
        ]}
    ]), [
        {"Germany", "12"},
        {"Belgium", "15"},
        {"France", "14"},
        {"Spain", "18"},
        {"Greece", "20"}
    ]),
    ok.

-endif.

% To launch the tests:
% rebar3 eunit --module=achlys_mr_adapter
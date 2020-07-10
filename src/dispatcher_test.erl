-module(dispatcher_test).
-export([
    test_1/0,
    test_2/0
]).

% dispatcher_test:test_1().
% dispatcher_test:test_2().

% @pre -
% @post -
test_1() ->
    Pairs = [
        {key1, value1}, {key1, value3}, {key1, value4}, {key1, value5}, {key1, value6},
        {key2, value7}, {key2, value8}, {key2, value9}, {key1, value2}
    ],
    Result = achlys_mr_dispatcher:start(Pairs),
    io:format("Result=~p", [Result]),
    ok.

% @pre -
% @post -
test_2() ->
    Pairs = [{key1, value1}, {key2, value3}, {key1, value2}],
    Result = achlys_mr_dispatcher:start(Pairs),
    io:format("Result=~p", [Result]),
    ok.
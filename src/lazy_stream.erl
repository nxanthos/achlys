-module(lazy_stream).

-export([
    test_clock/0,
    test_sync/0,
    test_multiple_input_vars/0
]).

% @pre -
% @post -
test_clock() ->

    ID = {<<"var">>, state_gset},
    lasp:declare(ID, state_gset),
    {ok, {_, _, Metadata1, Value1}} = lasp:read(ID, undefined),
    lasp:update(ID, {add, 42}, self()),
    {ok, {_, _, Metadata2, Value2}} = lasp:read(ID, undefined),

    io:format("Metadata 1=~p~n", [Metadata1]),
    io:format("Value 1=~p~n", [Value1]),
    io:format("Metadata 2=~p~n", [Metadata2]),
    io:format("Value 2=~p~n", [Value2]),

    Clock1 = orddict:fetch(clock, Metadata1),
    Clock2 = orddict:fetch(clock, Metadata2),

    io:format("Clock1=~p~n", [Clock1]),
    io:format("Clock2=~p~n", [Clock2]),
    io:format("Is Clock1 equal to Clock2 ? ~p~n", [lasp_vclock:equal(Clock1, Clock2)]),
    io:format("Is Clock1 equal to Clock1 ? ~p~n", [lasp_vclock:equal(Clock1, Clock1)]),
    io:format("Is Clock2 equal to Clock2 ? ~p~n", [lasp_vclock:equal(Clock2, Clock2)]),
    ok.

% @pre -
% @post -
test_sync() ->

    ID = {<<"var">>, state_gset},
    lasp:declare(ID, state_gset),

    InitialState = 0,
    {ok, Pid} = achlys_lazy_stream_reducer:start([
        {ID, fun(State, Operation) ->
            case Operation of
                {add, N} -> State + N;
                _ -> State
            end
        end}
    ], InitialState),

    io:format("PID=~p~n", [Pid]),
    io:format("Self=~p~n", [self()]),

    lasp:update(ID, {add, 5}, self()),
    lasp:update(ID, {add, 2}, self()),
    lasp:update(ID, {add, 3}, self()),

    S1 = achlys_lazy_stream_reducer:get_state(Pid),
    io:format("State=~p~n", [S1]),

    lasp:update(ID, {add, 9}, self()),
    lasp:update(ID, {add, 1}, self()),

    S2 = achlys_lazy_stream_reducer:get_state(Pid),
    io:format("State=~p~n", [S2]),
    ok.

% @pre -
% @post -
test_multiple_input_vars() ->

    A = {<<"a">>, state_gset},
    B = {<<"b">>, state_gset},

    lasp:declare(A, state_gset),
    lasp:declare(B, state_gset),

    InitialState = 0,

    Reduce = fun(State, Operation) ->
        case Operation of
            {add, N} -> State + N;
            _ -> State
        end
    end,

    {ok, Pid} = achlys_lazy_stream_reducer:start([
        {A, Reduce},
        {B, Reduce}
    ], InitialState),

    io:format("PID=~p~n", [Pid]),
    io:format("Self=~p~n", [self()]),

    lasp:update(A, {add, 5}, self()),
    lasp:update(B, {add, 2}, self()),
    lasp:update(A, {add, 3}, self()),

    S1 = achlys_lazy_stream_reducer:get_state(Pid),
    io:format("State=~p~n", [S1]),
    ok.
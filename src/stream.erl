-module(stream).

-export([
    test_gset/0,
    test_gcounter/0,
    test_pncounter/0,
    test_orset/0,
    test_twopset/0
]).

-export([
    test_async/0,
    test_multiple_input_vars/0,
    test_concurrent_stream/0,
    test_with_achlys_task_model/0
]).

default_callback(State) ->
    io:format("Current state=~p~n", [State]).

% Variables:

% @pre -
% @post -
test_gcounter() ->
 
    ID = {<<"var">>, state_gcounter},
    lasp:declare(ID, state_gcounter),

    InitialState = 0,
    Callback = fun default_callback/1,

    {ok, Pid} = achlys_stream_reducer:start([
        {ID, fun(State, Operation) ->
            io:format("Operation=~p~n", [Operation]),
            State
        end}
    ], InitialState, Callback),

    io:format("PID=~p~n", [Pid]),
    io:format("Self=~p~n", [self()]),

    io:format("Starting the timer~n"),
    timer:sleep(1000),
    lasp:update(ID, {increment, 1}, self()),
    timer:sleep(1000),
    lasp:update(ID, {increment, 2}, self()),
    ok.

% @pre -
% @post -
test_pncounter() ->

    ID = {<<"var">>, state_pncounter},
    lasp:declare(ID, state_pncounter),

    InitialState = 0,
    Callback = fun default_callback/1,

    {ok, Pid} = achlys_stream_reducer:start([
        {ID, fun(State, Operation) ->
            io:format("Operation=~p~n", [Operation]),
            State
        end}
    ], InitialState, Callback),

    io:format("PID=~p~n", [Pid]),
    io:format("Self=~p~n", [self()]),

    io:format("Starting the timer~n"),
    timer:sleep(1000),
    lasp:update(ID, {increment, 5}, self()),
    timer:sleep(1000),
    lasp:update(ID, {decrement, 1}, self()),
    timer:sleep(1000),
    lasp:update(ID, {increment, 2}, self()),
    ok.

% @pre -
% @post -
test_gset() ->

    ID = {<<"var">>, state_gset},
    lasp:declare(ID, state_gset),

    InitialState = 0,
    Callback = fun default_callback/1,

    {ok, Pid} = achlys_stream_reducer:start([
        {ID, fun(State, Operation) ->
            case Operation of
                {add, N} -> State + N;
                _ -> State
            end
        end}
    ], InitialState, Callback),

    io:format("PID=~p~n", [Pid]),
    io:format("Self=~p~n", [self()]),

    io:format("Starting the timer~n"),
    timer:sleep(1000),
    lasp:update(ID, {add, 42}, self()),
    timer:sleep(1000),
    lasp:update(ID, {add, 10}, self()),
    timer:sleep(1000),
    lasp:update(ID, {add, 12}, self()),
    timer:sleep(1000),    
    lasp:update(ID, {add, 1}, self()),
    ok.

% @pre -
% @post -
test_orset() ->

    ID = {<<"var">>, state_orset},
    lasp:declare(ID, state_orset),

    InitialState = 0,
    Callback = fun default_callback/1,

    {ok, Pid} = achlys_stream_reducer:start([
        {ID, fun(State, Operation) ->
            io:format("Operation=~p~n", [Operation]),
            State
        end}
    ], InitialState, Callback),

    io:format("PID=~p~n", [Pid]),
    io:format("Self=~p~n", [self()]),

    io:format("Starting the timer~n"),
    timer:sleep(1000),
    lasp:update(ID, {add, 1}, self()),
    timer:sleep(1000),
    lasp:update(ID, {add, 5}, self()),
    timer:sleep(1000),
    lasp:update(ID, {rmv, 5}, self()),
    timer:sleep(1000),
    lasp:update(ID, {rmv, 1}, self()),
    ok.

% @pre -
% @post -
test_twopset() ->

    ID = {<<"var">>, state_twopset},
    lasp:declare(ID, state_twopset),

    InitialState = 0,
    Callback = fun default_callback/1,

    {ok, Pid} = achlys_stream_reducer:start([
        {ID, fun(State, Operation) ->
            io:format("Operation=~p~n", [Operation]),
            State
        end}
    ], InitialState, Callback),

    io:format("PID=~p~n", [Pid]),
    io:format("Self=~p~n", [self()]),

    io:format("Starting the timer~n"),
    timer:sleep(1000),
    lasp:update(ID, {add, 1}, self()),
    timer:sleep(1000),
    lasp:update(ID, {rmv, 5}, self()),
    timer:sleep(1000),
    lasp:update(ID, {rmv, 5}, self()),
    ok.

% Other tests:

% @pre -
% @post -
test_async() ->
    
    ID = {<<"var">>, state_gset},
    lasp:declare(ID, state_gset),

    InitialState = 0,

    Reduce = fun(State, Operation) ->
        case Operation of
            {add, N} -> State + N;
            _ -> State
        end
    end,

    Callback = fun default_callback/1,

    {ok, Pid} = achlys_stream_reducer:start(
        [{ID, Reduce}],
        InitialState,
        Callback
    ),

    io:format("PID=~p~n", [Pid]),
    io:format("Self=~p~n", [self()]),

    io:format("Starting the timer~n"),
    timer:sleep(1000),
    lasp:update(ID, {add, 10}, self()),
    timer:sleep(1000),
    lasp:update(ID, {add, 5}, self()),
    timer:sleep(1000),
    lasp:update(ID, {add, 3}, self()),
    timer:sleep(1000),
    lasp:update(ID, {add, 2}, self()),
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

    Callback = fun default_callback/1,

    {ok, Pid} = achlys_stream_reducer:start([
        {A, Reduce},
        {B, Reduce}
    ], InitialState, Callback),

    io:format("PID=~p~n", [Pid]),
    io:format("Self=~p~n", [self()]),

    io:format("Starting the timer~n"),
    timer:sleep(1000),
    lasp:update(A, {add, 10}, self()),
    timer:sleep(1000),
    lasp:update(B, {add, 5}, self()),
    timer:sleep(1000),
    lasp:update(B, {add, 3}, self()),
    timer:sleep(1000),
    lasp:update(A, {add, 2}, self()),
    ok.

% @pre -
% @post -
test_concurrent_stream() ->

    A = {<<"a">>, state_gset},
    B = {<<"b">>, state_gset},

    lasp:declare(A, state_gset),
    lasp:declare(B, state_gset),

    Callback = fun default_callback/1,

    % Sum of the values:

    {ok, Pid1} = achlys_stream_reducer:start([
        {A, fun(State, Operation) ->
            case Operation of
                {add, N} -> State + N;
                _ -> State
            end
        end}
    ], 0, Callback),

    % Maximum of the values:

    {ok, Pid2} = achlys_stream_reducer:start([
        {B, fun(State, Operation) ->
            case {
                Operation,
                State
            } of
                {{add, N}, undefined} -> N;
                {{add, N}, Max} when N > Max -> N;
                {{add, N}, Max} when N =< Max -> Max;
                _ -> State
            end
        end}
    ], undefined, Callback),

    io:format("PID1=~p~n", [Pid1]),
    io:format("PID2=~p~n", [Pid2]),
    io:format("Self=~p~n", [self()]),

    io:format("Starting the timer~n"),
    timer:sleep(1000),
    lasp:update(A, {add, 42}, self()),
    timer:sleep(1000),
    lasp:update(B, {add, 10}, self()),
    timer:sleep(1000),
    lasp:update(B, {add, 12}, self()),
    timer:sleep(1000),    
    lasp:update(A, {add, 1}, self()),
    ok.

% @pre -
% @post -
test_with_achlys_task_model() ->

    ID = {<<"var">>, state_gset},
    lasp:declare(ID, state_gset),

    Task = achlys:declare(mytask, all, single, fun() ->

        Callback = fun default_callback/1,

        achlys_stream_reducer:start([
            {ID, fun(State, Operation) ->
                case Operation of
                    {add, N} -> State + N;
                    _ -> State
                end
            end}
        ], 0, Callback)
    end),

    achlys:bite(Task),

    io:format("Starting the timer~n"),
    timer:sleep(1000),
    lasp:update(ID, {add, 42}, self()),
    timer:sleep(1000),
    lasp:update(ID, {add, 10}, self()),
    timer:sleep(1000),
    lasp:update(ID, {add, 12}, self()),
    timer:sleep(1000),    
    lasp:update(ID, {add, 1}, self()),
    ok.

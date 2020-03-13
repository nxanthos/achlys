-module(path_test).
-export([
    schedule/0  
]).

% Helpers:

get_actor() -> 
    {
        partisan_remote_reference,
        partisan_peer_service_manager:mynode(),
        partisan_util:gensym(self())
    }.

sum({state_pncounter, LValue}, {state_pncounter, RValue}) ->
    {state_pncounter, orddict:merge(
        fun(_, {Inc1, Dec1}, {Inc2, Dec2}) ->
            {Inc1 + Inc2, Dec1 + Dec2}
        end,
        LValue,
        RValue
    )}.

% Test:

test_pncounter_read() ->
    
    Type = state_pncounter,
    ID = {<<"counter">>, Type},
    
    lasp:declare(ID, Type),
    
    erlang:spawn(fun() ->
        timer:sleep(500),
        lasp:update(ID, {increment, 1}, get_actor())
    end),
    
    {ok, {ID, Type, Metadata, Value}} = lasp:read(ID, {strict, state_pncounter:new()}),
    N = state_pncounter:query(Value),

    io:format("ID=~w~nType=~w~nMetadata=~w~nvalue=~w~n", [ID, Type, Metadata, Value]),
    io:format("N=~w~n", [N]),
    
    ok.

test_counter_sum() ->

    Type = state_pncounter,
    A = {<<"a">>, Type},
    B = {<<"b">>, Type},
    C = {<<"c">>, Type},

    lasp:declare(A, Type),
    lasp:declare(B, Type),
    lasp:declare(C, Type),

    % Update:

    lasp:update(A, {increment, 1}, get_actor()),
    lasp:update(A, {decrement, 5}, get_actor()),
    lasp:update(B, {increment, 2}, get_actor()),
    lasp:update(B, {decrement, 2}, get_actor()),

    {ok, {_, _, _, LValue}} = lasp:read(A, {strict, undefined}),
    {ok, {_, _, _, RValue}} = lasp:read(B, {strict, undefined}),
    {ok, {_, _, _, Value}} = lasp:bind(C, sum(LValue, RValue)),

    N = state_pncounter:query(Value),
    io:format("N=~w~n", [N]),
    ok.

test_pncounter_dag() ->

    Type = state_pncounter,
    A = {<<"a">>, Type},
    B = {<<"b">>, Type},
    C = {<<"c">>, Type},

    lasp:declare(A, Type),
    lasp:declare(B, Type),
    lasp:declare(C, Type),

    lasp_process:start_dag_link([
        [
            {A, fun(ID, Threshold) ->
                lasp:read(ID, Threshold)
            end},
            {B, fun(ID, Threshold) ->
                lasp:read(ID, Threshold)
            end}
        ],
        fun(R1, R2) ->
            {_, _, _, LValue} = R1,
            {_, _, _, RValue} = R2,
            sum(LValue, RValue)
        end,
        {C, fun(ID, Value) -> % Write function
            lasp:bind(ID, Value)
        end}
    ]),

    lasp:stream(C, fun(Value) ->
        io:format("Ovar=~w~n", [Value])
    end),

    timer:sleep(100),
    lasp:update(A, increment, get_actor()),
    timer:sleep(100),
    lasp:update(B, increment, get_actor()),
    timer:sleep(100),
    lasp:update(B, increment, get_actor()),
    timer:sleep(500),

    io:format("A=~w~n", [lasp:query(A)]),
    io:format("B=~w~n", [lasp:query(B)]),
    io:format("C=~w~n", [lasp:query(C)]),
    ok.

test_distributed_sum_counter() ->

    Type = state_pncounter,
    A = {<<"a">>, Type},
    B = {<<"b">>, Type},
    C = {<<"c">>, Type},

    lasp:declare(A, Type),
    lasp:declare(B, Type),
    lasp:declare(C, Type),

    lasp_process:start_dag_link([
        [
            {A, fun(ID, Threshold) ->
                lasp:read(ID, Threshold)
            end},
            {B, fun(ID, Threshold) ->
                lasp:read(ID, Threshold)
            end}
        ],
        fun(R1, R2) ->
            {_, _, _, LValue} = R1,
            {_, _, _, RValue} = R2,
            sum(LValue, RValue)
        end,
        {C, fun(ID, Value) ->
            lasp:bind(ID, Value)
        end}
    ]),

    lasp:stream(C, fun(Sum) ->
        io:format("Sum=~w~n", [Sum])    
    end),

    Task = achlys:declare(mytask, all, single, fun() ->
        io:format("Executing the task~n"),
        lasp:update(A, {increment, 1}, get_actor()),
        lasp:update(A, {decrement, 5}, get_actor()),
        lasp:update(B, {increment, 2}, get_actor()),
        lasp:update(B, {decrement, 2}, get_actor())
    end),

    achlys:bite(Task),
    ok.

schedule() ->
    % test_pncounter_read(),
    test_pncounter_dag(),
    % test_counter_sum(),
    % test_distributed_sum_counter(),
    ok.
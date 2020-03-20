-module(path_test).
-export([
    test_gcounter_read/0,
    test_gcounter_sum/0,
    test_gcounter_min/0,
    test_gcounter_max/0,
    test_gcounter_dag/0,
    test_achlys_process/0
]).

% Helpers:

get_actor() -> 
    {
        partisan_remote_reference,
        partisan_peer_service_manager:mynode(),
        partisan_util:gensym(self())
    }.

% Tests:

test_gcounter_read() ->
    
    Type = state_gcounter,
    ID = {<<"counter">>, Type},
    
    lasp:declare(ID, Type),
    
    erlang:spawn(fun() ->
        timer:sleep(500),
        lasp:update(ID, {increment, 5}, get_actor())
    end),
    
    {ok, {ID, Type, Metadata, Value}} = lasp:read(ID, {strict, state_gcounter:new()}),
    N = state_gcounter:query(Value),

    io:format("ID=~w~nType=~w~nMetadata=~w~nvalue=~w~n", [ID, Type, Metadata, Value]),
    io:format("N=~w~n", [N]),
    
    ok.

test_gcounter_sum() ->

    Type = state_gcounter,
    A = {<<"a">>, Type},
    B = {<<"b">>, Type},
    C = {<<"c">>, Type},

    lasp:declare(A, Type),
    lasp:declare(B, Type),
    lasp:declare(C, Type),

    % Update:

    lasp:update(A, {increment, 1}, get_actor()),
    lasp:update(B, {increment, 2}, get_actor()),

    {ok, {_, _, _, LValue}} = lasp:read(A, {strict, undefined}),
    {ok, {_, _, _, RValue}} = lasp:read(B, {strict, undefined}),
    {ok, {_, _, _, Value}} = lasp:bind(C, state_gcounter_ext:sum(LValue, RValue)),

    N = state_gcounter:query(Value),
    io:format("N=~w~n", [N]),
    ok.

test_gcounter_min() ->

    Type = state_gcounter,
    A = {<<"a">>, Type},
    B = {<<"b">>, Type},
    C = {<<"c">>, Type},

    lasp:declare(A, Type),
    lasp:declare(B, Type),
    lasp:declare(C, Type),

    lasp:update(A, {increment, 1}, get_actor()),
    lasp:update(B, {increment, 2}, get_actor()),

    {ok, {_, _, _, LValue}} = lasp:read(A, {strict, undefined}),
    {ok, {_, _, _, RValue}} = lasp:read(B, {strict, undefined}),
    {ok, {_, _, _, Value}} = lasp:bind(C, state_gcounter_ext:min(LValue, RValue, min)),

    N = state_gcounter:query(Value),
    io:format("N=~w~n", [N]),
    ok.

test_gcounter_max() ->

    Type = state_gcounter,
    A = {<<"a">>, Type},
    B = {<<"b">>, Type},
    C = {<<"c">>, Type},

    lasp:declare(A, Type),
    lasp:declare(B, Type),
    lasp:declare(C, Type),

    lasp:update(A, {increment, 1}, get_actor()),
    lasp:update(B, {increment, 2}, get_actor()),

    {ok, {_, _, _, LValue}} = lasp:read(A, {strict, undefined}),
    {ok, {_, _, _, RValue}} = lasp:read(B, {strict, undefined}),
    {ok, {_, _, _, Value}} = lasp:bind(C, state_gcounter_ext:max(LValue, RValue, max)),

    N = state_gcounter:query(Value),
    io:format("N=~w~n", [N]),
    ok.

test_gcounter_dag() ->

    Type = state_gcounter,
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
            state_gcounter_ext:sum(LValue, RValue)
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

test_achlys_process() ->

    Type = state_gcounter,
    A = {<<"a">>, Type},
    B = {<<"b">>, Type},
    C = {<<"c">>, Type},

    lasp:declare(A, Type),
    lasp:declare(B, Type),
    lasp:declare(C, Type),

    achlys_process:start_dag_link(
        [A, B], C,
        fun([GSet1, GSet2]) ->
            state_gcounter_ext:sum(GSet1, GSet2)
        end
    ),

    lasp:stream(C, fun(Sum) ->
        io:format("Sum=~w~n", [Sum])    
    end),

    erlang:spawn(fun() ->
        timer:sleep(1000),
        lasp:update(A, {increment, 1}, get_actor()),
        timer:sleep(1000),
        lasp:update(B, {increment, 2}, get_actor())
    end),

    ok.
-module(circuit).
-export([
    start/0,
    start_task/0,
    halfadder/0,
    fulladder/0,
    debug_ha/0,
    debug_fa/0,
    debug/0
]).

% I1 - Input variable
% I2 - Input variable
% O - Output variable
% Fun - Condition
build_gate(S1, S2, O, Fun) -> 
    lasp_process:start_dag_link([
        [ % Read function
            {S1, fun(ID, Threshold) ->
                lasp:read(ID, Threshold)
            end},
            {S2, fun(ID, Threshold) ->
                lasp:read(ID, Threshold)
            end}
        ],
        fun(R1, R2) -> % Transformation function
            {_, _, _, {_, {_, V1}}} = R1,
            {_, _, _, {_, {_, V2}}} = R2,
            Fun(V1, V2)
        end,
        {O, fun(ID, Value) -> % Write function
            lasp:update(ID, {set, get_timestamp(), Value}, self())
        end}
    ]).

% I1 - Input variable
% I2 - Input variable
% O - Output variable
logical_and(I1, I2, O) ->
    build_gate(I1, I2, O, fun(V1, V2) ->
        V1 and V2
    end).

% I1 - Input variable
% I2 - Input variable
% O - Output variable
logical_or(I1, I2, O) ->
    build_gate(I1, I2, O, fun(V1, V2) ->
        V1 or V2
    end).

% I1 - Input variable
% I2 - Input variable
% O - Output variable
logical_xor(I1, I2, O) ->
    build_gate(I1, I2, O, fun(V1, V2) ->
        V1 xor V2
    end).

half_adder(A, B, S, C) ->
    logical_xor(A, B, S),
    logical_and(A, B, C).

full_adder(A, B, Cin, S, Cout) ->
    % intermediate values
    Type = state_lwwregister,
    {ok, {S1, _, _, _}} = lasp:declare({<<"s1">>, Type}, Type),
    {ok, {C1, _, _, _}} = lasp:declare({<<"c1">>, Type}, Type),
    {ok, {C2, _, _, _}} = lasp:declare({<<"c2">>, Type}, Type),

    half_adder(A, B, S1, C1),
    half_adder(S1, Cin, S, C2),
    logical_or(C1, C2, Cout).

get_timestamp() -> 
    erlang:unique_integer([monotonic, positive]).

start_task() ->
    Type = state_lwwregister,
    {ok, {S1, _, _, _}} = lasp:declare({<<"s1">>, Type}, Type),
    {ok, {S2, _, _, _}} = lasp:declare({<<"s2">>, Type}, Type),
    {ok, {S3, _, _, _}} = lasp:declare({<<"s3">>, Type}, Type),
    {ok, {S4, _, _, _}} = lasp:declare({<<"s4">>, Type}, Type),
    {ok, {S5, _, _, _}} = lasp:declare({<<"s5">>, Type}, Type),

    lasp:update(S1, {set, get_timestamp(), true}, self()),
    lasp:update(S2, {set, get_timestamp(), false}, self()),
    lasp:update(S3, {set, get_timestamp(), false}, self()),

    % (S1 AND S2) OR S3

    Task = achlys:declare(mytask, all, single, fun() ->
        logical_and(S1, S2, S4),
        logical_or(S3, S4, S5),

        lasp:stream(S5, fun(Value) ->
            io:format("Output=~w~n", [Value])
        end)
    end),
    achlys:bite(Task),
    ok.

start() ->

    Type = state_lwwregister,
    {ok, {S1, _, _, _}} = lasp:declare({<<"s1">>, Type}, Type),
    {ok, {S2, _, _, _}} = lasp:declare({<<"s2">>, Type}, Type),
    {ok, {S3, _, _, _}} = lasp:declare({<<"s3">>, Type}, Type),
    {ok, {S4, _, _, _}} = lasp:declare({<<"s4">>, Type}, Type),
    {ok, {S5, _, _, _}} = lasp:declare({<<"s5">>, Type}, Type),

    lasp:update(S1, {set, get_timestamp(), true}, self()),
    lasp:update(S2, {set, get_timestamp(), false}, self()),
    lasp:update(S3, {set, get_timestamp(), false}, self()),

    % (S1 AND S2) OR S3

    logical_and(S1, S2, S4),
    logical_or(S3, S4, S5),

    lasp:stream(S5, fun(Value) ->
        io:format("Output=~w~n", [Value])
    end),

    % The arity of the transformation function corresponds to the number of read functions in the list. The nth argument of the transformation function will be the result of the nth read function (i.e: in this example, the lasp:read function)

    debug(),
    ok.

halfadder() ->
    Type = state_lwwregister,
    {ok, {A, _, _, _}} = lasp:declare({<<"a">>, Type}, Type),
    {ok, {B, _, _, _}} = lasp:declare({<<"b">>, Type}, Type),
    {ok, {S, _, _, _}} = lasp:declare({<<"s">>, Type}, Type),
    {ok, {C, _, _, _}} = lasp:declare({<<"c">>, Type}, Type),

    lasp:update(A, {set, get_timestamp(), true}, self()),
    lasp:update(B, {set, get_timestamp(), true}, self()),

    % S = (A XOR B), C = (A AND B)

    Task = achlys:declare(mytask, all, single, fun() ->
        half_adder(A,B,S,C),

        lasp:stream(S, fun(Value) ->
            io:format("Sum=~w~n", [Value])
        end),
        lasp:stream(C, fun(Value) ->
            io:format("Carry=~w~n", [Value])
        end)
    end),
    achlys:bite(Task),
    ok.

fulladder() ->
    Type = state_lwwregister,
    {ok, {A, _, _, _}} = lasp:declare({<<"a">>, Type}, Type),
    {ok, {B, _, _, _}} = lasp:declare({<<"b">>, Type}, Type),
    {ok, {Cin, _, _, _}} = lasp:declare({<<"cin">>, Type}, Type),
    {ok, {S, _, _, _}} = lasp:declare({<<"s">>, Type}, Type),
    {ok, {Cout, _, _, _}} = lasp:declare({<<"cout">>, Type}, Type),

    lasp:update(A, {set, get_timestamp(), true}, self()),
    lasp:update(B, {set, get_timestamp(), true}, self()),
    lasp:update(Cin, {set, get_timestamp(), true}, self()),

    Task = achlys:declare(mytask, all, single, fun() ->
        full_adder(A, B, Cin, S, Cout),

        lasp:stream(S, fun(Value) ->
            io:format("Sum=~w~n", [Value])
        end),
        lasp:stream(Cout, fun(Value) ->
            io:format("Carry=~w~n", [Value])
        end)
    end),
    achlys:bite(Task),
    ok.

debug_ha() ->
    Type = state_lwwregister,
    {ok, A} = lasp:query({<<"a">>, Type}),
    {ok, B} = lasp:query({<<"b">>, Type}),
    {ok, S} = lasp:query({<<"s">>, Type}),
    {ok, C} = lasp:query({<<"c">>, Type}),
    io:format("A=~w B=~w S=~w C=~w~n", [A, B, S, C]).

debug_fa() ->
    Type = state_lwwregister,
    {ok, A} = lasp:query({<<"a">>, Type}),
    {ok, B} = lasp:query({<<"b">>, Type}),
    {ok, Cin} = lasp:query({<<"cin">>, Type}),
    {ok, S} = lasp:query({<<"s">>, Type}),
    {ok, Cout} = lasp:query({<<"cout">>, Type}),
    io:format("A=~w B=~w Cin=~w S=~w Cout=~w~n", [A, B, Cin, S, Cout]).

debug() ->
    Type = state_lwwregister,
    {ok, V1} = lasp:query({<<"s1">>, Type}),
    {ok, V2} = lasp:query({<<"s2">>, Type}),
    {ok, V3} = lasp:query({<<"s3">>, Type}),
    {ok, V4} = lasp:query({<<"s4">>, Type}),
    {ok, V5} = lasp:query({<<"s5">>, Type}),
    io:format("V1=~w V2=~w V3=~w V4=~w V5=~w~n", [V1, V2, V3, V4, V5]).

-module(circuit).
-export([
    start/0,
    start_task/0,
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

debug() ->
    Type = state_lwwregister,
    {ok, V1} = lasp:query({<<"s1">>, Type}),
    {ok, V2} = lasp:query({<<"s2">>, Type}),
    {ok, V3} = lasp:query({<<"s3">>, Type}),
    {ok, V4} = lasp:query({<<"s4">>, Type}),
    {ok, V5} = lasp:query({<<"s5">>, Type}),
    io:format("V1=~w V2=~w V3=~w V4=~w V5=~w~n", [V1, V2, V3, V4, V5]).

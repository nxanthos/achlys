-module(compute_pi_with_counters).
-export([
    schedule/0,
    debug/0
]).

get_actor() -> 
    {
        partisan_remote_reference,
        partisan_peer_service_manager:mynode(),
        partisan_util:gensym(self())
    }.

get_distance(Point) ->
    case Point of {X, Y} ->
        math:sqrt(X * X + Y * Y)
    end.

get_random_point() ->
    {
        rand:uniform(),
        rand:uniform()
    }.

add_sampling(ICVar, OCVar) ->
    Point = get_random_point(),
    case get_distance(Point) of
        Distance when Distance =< 1 ->
            lasp:update(ICVar, increment, get_actor());
        Distance when Distance > 1 ->
            lasp:update(OCVar, increment, get_actor())
    end.

get_pi(IC, OC) ->
    4 * IC / (IC + OC).

schedule() ->

    Type = state_gcounter,
    ICVar = {<<"icvar">>, Type}, % Inner Counter Variable
    OCVar = {<<"ocvar">>, Type}, % Outer Counter Variable
    
    lasp:declare(ICVar, Type),
    lasp:declare(OCVar, Type),

    lasp:stream(ICVar, fun(IC) ->
        {ok, OC} = lasp:query(OCVar),
        io:format("π ≃ ~w~n", [get_pi(IC, OC)])
    end),

    lasp:stream(OCVar, fun(OC) ->
        {ok, IC} = lasp:query(ICVar),
        io:format("π ≃ ~w~n", [get_pi(IC, OC)])
    end),

    Task = achlys:declare(mytask, all, single, fun() ->
        io:format("Starting the task ~n"),
        achlys_util:repeat(100, fun(_) ->
            add_sampling(ICVar, OCVar)
        end)
    end),
    achlys:bite(Task),

    % compute_pi_with_counters:schedule().
    ok.

debug() ->

    Type = state_gcounter,
    ICVar = {<<"icvar">>, Type},
    OCVar = {<<"ocvar">>, Type},

    {ok, IC} = lasp:query(ICVar),
    {ok, OC} = lasp:query(OCVar),

    io:format("IC=~w OC=~w n=~w~n", [IC, OC, IC + OC]),

    % compute_pi_with_counters:debug().
    ok.
-module(spawn_test).

-export([
    test_async/2,
    test_sync/2
]).

% Tests:
% spawn_test:test_async(2, 10).
% spawn_test:test_async(50, 100).

test_async(N, MaxDelay) when MaxDelay > 0 ->
    achlys_util:repeat(N, fun(K) ->
        achlys_spawn:schedule(fun() ->
            timer:sleep(rand:uniform(MaxDelay)),
            K
        end, [], fun(Result) ->
            io:format("Result ~p~n", [Result])
        end)
    end).


% Tests:
% spawn_test:test_sync(2, 10).
% spawn_test:test_sync(100, 100).

test_sync(N, MaxDelay) when MaxDelay > 0 ->
    achlys_util:repeat(N, fun(K) ->
        erlang:spawn(fun() ->
            Result = achlys_spawn:schedule(fun() ->
                timer:sleep(rand:uniform(MaxDelay)),
                K
            end, []),
            io:format("Result ~p~n", [Result])
        end)
    end).
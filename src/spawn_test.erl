-module(spawn_test).

-export([
    test_sync/2,
    test_sync_burst/3,
    test_async/2,
    test_async_burst/3,
    test_async_timeout/0,
    test_async_error/0
]).

% Tests:

% spawn_test:test_sync(2, 10).
% spawn_test:test_sync(100, 100).
% spawn_test:test_sync_burst(10, 100, 500).

% spawn_test:test_async(2, 10).
% spawn_test:test_async(10000, 100).
% spawn_test:test_async_burst(10, 100, 500).
% spawn_test:test_async_error().
% spawn_test:test_async_timeout().

% @pre -
% @post -
test_async(N, MaxDelay) when MaxDelay > 0 ->
    achlys_util:repeat(N, fun(K) ->
        achlys_spawn:schedule(fun() ->
            timer:sleep(rand:uniform(MaxDelay)),
            % timer:sleep(MaxDelay),
            K
        end, [], fun(Result) ->
            io:format("Result ~p~n", [Result])
        end)
    end).

% @pre -
% @post -
test_sync(N, MaxDelay) when MaxDelay > 0 ->
    achlys_util:repeat(N, fun(K) ->
        erlang:spawn(fun() ->
            Result = achlys_spawn:schedule(fun() ->
                timer:sleep(rand:uniform(MaxDelay)),
                % timer:sleep(MaxDelay),
                K
            end, []),
            io:format("Result ~p~n", [Result])
        end)
    end).

% @pre -
% @post -
test_async_burst(N, M, MaxDelay) ->
    achlys_util:repeat(N, fun(I) ->
        timer:sleep(120), % Wait 120ms before the next burst
        achlys_util:repeat(M, fun(J) ->
            achlys_spawn:schedule(fun() ->
                timer:sleep(rand:uniform(MaxDelay)),
                % timer:sleep(MaxDelay),
                I * M + J
            end, [], fun(Result) ->
                io:format("Result ~p~n", [Result])
            end)
        end)
    end).

% @pre -
% @post -
test_sync_burst(N, M, MaxDelay) ->
    achlys_util:repeat(N, fun(I) ->
        % timer:sleep(120), % Wait 120ms before the next burst
        achlys_util:repeat(M, fun(J) ->
            erlang:spawn(fun() ->
                Result = achlys_spawn:schedule(fun() ->
                    timer:sleep(rand:uniform(MaxDelay)),
                    % timer:sleep(MaxDelay),
                    I * M + J
                end, []),
                io:format("Result ~p~n", [Result])
            end)
        end)
    end).

% @pre -
% @post -
test_async_error() ->
    achlys_spawn:schedule(fun() ->
        1 / 0 % Error
    end, [], fun(Result) ->
        io:format("Result ~p~n", [Result])
    end).

% @pre -
% @post -
test_async_timeout() ->
    achlys_spawn:schedule(fun() ->
        timer:sleep(3000)
    end, [], fun(Result) ->
        io:format("Result ~p~n", [Result])
    end).
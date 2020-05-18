-module(spawn_test).

-export([test/1]).

test(N) ->
    case N > 0 of
        true ->
            achlys_spawn:schedule(
                fun() -> io:format("TEST ~n") end,
                [],
                fun(Result) -> io:format("Result ~p~n", [Result]) end
            ),
            test(N-1);
        false -> ok
    end.

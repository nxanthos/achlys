-module(prime_numbers).
-export([
    debug/0,
    schedule/0
]).

% Helpers :

% @pre -
% @post -
is_prime(1) -> false;
is_prime(K) ->
    is_prime(K, 2, erlang:trunc(math:sqrt(K)) + 1).
is_prime(_, Max, Max) -> true;
is_prime(K, I, Max) ->
    case (K rem I) =:= 0 of
        true -> false;
        _ -> is_prime(K, I + 1, Max)
    end.

% @pre -
% @post -
event_listener({is_prime, K}, State) ->
    case State of #{
        numbers := Numbers,
        solutions := Solutions
    } -> #{
        numbers => lists:delete(K, Numbers),
        solutions => [K|Solutions]
    } end;
event_listener({is_not_prime, K}, State) ->
    case State of #{
        numbers := Numbers,
        solutions := Solutions
    } -> #{
        numbers => lists:delete(K, Numbers),
        solutions => Solutions
    } end;
event_listener(_, State) -> State.

% @pre -
% @post -
update(State, Emit) ->
    case State of
        #{numbers := []} -> State;
        #{numbers := [K|_]} ->
            case is_prime(K) of
                true -> Emit({is_prime, K});
                _ -> Emit({is_not_prime, K})
            end
    end.

% @pre -
% @post -
loop() ->
    case achlys_es:read() of
        #{numbers := []} -> ok;
        #{numbers := [_|_]} ->
            achlys_es:update(fun update/2),
            timer:sleep(100),
            loop()
    end.

% @pre -
% @post -
schedule() ->
    ID = {<<"events">>, state_gset},
    State = #{
        numbers => lists:seq(1, 10),
        solutions => []
    },
    Task = achlys:declare(mytask, all, single, fun() ->
        io:format("Execution of the task~n"),
        achlys_es:start_link(ID, State,
            fun event_listener/2
        ),
        loop(),
        case achlys_es:read() of #{solutions := Solutions} ->
            io:format("Solution: ~p~n", [Solutions])
        end
    end),
    achlys:bite(Task).

% @pre -
% @post -
debug() ->
    ID = {<<"events">>, state_gset},
    {ok, Set} = lasp:query(ID),
    Content = sets:to_list(Set),
    io:format("Variable: ~p~n", [Content]).

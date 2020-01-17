-module(achlys_map_reduce).

-export([
    debug/0,
    schedule_map/0,
    schedule_reduce/0
]).

% Map phase :

% TODO

% Reduce phase :

get_constraints(K) ->
    get_constraints(lists:seq(1, K), K + 1).
get_constraints(L, K) ->
    case L of
        [H1|[H2|T]] ->
            Constraint = {{H1, H2}, K},
            [Constraint|get_constraints(
                T ++ [K],
                K + 1
            )];
        [_|[]] -> [];
        [] -> []
    end.

reduce(Pair_1, Pair_2, ID) ->
    case {Pair_1, Pair_2} of {
        #{key := K1, value := V1},
        #{key := K2, value := V2}
    } when K1 == K2 ->
        #{
            id => ID,
            key => K1,
            value => V1 + V2
        }
    end.

reduction_loop(Constraints) ->
    timer:sleep(rand:uniform(100)), % Delay
    {ok, Set} = lasp:query({<<"pairs">>, state_twopset}),
    Pairs = sets:to_list(Set),
    case erlang:length(Pairs) of
        Length when Length > 1 ->
            reduction_step(Constraints, Pairs),
            reduction_loop(Constraints);
        Length when Length =< 1 ->
            io:format("~n~p~n", [Pairs])
    end.

reduction_step(Constraints, Pairs) ->
    case Constraints of
        [{{A, B}, C}|T] ->
            case {
                lists:search(fun(E) ->
                    case E of #{id := ID} -> ID == A end
                end, Pairs),
                lists:search(fun(E) ->
                    case E of #{id := ID} -> ID == B end
                end, Pairs)
            } of {{value, Pair_1}, {value, Pair_2}} ->
                ID = {<<"pairs">>, state_twopset},
                lasp:update(ID, {add,
                    reduce(Pair_1, Pair_2, C)
                }, self()),
                lasp:update(ID, {rmv, Pair_1}, self()),
                lasp:update(ID, {rmv, Pair_2}, self()),
                io:format("Reduction done!~n");
            _ -> reduction_step(T, Pairs) end;
        [] -> ok
    end.

reduce_phase(Nodes) ->
    Task = achlys:declare(mytask, all, single, fun() ->
        io:format("Starting the task"),
        Constraints = get_constraints(150),
        reduction_loop(Constraints)
    end),
    achlys:bite(Task).

% Scheduling :

schedule_map() ->
    lasp:declare({<<"n">>, state_gset}, state_gset),
    lasp:declare({<<"pairs">>, state_twopset}, state_twopset),
    ID = {<<"pairs">>, state_twopset},
    lists:foreach(fun(K) ->
        lasp:update(ID, {add, #{
            id => K,
            key => test,
            value => K
        }}, self())
    end, lists:seq(1, 150)).

schedule_reduce() ->
    % Wait convergence before calling the reduce
    reduce_phase([
        'achlys1@192.168.1.6',
        'achlys2@192.168.1.6'
    ]).

debug() ->
    {ok, Set} = lasp:query({<<"pairs">>, state_twopset}),
    % Pairs = sets:to_list(Set),
    % io:format("Pairs: ~p~n", [Pairs]),
    io:format("Size: ~p~n", [sets:size(Set)]).

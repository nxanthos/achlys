-module(achlys_map_reduce).

-export([
    get_size/0,
    debug/0,
    schedule_map/0,
    schedule_reduce/0
]).

reduce(Pair_1, Pair_2, ID) ->
    case {Pair_1, Pair_2} of {
        #{value := V1},
        #{value := V2}
    } -> #{
        id => ID,
        key => test,
        value => V1 + V2
    } end.

% Map phase :

map_phase() ->
    ID = {<<"pairs">>, state_twopset},
    lists:foreach(fun(K) ->
        lasp:update(ID, {add, #{
            id => K,
            key => test,
            value => K
        }}, self())
    end, lists:seq(1, 150)).

% Reduce phase :

get_pairs() ->
    {ok, Set} = lasp:query({<<"pairs">>, state_twopset}),
    List = sets:to_list(Set),
    lists:foldl(fun(Pair, Acc) ->
        case Pair of #{id := ID, key := Key, value := Value} ->
            maps:put(ID, #{
                key => Key,
                value => Value
            }, Acc)
        end
    end, #{}, List).

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

reduction_loop(Constraints) ->
    timer:sleep(rand:uniform(100)), % Simulate delay
    Pairs = get_pairs(),
    case maps:size(Pairs) of
        Size when Size > 1 ->
            reduction_step(Constraints, Pairs),
            reduction_loop(Constraints);
        Size when Size =< 1 ->
            io:format("~n~p~n", [Pairs])
    end.

reduction_step(Constraints, Pairs) ->
    ID = {<<"pairs">>, state_twopset},
    case Constraints of
        [{{A, B}, C}|T] ->
            case {
                maps:is_key(A, Pairs),
                maps:is_key(B, Pairs)
            } of {true, true} ->
                Pair_1 = maps:put(id, A, maps:get(A, Pairs)),
                Pair_2 = maps:put(id, B, maps:get(B, Pairs)),
                lasp:update(ID, {add,
                    reduce(Pair_1, Pair_2, C)
                }, self()),
                lasp:update(ID, {rmv, Pair_1}, self()),
                lasp:update(ID, {rmv, Pair_2}, self()),
                io:format("Node left: ~p~n", [get_size()]);
            _ -> reduction_step(T, Pairs) end;
        [] -> ok
    end.

reduce_phase(Nodes) ->
    Task = achlys:declare(mytask, all, single, fun() ->
        % io:format("Waiting convergence...~n"),
        % ID = {<<"pairs">>, state_twopset},
        % lasp:read(ID, {cardinality, 150}),
        io:format("Starting the task...~n"),
        Constraints = get_constraints(150),
        reduction_loop(Constraints)
    end),
    achlys:bite(Task).

% Scheduling :

schedule_map() ->
    lasp:declare({<<"n">>, state_gset}, state_gset),
    lasp:declare({<<"pairs">>, state_twopset}, state_twopset),
    map_phase().

schedule_reduce() ->
    % Wait convergence before calling the reduce
    reduce_phase([
        'achlys1@192.168.1.6',
        'achlys2@192.168.1.6'
    ]).

get_size() ->
    {ok, Set} = lasp:query({<<"pairs">>, state_twopset}),
    sets:size(Set).

debug() ->
    {ok, Set} = lasp:query({<<"pairs">>, state_twopset}),
    Pairs = sets:to_list(Set),
    io:format("Pairs: ~p~n", [Pairs]).

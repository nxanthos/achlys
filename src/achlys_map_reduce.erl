-module(achlys_map_reduce).
-define(SERVER, ?MODULE).

-export([
    schedule/2,
    get_size/0,
    debug/1
]).

% ---------------------------------------------
% Map phase :
% ---------------------------------------------

% @pre -
% @post -
add_pairs([], _, I) -> I;
add_pairs([{Key, Value}|Pairs], OVar, I) ->
    lasp:update(OVar, {add, #{
        id => I,
        key => Key,
        value => Value
    }}, self()),
    add_pairs(Pairs, OVar, I + 1);
add_pairs([_|Pairs], OVar, I) ->
    add_pairs(Pairs, OVar, I).

% @pre -
% @post -
map_phase(Entries, OVar) ->
    map_phase(Entries, OVar, 0).
map_phase([], _, I) -> I;
map_phase([{IVar, Map}|Entries], OVar, I) ->
    Values = achlys_util:query(IVar),
    Pairs = erlang:apply(Map, [Values]),
    J = add_pairs(Pairs, OVar, I),
    map_phase(Entries, OVar, J);
map_phase([_|Entries], OVar, I) ->
    map_phase(Entries, OVar, I).

% ---------------------------------------------
% Shuffle phase :
% ---------------------------------------------

% @pre -
% @post -
shuffle([], Dispatching, _) -> Dispatching;
shuffle([Pair|Pairs], Dispatching, GetOVar) ->
    case Pair of
        #{key := Key} ->
            OVar = GetOVar(Key),
            lasp:update(OVar, {add, Pair}, self()),
            #{n := N} = maps:get(Key, Dispatching, #{n => 0}),
            shuffle(Pairs, maps:put(Key, #{
                n => N + 1,
                variable => OVar
            }, Dispatching), GetOVar);
        _ -> shuffle(Pairs, Dispatching, GetOVar)
    end.

% @pre -
% @post -
shuffle_phase(IVar, GetOVar) ->
    Pairs = achlys_util:query(IVar),
    shuffle(Pairs, #{}, GetOVar).

% ---------------------------------------------
% Reduce phase :
% ---------------------------------------------

% @pre -
% @post -
get_values([]) -> [];
get_values([Pair|Pairs]) ->
    case Pair of #{value := Value} ->
        [Value|get_values(Pairs)]
    end.

% @pre -
% @post -
get_unique_id([]) -> none;
get_unique_id([Pair|Pairs]) ->
    case Pair of
        #{id := {_, N}} ->
            get_unique_id(Pairs, N);
        #{id := N} ->
            get_unique_id(Pairs, N)
    end.
get_unique_id([], Acc) -> Acc + 1;
get_unique_id([Pair|Pairs], Acc) ->
    case Pair of
        #{id := {_, N}} ->
            get_unique_id(Pairs, erlang:max(N, Acc));
        #{id := N} ->
            get_unique_id(Pairs, erlang:max(N, Acc))
    end.

% @pre -
% @post -
add_reduced_pairs(ID, Pairs, OVar) ->
    add_reduced_pairs(ID, Pairs, OVar, 0).
add_reduced_pairs(_, [], _, I) -> I;
add_reduced_pairs(ID, [Pair|Pairs], OVar, I) ->
    case Pair of
        {Key, Value} ->
            lasp:update(OVar, {add, #{
                id => ID,
                key => Key,
                value => Value
            }}, self()),
            {Group, K} = ID,
            add_reduced_pairs({Group, K + 1}, Pairs, OVar, I + 1);
        _ -> add_reduced_pairs(ID, Pairs, OVar, I)
    end.

% @pre -
% @post -
reduce_phase(Key, IVar, Reduce, OVar) ->
    {ok, Set} = lasp:query(IVar),
    case sets:size(Set) of
        Size when Size == 0 -> 0;
        Size when Size > 0 ->
            P1 = sets:to_list(Set),
            P2 = erlang:apply(Reduce, [Key, get_values(P1)]),
            N = get_unique_id(P1),
            add_reduced_pairs({Key, N}, P2, OVar)
        end.

% ---------------------------------------------
% Tasks:
% ---------------------------------------------

% @pre -
% @post -
get_cardinality(L) ->
    get_cardinality(L, 0).
get_cardinality([], Total) -> Total;
get_cardinality([{_, N}|T], Total) ->
    get_cardinality(T, Total + N).

% @pre -
% @post -
get_counter(L) -> get_counter(L, #{}).
get_counter([], Acc) -> Acc;
get_counter([{Key, N}|T], Acc) ->
    get_counter(T, maps:put(Key, N, Acc)).

% @pre -
% @post -
get_groups_with_missing_pairs([], Acc) ->
    maps:keys(Acc);
get_groups_with_missing_pairs([Pair|Pairs], Acc) ->
    case Pair of #{id := {Group, _}} ->
        case maps:get(Group, Acc) of
            N when N > 1 ->
                get_groups_with_missing_pairs(
                    Pairs,
                    maps:update(Group, N - 1, Acc)
                );
            N when N =< 1 ->
                get_groups_with_missing_pairs(
                    Pairs,
                    maps:remove(Group, Acc)
                )
        end
    end.

% @pre -
% @post -
is_irreductible(Pairs) ->
    is_irreductible(Pairs, sets:new()).
is_irreductible([], _) -> true;
is_irreductible([Pair|Pairs], Set) ->
    case Pair of #{key := Key} ->
        case sets:is_element(Key, Set) of
            true -> false;
            false -> is_irreductible(
                Pairs,
                sets:add_element(Key, Set)
            )
        end
    end.

% ---------------------------------------------
% Debugging:
% ---------------------------------------------

% @pre -
% @post -
get_size() ->
    {ok, Set} = lasp:query({<<"pairs">>, state_twopset}),
    sets:size(Set).

% @pre -
% @post -
debug(Name) ->
    {ok, Set} = lasp:query({
        erlang:list_to_binary(Name),
        state_twopset
    }),
    Content = sets:to_list(Set),
    io:format("Variable (~p) -> ~p~n", [Name, Content]).

% ---------------------------------------------
% API:
% ---------------------------------------------

% @pre -
% @post -
give_task(none, _) -> ok;
give_task({Key, #{n := N, variable := IVar}, Iterator}, Reduce) ->

    CVar = {<<"cardinality">>, state_twopset},
    OVar = {<<"output">>, state_twopset},

    Task = achlys:declare(mytask, all, single, fun() ->
        lasp:read(IVar, {cardinality, N}),
        M = reduce_phase(Key, IVar, Reduce, OVar),
        lasp:update(CVar, {add, {Key, M}}, self())
    end),

    achlys:bite(Task),
    give_task(maps:next(Iterator), Reduce).

% @pre -
% @post -
schedule(Entries, Reduce) ->

    IVar = {<<"pairs">>, state_twopset},
    CVar = {<<"cardinality">>, state_twopset},
    OVar = {<<"output">>, state_twopset},

    I = map_phase(Entries, IVar),
    lasp:read(IVar, {cardinality, I}),
    
    Dispatching = shuffle_phase(IVar, fun(Key) ->
        A = erlang:integer_to_list(1), % Round
        B = erlang:atom_to_list(Key),
        C = erlang:list_to_binary(A ++ B),
        {C, state_twopset}
    end),

    Iterator = maps:iterator(Dispatching),
    give_task(maps:next(Iterator), Reduce),

    J = maps:size(Dispatching),
    lasp:read(CVar, {cardinality, J}),
    K = get_cardinality(achlys_util:query(CVar)),

    lasp:read(OVar, {cardinality, K}),
    % TODO: Timeout ->
    Groups = get_groups_with_missing_pairs(
        achlys_util:query(OVar),
        get_counter(achlys_util:query(CVar))
    ),

    debug("pairs"),
    debug("cardinality"),
    debug("output"),

    io:format("No convergence for group:~p~n", [Groups]),
    io:format("is output irreductible ? ~p~n", [
        is_irreductible(achlys_util:query(OVar))
    ]).

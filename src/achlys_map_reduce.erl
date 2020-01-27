-module(achlys_map_reduce).
-define(SERVER, ?MODULE).

-export([
    schedule/2,
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
    Pairs = lists:foldl(fun(Value, Acc) ->
        Acc ++ erlang:apply(Map, [Value])
    end, [], Values),
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
sort_pairs(Pairs) when erlang:is_list(Pairs) -> lists:sort(Pairs);
sort_pairs(Pair) when erlang:is_tuple(Pair) -> [Pair].

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
            add_reduced_pairs({Key, N}, sort_pairs(P2), OVar)
        end.

% ---------------------------------------------
% Helpers:
% ---------------------------------------------

% @pre -
% @post -
get_total_cardinality(L) ->
    get_total_cardinality(L, 0).
get_total_cardinality([], Total) -> Total;
get_total_cardinality([{_, N}|T], Total) ->
    get_total_cardinality(T, Total + N).

% @pre -
% @post -
get_cardinality_per_key(L) ->
    get_cardinality_per_key(L, #{}).
get_cardinality_per_key([], Acc) -> Acc;
get_cardinality_per_key([{Key, N}|T], Acc) ->
    get_cardinality_per_key(T, maps:put(Key, N, Acc)).

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
is_irreductible(IVar, OVar) ->
    P1 = achlys_util:query(IVar),
    P2 = achlys_util:query(OVar),
    % TODO: When do we stop ?
    false.

% @pre -
% @post -
choose_node() ->
    {ok, Members} = partisan_peer_service:members(),
    Length = erlang:length(Members),
    Index = rand:uniform(Length),
    [lists:nth(Index, Members)].

% @pre -
% @post -
give_task(none, _, _) -> ok;
give_task(Current, Vars, Reduce) ->
    {Key, #{n := N, variable := IVar}, Iterator} = Current,
    {CVar, OVar} = Vars,
    Node = choose_node(),
    Task = achlys:declare(mytask, Node, single, fun() ->
        lasp:read(IVar, {cardinality, N}),
        I = reduce_phase(Key, IVar, Reduce, OVar),
        lasp:update(CVar, {add, {Key, I}}, self())
    end),
    achlys:bite(Task),
    give_task(maps:next(Iterator), Vars, Reduce).

% @pre -
% @post -
format_result(ID) ->
    Pairs = achlys_util:query(ID),
    lists:map(fun(Pair) ->
        % maps:without([id], Pair)
        Pair
    end, Pairs).

% @pre -
% @post -
convert_key(Key) when erlang:is_atom(Key) ->
    erlang:atom_to_list(Key);
convert_key(Key) when erlang:is_integer(Key) ->
    erlang:integer_to_list(Key).

% @pre -
% @post -
round(Vars, Round, Reduce, Options) ->
    {IVar, CVar, OVar} = Vars,
    case maps:get(max_round, Options) of
        Max when Round > Max ->
            io:format("Max round reached !~n"),
            format_result(IVar);
        _ ->
            Dispatching = shuffle_phase(IVar, fun(Key) ->
                A = erlang:integer_to_list(Round),
                B = convert_key(Key),
                C = erlang:list_to_binary(A ++ B),
                {C, state_twopset}
            end),
            Iterator = maps:iterator(Dispatching),
            give_task(maps:next(Iterator), {CVar, OVar}, Reduce),
            I = maps:size(Dispatching),
            % TODO: Add timeout here
            lasp:read(CVar, {cardinality, I}),
            J = get_total_cardinality(achlys_util:query(CVar)),
            % TODO: Add timeout here
            % Groups = get_groups_with_missing_pairs(
            %     achlys_util:query(OVar),
            %     get_cardinality_per_key(achlys_util:query(CVar))
            % ),
            lasp:read(OVar, {cardinality, J}),    
            case is_irreductible(IVar, OVar) of
                true -> format_result(OVar);
                false -> round({
                    OVar,
                    {erlang:list_to_binary(
                        erlang:integer_to_list(Round) ++ "-cvar"
                    ), state_gset},
                    {erlang:list_to_binary(
                        erlang:integer_to_list(Round) ++ "-over"
                    ), state_gset}
                }, Round + 1, Reduce, Options)
            end
    end.

% @pre -
% @post -
schedule(Entries, Reduce) ->
    IVar = {<<"0-ivar">>, state_gset},
    CVar = {<<"0-cvar">>, state_gset},
    OVar = {<<"0-ovar">>, state_gset},
    I = map_phase(Entries, IVar),
    lasp:read(IVar, {cardinality, I}),
    round({IVar, CVar, OVar}, 1, Reduce, #{
        max_round => 1
    }).

% ---------------------------------------------
% Debugging:
% ---------------------------------------------

% @pre -
% @post -
debug(Name) ->
    {ok, Set} = lasp:query({
        erlang:list_to_binary(Name),
        state_gset
    }),
    Content = sets:to_list(Set),
    io:format("Variable (~p) -> ~p~n", [Name, Content]).

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
add_pairs([], _, I) -> I - 1;
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
map_phase([], _) -> 0;
map_phase(Entries, OVar) ->
    Pairs = lists:foldl(fun(Entry, Acc1) ->
        case Entry of {IVar, Map} ->
            Values = achlys_util:query(IVar),
            Acc1 ++ lists:foldl(fun(Value, Acc2) ->
                Acc2 ++ erlang:apply(Map, [Value])
            end, [], Values)
        end
    end, [], Entries),
    add_pairs(lists:sort(Pairs), OVar, 1).

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
sort_pairs(Pairs) when erlang:is_list(Pairs) ->
    lists:sort(Pairs);
sort_pairs(Pair) when erlang:is_tuple(Pair) ->
    [Pair].

% @pre -
% @post -
has_changed(P1, P2) ->
    P3 = [{Key, Value} || #{key := Key, value := Value} <- P1],
    % io:format("P2=~p~nP3=~p~n", [P2, P3]),
    not (lists:sort(P2) =:= lists:sort(P3)).

% @pre -
% @post -
reduce_phase(Key, IVar, Reduce, OVar) ->
    {ok, Set} = lasp:query(IVar),
    case sets:size(Set) of
        Size when Size == 0 -> {0, false};
        Size when Size > 0 ->
            P1 = sets:to_list(Set),
            Values = [Value || #{value := Value} <- P1],
            P2 = erlang:apply(Reduce, [Key, Values]),
            Pairs = add_reduced_pairs(
                {Key, 1},
                sort_pairs(P2),
                OVar
            ),
            Status = has_changed(P1, P2),
            {Pairs, Status}
        end.

% ---------------------------------------------
% Helpers:
% ---------------------------------------------

% @pre -
% @post -
get_total_cardinality(L) ->
    get_total_cardinality(L, 0).
get_total_cardinality([], Total) -> Total;
get_total_cardinality([{_, Number, _}|T], Total) ->
    get_total_cardinality(T, Number + Total).

% @pre -
% @post -
get_cardinality_per_key(L) ->
    get_cardinality_per_key(L, #{}).
get_cardinality_per_key([], Acc) -> Acc;
get_cardinality_per_key([{Key, Number, _}|T], Acc) ->
    get_cardinality_per_key(T, maps:put(Key, Number, Acc)).

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
is_irreductible([]) -> true;
is_irreductible([{_, _, Flag}|T]) ->
    case Flag of
        true -> false;
        false -> is_irreductible(T)
    end.

% @pre -
% @post -
choose_node() ->
    {ok, Members} = partisan_peer_service:members(),
    Length = erlang:length(Members),
    Index = rand:uniform(Length),
    [lists:nth(Index, Members)].

% @pre -
% @post -
give_task([], _, _, _) -> ok;
give_task([Key|Keys], Dispatching, Vars, Reduce) ->
    give_task(Key, Dispatching, Vars, Reduce),
    give_task(Keys, Dispatching, Vars, Reduce);
give_task(Key, Dispatching, Vars, Reduce) ->
    #{n := N, variable := IVar} = maps:get(Key, Dispatching),
    {CVar, OVar} = Vars,
    Node = choose_node(),
    Task = achlys:declare(mytask, Node, single, fun() ->
        lasp:read(IVar, {cardinality, N}),
        {Number, Flag} = reduce_phase(Key, IVar, Reduce, OVar),
        lasp:update(CVar, {add,
            {Key, Number, Flag}
        }, self())
    end),
    achlys:bite(Task).

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
round(Round, Vars, Reduce, Options) ->
    {IVar, CVar, OVar} = Vars,
    case maps:get(max_round, Options) of
        Max when Round > Max ->
            io:format("Max round reached !~n"),
            format_result(IVar);
        _ ->
            Dispatching = shuffle_phase(IVar, fun(Key) ->
                Name = erlang:list_to_binary(
                    erlang:integer_to_list(Round) ++ convert_key(Key)
                ), 
                {Name, state_gset}
            end),

            Keys = maps:keys(Dispatching),
            give_task(Keys, Dispatching, {CVar, OVar}, Reduce),
            I = maps:size(Dispatching),

            % TODO: Add timeout here

            {ok, {_, _, _, {_, Status}}} = lasp:read(CVar, {cardinality, I}),
            J = get_total_cardinality(Status),
            
            % TODO: Add timeout here
            % Groups = get_groups_with_missing_pairs(
            %     achlys_util:query(OVar),
            %     get_cardinality_per_key(Status)
            % ),
            % give_task(Groups, Dispatching, {CVar, OVar}, Reduce),
            % io:format("Missing pairs for group~p~n", [Groups]),
            
            lasp:read(OVar, {cardinality, J}),    
            case is_irreductible(Status) of
                true ->
                    io:format("Round ~p~n", [Round]), 
                    format_result(OVar);
                false ->
                    NextIVar = OVar,
                    NextCVar = {erlang:list_to_binary(
                        erlang:integer_to_list(Round) ++ "-cvar"
                    ), state_gset},
                    NextOVar = {erlang:list_to_binary(
                        erlang:integer_to_list(Round) ++ "-over"
                    ), state_gset},
                    round(
                        Round + 1,
                        {NextIVar, NextCVar, NextOVar},
                        Reduce,
                        Options
                    )
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
    round(1, {IVar, CVar, OVar}, Reduce, #{
        max_round => 10
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

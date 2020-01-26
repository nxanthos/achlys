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
shuffle_pairs([], Counter, _) -> Counter;
shuffle_pairs([Pair|Pairs], Counter, GetOVar) ->
    case Pair of
        #{key := Key} ->
            OVar = GetOVar(Key),
            lasp:update(OVar, {add, Pair}, self()),
            #{n := N} = maps:get(Key, Counter, #{n => 0}),
            shuffle_pairs(Pairs, maps:put(Key, #{
                n => N + 1,
                variable => OVar
            }, Counter), GetOVar);
        _ -> shuffle_pairs(Pairs, Counter, GetOVar)
    end.

% @pre -
% @post -
shuffle_phase(IVar, Round) ->
    Pairs = achlys_util:query(IVar),
    shuffle_pairs(Pairs, #{}, fun(Key) ->
        A = erlang:integer_to_list(Round),
        B = erlang:atom_to_list(Key),
        C = erlang:list_to_binary(A ++ B),
        {C, state_twopset}
    end).

% ---------------------------------------------
% Reduce phase :
% ---------------------------------------------

% @pre -
% @post -
get_cardinalities([], Acc) -> Acc;
get_cardinalities([{_, N}|Cardinalities], Acc) ->
    get_cardinalities(Cardinalities, N + Acc).

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
            io:format("P1=~p~n", [P1]),
            io:format("P2=~p~n", [P2]),
            add_reduced_pairs({Key, N}, P2, OVar)
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
schedule(Entries, Reduce) ->

    Total = map_phase(Entries, {
        <<"pairs">>,
        state_twopset
    }),

    lasp:read(
        {<<"pairs">>, state_twopset},
        {cardinality, Total}
    ),

    % debug("pairs"),
    % io:format("Total=~p~n", [Total]),
    % io:format("Size=~p~n", [get_size()]),

    Round = 1,
    Counter = shuffle_phase({<<"pairs">>, state_twopset}, Round),
    Keys = maps:keys(Counter),

    io:format("Counter=~p~n", [Counter]),
    io:format("Keys=~p~n", [Keys]),

    lists:foreach(fun(Key) ->
        case maps:is_key(Key, Counter) of true ->
            #{variable := IVar, n := N} = maps:get(Key, Counter),
            lasp:read(IVar, {cardinality, N}),

            OVar = {<<"output">>, state_twopset},
            M = reduce_phase(Key, IVar, Reduce, OVar),
            
            io:format("~p~n", [M]),
            debug("output")
            % lasp:read(OVar, {cardinality, M})
        end
    end, Keys),
    ok.

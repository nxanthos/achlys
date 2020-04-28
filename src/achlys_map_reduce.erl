-module(achlys_map_reduce).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).
-define(TYPE, state_gset).

-export([
    schedule/2
]).

% @pre -
% @post -
get_values(ID) ->
    % TODO: Add more types
    case ID of
        {_, state_gset} ->
            {ok, Set} = lasp:query(ID),
            sets:to_list(Set)
    end.

% @pre -
% @post -
sort_pairs(Pairs) when erlang:is_list(Pairs) ->
    lists:sort(Pairs);
sort_pairs(Pair) when erlang:is_tuple(Pair) ->
    [Pair].

% @pre -
% @post -
give_ids(Pairs) ->
    lists:foldl(fun({Key, Value}, {K, Tuples}) ->
        Tuple = #{
            id => K,
            key => Key,
            value => Value
        },
        {K + 1, [Tuple|Tuples]}
    end, {0, []}, Pairs).

% @pre -
% @post -
give_ids(Batch, Pairs) ->
    lists:foldl(fun({Key, Value}, {K, Tuples}) ->
        Tuple = #{
            id => {Batch, K},
            key => Key,
            value => Value
        },
        {K + 1, [Tuple|Tuples]}
    end, {0, []}, Pairs).

% @pre -
% @post -
convert_key(Key) when erlang:is_atom(Key) ->
    erlang:atom_to_list(Key);
convert_key(Key) when erlang:is_integer(Key) ->
    erlang:integer_to_list(Key);
convert_key(Key) when erlang:is_list(Key) ->
    Key.

% @pre -
% @post -
choose_node() ->
    {ok, Members} = partisan_peer_service:members(),
    Length = erlang:length(Members),
    Index = rand:uniform(Length),
    [lists:nth(Index, Members)].

% @pre -
% @post -
gen_var(Generator, Name, Args) ->
    Fun = maps:get(Name, Generator),
    case Args of
        L when erlang:is_list(L) ->
            erlang:apply(Fun, Args);
        E ->
            erlang:apply(Fun, [E])
    end.

% @pre -
% @post -
gen_task_name() ->
    case lasp_unique:unique() of
        {ok, Name} -> Name
    end.

% @pre -
% @post -
get_default_name_generator() ->
    case lasp_unique:unique() of
        {ok, Seed} ->
            #{
                ivar => fun(Round) ->
                    Prefix = erlang:list_to_binary(
                        "ivar-" ++ erlang:integer_to_list(Round) ++ "-"
                    ),
                    {<<Prefix/binary, Seed/binary>>, ?TYPE}
                end,
                svar => fun(Round) ->
                    Prefix = erlang:list_to_binary(
                        "svar-" ++ erlang:integer_to_list(Round) ++ "-"
                    ),
                    {<<Prefix/binary, Seed/binary>>, ?TYPE}
                end,
                ovar => fun(Round) ->
                    Prefix = erlang:list_to_binary(
                        "ovar-" ++ erlang:integer_to_list(Round) ++ "-"
                    ),
                    {<<Prefix/binary, Seed/binary>>, ?TYPE}
                end,
                batch => fun(Round, Key) ->
                    Prefix = erlang:list_to_binary(
                        "batch-" ++ erlang:integer_to_list(Round) ++ "-" ++ convert_key(Key) ++ "-"
                    ),
                    {<<Prefix/binary, Seed/binary>>, ?TYPE}
                end
            }
    end.

% @pre -
% @post -
read(ID, N) ->
    fun() ->
        {ok, {_, _, _, {_, Values}}} = lasp:read(ID, {cardinality, N}),
        Values
    end.

% ---------------------------------------------
% Map phase :
% ---------------------------------------------

% @pre -
% @post -
map_pairs(Entries) ->
    lists:foldl(fun({IVar, Map}, Acc1) ->
        Values = get_values(IVar),
        lists:foldl(fun(Value, Acc2) ->
            Pairs = erlang:apply(Map, [Value]),
            lists:foldl(fun(Pair, Acc3) ->
                case Pair of {_, _} -> [Pair|Acc3];
                _ -> Acc3 end
            end, Acc2, Pairs)
        end, Acc1, Values)
    end, [], Entries).

% @pre -
% @post -
map_phase([], _) -> 0;
map_phase(Entries, OVar) ->
    P1 = map_pairs(Entries),
    P2 = sort_pairs(P1),
    {N, P3} = give_ids(P2),
    lasp:bind(OVar, {?TYPE, P3}),
    N.

% ---------------------------------------------
% Shuffle phase :
% ---------------------------------------------

% @pre -
% @post -
get_partitions(Pairs) ->
    Partitions = lists:foldl(fun(Pair, Acc) ->
        case Pair of #{key := Key} ->
            orddict:append(Key, Pair, Acc)
        end
    end, orddict:new(), Pairs),
    orddict:to_list(Partitions).

% @pre -
% @post -
shuffle_phase(Pairs, Generator, Round) ->
    Partitions = get_partitions(Pairs),
    lists:foldl(fun({Key, Partition}, Dispatching) ->
        OVar = gen_var(Generator, batch, [Round, Key]),
        lasp:bind(OVar, {?TYPE, Partition}),
        maps:put(Key, #{
            n => erlang:length(Partition),
            variable => OVar
        }, Dispatching)
    end, maps:new(), Partitions).

% ---------------------------------------------
% Reduce phase :
% ---------------------------------------------

% @pre -
% @post -
reduce_pairs(Batch, Pairs, Reduce) ->
    Values = [Value || #{value := Value} <- Pairs],
    L = erlang:apply(Reduce, [Batch, Values, false]),
    lists:foldl(fun(Pair, Acc) ->
        case Pair of {_, _} ->
            [Pair|Acc];
        _ -> Acc end
    end, [], L).

% @pre -
% @post -
has_changed(P1, P2) ->
    P3 = sort_pairs([{Key, Value} || #{key := Key, value := Value} <- P1]),
    not (P2 =:= P3). % not exactly equal

% @pre -
% @post -
reduce_phase(_, [], _, _) -> {0, false};
reduce_phase(Batch, Pairs, Reduce, OVar) ->
    P1 = reduce_pairs(Batch, Pairs, Reduce),
    P2 = sort_pairs(P1),
    {N, P3} = give_ids(Batch, P2),
    lasp:bind(OVar, {?TYPE, P3}),
    {N, has_changed(Pairs, P2)}.

% ---------------------------------------------
% Helpers:
% ---------------------------------------------

% @pre -
% @post -
get_number_of_produced_pairs(Status) ->
    lists:foldl(fun(Entry, Total) ->
        case Entry of
            {_, N, _} -> Total + N;
            _ -> Total
        end
    end, 0, Status).

% @pre -
% @post -
is_irreductible(L) ->
    not lists:any(fun({_, _, Flag}) -> Flag end, L).

% @pre -
% @post -
give_task(Batch, N, {IVar, SVar, OVar}, Reduce) ->
    Node = choose_node(),
    Name = gen_task_name(),
    Task = achlys:declare(Name, Node, single, fun() ->
        io:format("Starting the reduction~n"),
        Options = #{
            max_attempts => 10,
            timeout => 1000
        },
        case promise:all([
            {read(IVar, N), []}
        ], Options) of
            max_attempts_reached ->
                io:format("Error: Max attempts reached - Could not get the input variable~n");
            {ok, Results} ->
                Pairs = lists:flatmap(fun({_, Pair}) -> Pair end, orddict:to_list(Results)),
                {Cardinality, Flag} = reduce_phase(Batch, Pairs, Reduce, OVar),
                lasp:update(SVar, {add,
                    {Batch, Cardinality, Flag}
                }, self())
        end
    end),
    achlys:bite(Task).

% @pre -
% @post -
dispatch_tasks(Batches, Dispatching, Vars, Reduce) ->
    {SVar, OVar} = Vars,
    lists:foreach(fun(Batch) ->
        case maps:get(Batch, Dispatching) of #{n := N, variable := IVar} ->
            give_task(Batch, N, {IVar, SVar, OVar}, Reduce)
        end
    end, Batches).

% @pre -
% @post -
get_unresponsive_batches(SVar, Batches) ->
    Status = achlys_util:query(SVar),
    lists:subtract(Batches, [Batch || {Batch, _, _} <- Status]).

% @pre -
% @post -
get_incomplete_batches(OVar, Status) ->
    Pairs = achlys_util:query(OVar),
    Cardinalities = lists:foldl(fun(Pair, Acc) ->
        case Pair of
            #{id := {Batch, _}, key := _, value := _} ->
                case maps:is_key(Batch, Acc) of
                    true ->
                        maps:update_with(Batch, fun(N) ->
                            N + 1
                        end, Acc);
                    false ->
                        maps:put(Batch, 1, Acc)
                end;
            _ -> Acc
        end
    end, #{}, Pairs),
    lists:foldl(fun({Batch, Cardinality, _}, Batches) ->
        case Cardinality - maps:get(Batch, Cardinalities, 0) of
            N when N > 0 -> [Batch|Batches];
            _ -> Batches
        end
    end, [], Status).

% @pre -
% @post -
round(Round, Pairs, Generator, Reduce, Options) ->

    Dispatching = shuffle_phase(Pairs, Generator, Round),
    Batches = maps:keys(Dispatching),

    SVar = gen_var(Generator, svar, Round),
    OVar = gen_var(Generator, ovar, Round),
    Vars = {SVar, OVar},

    dispatch_tasks(Batches, Dispatching, Vars, Reduce),

    I = maps:size(Dispatching),

    case promise:all([
        {read(SVar, I), []}
    ], fun(Tasks) ->
        FaultyBatches = get_unresponsive_batches(SVar, Batches),
        dispatch_tasks(FaultyBatches, Dispatching, Vars, Reduce),
        Tasks
    end, Options) of
        max_attempts_reached ->
            io:format("Error: Max attempts reached - Could not the status of each node~n"),
            [];
        {ok, R1} ->
            Status = lists:flatmap(fun({_, Pair}) -> Pair end, orddict:to_list(R1)),
            J = get_number_of_produced_pairs(Status),
            case promise:all([
                {read(OVar, J), []}
            ], fun(Tasks) ->
                FaultyBatches = get_incomplete_batches(OVar, Status),
                dispatch_tasks(FaultyBatches, Dispatching, Vars, Reduce),
                Tasks
            end, Options) of
                max_attempts_reached ->
                    io:format("Error: Max attempts reached - Could not get the output pairs~n"),
                    [];
                {ok, R2} ->
                    NewPairs = lists:flatmap(fun({_, Pair}) -> Pair end, orddict:to_list(R2)),
                    case is_irreductible(Status) of
                        true ->
                            NewPairs;
                        false ->
                            start_round(
                                Round + 1,
                                NewPairs,
                                Generator,
                                Reduce,
                                Options
                            )
                    end
            end
    end.

% @pre -
% @post -
start_round(Round, Pairs, Generator, Reduce, Options) ->
    case maps:get(max_round, Options) of
        Max when Round < Max ->
            io:format("Starting round n°~p~n", [Round]),
            round(Round, Pairs, Generator, Reduce, Options);
        _ ->
            io:format("Warning: Too many rounds!~n"),
            Pairs
    end.

% @pre -
% @post -
schedule(Entries, Reduce) ->
    schedule(Entries, Reduce, #{
        max_round => 10,
        max_attempts => 3,
        max_batch_cardinality => 50,
        timeout => 1000
    }).

% @pre -
% @post -
schedule(Entries, Reduce, Options) ->

    Generator = get_default_name_generator(),
    IVar = gen_var(Generator, ivar, 0),
    N = map_phase(Entries, IVar),

    case promise:all([
        {read(IVar, N), []}
    ], fun(Tasks) ->
        map_phase(Entries, IVar),
        Tasks
    end, Options) of
        max_attempts_reached ->
            io:format("Error: Max attempts reached - Could not map the input variables~n"),
            [];
        {ok, Results} ->
            Pairs = lists:flatmap(fun({_, Pair}) -> Pair end, orddict:to_list(Results)),
            start_round(1, Pairs, Generator, Reduce, Options)
    end.

% ---------------------------------------------
% EUnit tests:
% ---------------------------------------------

-ifdef(TEST).

% @pre -
% @post -
give_ids_test() ->
    {A, B} = give_ids([
        {a, 1},
        {b, 2},
        {a, 3}
    ]),
    ?assertEqual(A, 3),
    ?assertEqual(B, [
        #{id => 2, key => a, value => 3},
        #{id => 1, key => b, value => 2},
        #{id => 0, key => a, value => 1}
    ]),
    {C, D} = give_ids([
        {b, 2},
        {a, 1},
        {a, 3}
    ]),
    ?assertEqual(C, 3),
    ?assertEqual(D, [
        #{id => 2, key => a, value => 3},
        #{id => 1, key => a, value => 1},
        #{id => 0, key => b, value => 2}
    ]),
    ok.

-endif.

% To launch the tests:
% rebar3 eunit --module=achlys_map_reduce
-module(achlys_map_reduce).
-define(SERVER, ?MODULE).
-define(TIMEOUT, 1000).

-export([
    schedule/2,
    debug/1
]).

% ---------------------------------------------
% Map phase :
% ---------------------------------------------

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
remove_invalid_pairs(Pairs) ->
    lists:filter(fun(Pair) -> 
        case Pair of
            {_, _} -> true;
            _ -> false
        end
    end, Pairs).

% @pre -
% @post -
get_pairs({IVar, Map}) ->
    Values = get_values(IVar),
    lists:foldl(fun(Value, Pairs) ->
        case erlang:apply(Map, [Value]) of
            Result when erlang:is_list(Result) ->
                Pairs ++ remove_invalid_pairs(Result);
            {_, _} = Pair ->
                Pairs ++ [Pair]
        end
    end, [], Values).

% @pre -
% @post -
sort_pairs(Pairs) when erlang:is_list(Pairs) ->
    lists:sort(Pairs);
sort_pairs(Pair) when erlang:is_tuple(Pair) ->
    [Pair].

% @pre -
% @post -
map_phase([], _) -> 0;
map_phase(Entries, OVar) ->
    P1 = lists:foldl(fun(Entry, Pairs) ->
        Pairs ++ get_pairs(Entry)
    end, [], Entries),
    P2 = sort_pairs(P1),
    lists:foldl(fun(Pair, K) ->
        {Key, Value} = Pair,
        lasp:update(OVar, {add, #{
            id => K,
            key => Key,
            value => Value
        }}, self()),
        K + 1
    end, 0, P2).

% ---------------------------------------------
% Shuffle phase :
% ---------------------------------------------

% @pre -
% @post -
shuffle_phase(Pairs, Generator, Round) ->
    lists:foldl(fun(Pair, Dispatching) ->
        #{id := _, key := Key, value := _} = Pair,
        OVar = gen_var(Generator, batch, [Round, Key]),
        lasp:update(OVar, {add, Pair}, self()),
        case maps:is_key(Key, Dispatching) of
            true ->
                maps:update_with(Key, fun(Info) ->
                    maps:update_with(n, fun(N) ->
                        N + 1
                    end, Info)
                end, Dispatching);
            false ->
                maps:put(Key, #{
                    n => 1,
                    variable => OVar
                }, Dispatching)
        end
    end, #{}, Pairs).

% ---------------------------------------------
% Reduce phase :
% ---------------------------------------------

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
reduce_phase(_, [], _, _) -> {0, false};
reduce_phase(Batch, Pairs, Reduce, OVar) ->
    Values = [Value || #{value := Value} <- Pairs],
    P1 = erlang:apply(Reduce, [Batch, Values, false]),
    P2 = remove_invalid_pairs(P1),
    P3 = sort_pairs(P2),
    N = lists:foldl(fun(Pair, K) ->
        {Key, Value} = Pair,
        lasp:update(OVar, {add, #{
            id => {Batch, K},
            key => Key,
            value => Value
        }}, self()),
        K + 1
    end, 0, P3),
    P4 = sort_pairs([{Key, Value} || #{key := Key, value := Value} <- Pairs]),
    {N, not (P3 =:= P4)}.

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
give_task(Batch, N, {IVar, SVar, OVar}, Reduce) ->
    Node = choose_node(),
    Name = gen_task_name(),
    Task = achlys:declare(Name, Node, single, fun() ->
        % io:format("Starting the reduction~n"),
        case await(read(IVar, N)) of
            {error, timeout} ->
                io:format("Error: Could not get the input variable~n"),
                [];
            {ok, Pairs} ->
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
await(Action) ->
    Pid = self(),
    erlang:spawn(fun() ->
        Self = self(),
        erlang:spawn(fun() ->
            Self ! Action()
        end),
        receive Result ->
            Pid ! {ok, Result}
        after ?TIMEOUT ->
            Pid ! {error, timeout}
        end
    end),
    receive Result -> Result end.
await(_, _, 0) -> {error, max_attempts_reached};
await(Action, OnTimeout, K) -> 
    case await(Action) of
        {error, timeout} ->
            OnTimeout(),
            await(Action, OnTimeout, K);
        Response -> Response
    end.

% @pre -
% @post -
read(ID, N) ->
    fun() ->
        {ok, {_, _, _, {_, Values}}} = lasp:read(ID, {cardinality, N}),
        Values
    end.

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

    SVar = gen_var(Generator, svar, Round),
    OVar = gen_var(Generator, ovar, Round),
    Vars = {SVar, OVar},

    Dispatching = shuffle_phase(Pairs, Generator, Round),
    Batches = maps:keys(Dispatching),

    dispatch_tasks(Batches, Dispatching, Vars, Reduce),
    
    % Debug :
    % lists:foreach(fun(Status) ->
    %     case Status of
    %         {_, #{variable := ID}} ->
    %             debug(ID)
    %     end
    % end, maps:to_list(Dispatching)),

    I = maps:size(Dispatching),

    case await(read(SVar, I), fun() ->
        io:format("Resending task to unresponsive batches~n"),
        FaultyBatches = get_unresponsive_batches(SVar, Batches),
        dispatch_tasks(FaultyBatches, Dispatching, Vars, Reduce)
    end, maps:get(max_attempts, Options)) of
        {error, _} ->
            io:format("Error: Could not get the status of each node~n"),
            [];
        {ok, Status} ->
            J = get_number_of_produced_pairs(Status),
            case await(read(OVar, J), fun() ->
                io:format("Resending task to incomplete batches~n"),
                FaultyBatches = get_incomplete_batches(OVar, Status),
                dispatch_tasks(FaultyBatches, Dispatching, Vars, Reduce)
            end, maps:get(max_attempts, Options)) of
                {error, _} ->
                    io:format("Error: Could not get the output pairs~n"),
                    [];
                {ok, Result} ->
                    case is_irreductible(Status) of
                        true ->
                            % io:format("OVar = ~w Result = ~w~n", [OVar, Result]),
                            Result;
                        false ->
                            start_round(
                                Round + 1,
                                Result,
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
gen_var(Generator, Name, Args) ->
    Fun = maps:get(Name, Generator),
    case Args of
        [_|_] -> erlang:apply(Fun, Args);
        _ -> erlang:apply(Fun, [Args])
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
            Type = state_gset,
            #{
                ivar => fun(Round) ->
                    Prefix = erlang:list_to_binary(
                        "ivar-" ++ erlang:integer_to_list(Round) ++ "-"
                    ),
                    {<<Prefix/binary, Seed/binary>>, Type}
                end,
                svar => fun(Round) ->
                    Prefix = erlang:list_to_binary(
                        "svar-" ++ erlang:integer_to_list(Round) ++ "-"
                    ),
                    {<<Prefix/binary, Seed/binary>>, Type}
                end,
                ovar => fun(Round) ->
                    Prefix = erlang:list_to_binary(
                        "ovar-" ++ erlang:integer_to_list(Round) ++ "-"
                    ),
                    {<<Prefix/binary, Seed/binary>>, Type}
                end,
                batch => fun(Round, Key) ->
                    Prefix = erlang:list_to_binary(
                        "batch-" ++ erlang:integer_to_list(Round) ++ "-" ++ convert_key(Key) ++ "-"
                    ),
                    {<<Prefix/binary, Seed/binary>>, Type}
                end
            }
    end.

% @pre -
% @post -
schedule(Entries, Reduce) ->
    schedule(Entries, Reduce, #{
        max_round => 10,
        max_attempts => 5
    }).

% @pre -
% @post -
schedule(Entries, Reduce, Options) ->

    Generator = get_default_name_generator(),
    IVar = gen_var(Generator, ivar, 0),
    N = map_phase(Entries, IVar),

    case await(read(IVar, N), fun() ->
        map_phase(Entries, IVar)
    end, maps:get(max_attempts, Options)) of
        {error, _} ->
            io:format("Error: Could not map the input variables~n"),
            [];
        {ok, Pairs} ->
            start_round(1, Pairs, Generator, Reduce, Options)
    end.


% ---------------------------------------------
% Debugging:
% ---------------------------------------------

% @pre -
% @post -
debug(ID) ->
    {ok, Set} = lasp:query(ID),
    Values = sets:to_list(Set),
    io:format("Variable (~w) -> ~w~n", [ID, Values]).

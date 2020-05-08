-module(achlys_mr).
-behavior(gen_server).

-export([
    handle_cast/2,
    handle_call/3,
    handle_info/2,
    init/1
]).

-export([
    schedule/2,
    schedule/3,
    start_link/0
]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, orddict:new()}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({schedule, Process, Entries, Reduce, Options}, State) ->
    case lasp_unique:unique() of
        {ok, ID} ->
            Pid = erlang:spawn(fun() ->
                Pairs = map_phase(Entries),
                case reduce_phase(ID, Pairs, Reduce, Options) of
                    {error, Reason} ->
                        gen_server:cast(?MODULE, {free, ID}),
                        Process ! {error, Reason};
                    {ok, Result} ->
                        Process ! {ok, Result}
                end
            end),
            NextState = orddict:store(ID, Pid, State),
            {noreply, NextState}
    end;
handle_cast({on_reduce, Message}, State) ->
    case Message of #{
        header := #{
            src := _Src
        },
        payload := #{
            id := ID,
            batch := Name,
            pairs := Pairs
        }
    } ->
        case orddict:find(ID, State) of
            {ok, Pid} ->
                Pid ! {ok, {Name, Pairs}};
            _ ->
                io:format("The task no longer exists: The message has been dropped~n"),
                ok
        end
    end,
    {noreply, State};
handle_cast({free, ID}, State) ->
    case orddict:find(ID, State) of {ok, Pid} ->
        erlang:exit(Pid, kill)
    end,
    NextState = orddict:erase(ID, State),
    {noreply, NextState};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Request, State) ->
    {noreply, State}.

% Map phase:

% @pre -
% @post -
map_phase(Entries) ->
    lists:flatmap(fun({IVar, Map}) ->
        Values = achlys_util:query(IVar),
        lists:flatmap(fun(Value) ->
            erlang:apply(Map, [Value])
        end, Values)
    end, Entries).

% Reduce phase :

% @pre -
% @post -
is_irreductible(InputPairs, OutputPairs) ->
    lists:sort(InputPairs) == lists:sort(OutputPairs).

% @pre -
% @post -
reduce_phase(ID, Pairs, Reduce, Options) ->
    start_round(ID, 1, Pairs, Reduce, Options).

% @pre -
% @post -
start_round(ID, Round, InputPairs, Reduce, Options) ->
    MaxRound = maps:get(max_round, Options),
    case Round =< MaxRound of
        false ->
            {error, "Max round reached"};
        true ->
            case get_reduction(ID, InputPairs, Reduce, Options) of
                {error, Reason} ->
                    {error, Reason};
                {ok, OutputPairs} ->
                    case is_irreductible(InputPairs, OutputPairs) of
                        true ->
                            {ok, OutputPairs};
                        false ->
                            start_round(
                                ID,
                                Round + 1,
                                OutputPairs,
                                Reduce,
                                Options
                            )
                    end
            end
    end.

% @pre -
% @post - This function should return a list of tuples:
%       [
%           {ID, [{Key 1, Value 1}, {Key 2, Value 2}]},
%           {ID, [{Key 3, Value 3}]}
%       ]
get_batches(InputPairs, Options) ->
    Groups = lists:foldl(fun(Pair, Orddict) ->
        case Pair of {Key, _} ->
            orddict:append(Key, Pair, Orddict)
        end
    end, orddict:new(), InputPairs),
    Fun = fun({_, Pairs}) ->
        Name = erlang:unique_integer([monotonic, positive]),
        {Name, Pairs}
    end,
    lists:map(Fun, orddict:to_list(Groups)).

% @pre -
% @post -
choose_node() ->
    case achlys_util:get_neighbors() of
        [] -> [];
        [H|_T] -> H
    end.

% @pre -
% @post - This function should return a list of tuples
%       [
%           {{Batch ID, [Node 1]},
%           {{Batch ID, [Node 1, Node 2]}
%       ]
dispatch_tasks(ID, Batches, Reduce) ->
    Dispatching = orddict:new(),
    dispatch_tasks(ID, Batches, Dispatching, Reduce).

dispatch_tasks(ID, Batches, Dispatching, Reduce) ->
    lists:foldl(fun({Name, Pairs}, Acc) ->
        case choose_node() of
            [] ->
                MySelf = self(),
                erlang:send(achlys_mr_worker, {reduce, #{
                    header => #{
                        src => MySelf
                    },
                    payload => #{
                        batch => Name,
                        pairs => Pairs,
                        reduce => Reduce
                    }
                }}),
                orddict:append(Name, self(), Acc);
            Node ->
                % TODO: Avoid sending the task to the same node
                MySelf = achlys_util:myself(),
                partisan_peer_service:cast_message(
                    Node,
                    achlys_mr_worker,
                    {reduce, #{
                        header => #{
                            src => MySelf
                        },
                        payload => #{
                            id => ID,
                            batch => Name,
                            pairs => Pairs,
                            reduce => Reduce
                        }
                    }}
                ),
                orddict:append(Name, Node, Acc)
        end
    end, Dispatching, Batches).

% @pre -
% @post -
get_reduction(ID, InputPairs, Reduce, Options) ->
    Batches = get_batches(InputPairs, Options),
    Dispatching = dispatch_tasks(ID, Batches, Reduce),
    N = maps:get(max_attempts, Options),
    Acc = {Batches, Dispatching, []},
    % io:format("Batches: ~p~n", [Batches]),
    % io:format("Dispatching: ~p~n", [Dispatching]),
    receive_all(N, ID, Reduce, Options, Acc).

% @pre -
% @post -
receive_all(K, _ID, _Reduce, _Options, _Acc) when K =< 0 ->
    {error, max_attempts_reached};
receive_all(K, ID, Reduce, Options, Acc) ->
    case receive_all(Acc, Options) of
        {ok, OutputPairs} ->
            {ok, OutputPairs};
        {timeout, {Batches, Dispatching, Pairs}} ->
            io:format("Timeout: Retrying~n"),
            receive_all(K - 1, ID, Reduce, Options, {
                Batches,
                dispatch_tasks(ID, Batches, Dispatching, Reduce),
                Pairs
            })
    end.

% @pre -
% @post - Acc = {Batches, Dispatching, Pairs}
receive_all(Acc, Options) ->
    Timeout = maps:get(timeout, Options),
    {Batches, Dispatching, OutputPairs} = Acc,
    case orddict:is_empty(Dispatching) of
        true ->
            {ok, OutputPairs};
        false ->
            receive {ok, {Name, Pairs}} ->
                receive_all({
                    orddict:erase(Name, Batches),
                    orddict:erase(Name, Dispatching),
                    OutputPairs ++ Pairs
                }, Options)
            after Timeout ->
                {timeout, Acc}
            end
    end.

% @pre -
% @post -
schedule(Entries, Reduce) ->
    io:format("Starting the map reduce~n"),
    schedule(Entries, Reduce, #{
        max_round => 10,
        max_attempts => 3,
        max_batch_cardinality => 2,
        timeout => 3000
    }).

% @pre -
% @post -
schedule(Entries, Reduce, Options) ->
    gen_server:cast(?MODULE, {schedule,
        self(),
        Entries,
        Reduce,
        Options
    }),
    receive
        {ok, Result} ->
            Result;
        {error, Reason} ->
            io:format("Error: ~p~n", [Reason]),
            []
    end.

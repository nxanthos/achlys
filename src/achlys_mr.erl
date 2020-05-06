-module(achlys_mr).
-behavior(gen_server).

-export([
    handle_cast/2,
    handle_call/3,
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
                start_round(
                    Process,
                    ID,
                    1,
                    Pairs,
                    Reduce,
                    Options
                )
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
        case orddict:find(ID, State) of {ok, Pid} ->
            Pid ! {ok, {Name, Pairs}}
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
% @post - This function should return a list of tuples:
%       {
%           {Name of the batch, [{Key 1, Value 1}, {Key 2, Value 2}]},
%           {Name of the batch, [{Key 3, Value 3}]},
%       }
get_batches(InputPairs) ->
    Groups = lists:foldl(fun(Pair, Orddict) ->
        case Pair of {Key, _} ->
            orddict:append(Key, Pair, Orddict)
        end
    end, orddict:new(), InputPairs),
    orddict:to_list(Groups).

% @pre -
% @post -
choose_node() ->
    L = achlys_util:get_neighbors(),
    erlang:hd(L).

% @pre -
% @post - This function should return a list of tuples
%       {
%           {Name of the batch, [Node 1]},
%           {Name of the batch, [Node 1, Node 2]}
%       }
reduce_phase(ID, Batches, Reduce) ->
    lists:foldl(fun(Batch, Dispatching) ->
        {Name, Pairs} = Batch,
        Node = choose_node(),
        partisan_peer_service:cast_message(
            Node,
            achlys_mr_worker,
            {reduce, #{
                header => #{
                    src => achlys_util:myself()
                },
                payload => #{
                    id => ID,
                    batch => Name,
                    pairs => Pairs,
                    reduce => Reduce
                }
            }}
        ),
        orddict:append(Name, Node, Dispatching)
    end, orddict:new(), Batches).

% @pre -
% @post -
has_changed(InputPairs, OutputPairs) ->
    lists:sort(InputPairs) =:= lists:sort(OutputPairs).

% @pre -
% @post -
start_round(Process, ID, Round, Pairs, Reduce, Options) ->
    MaxRound = maps:get(max_round, Options),
    case Round =< MaxRound of
        false ->
            Process ! {error, "Max round reached"};
        true ->
            round(
                Process,
                ID,
                Round,
                Pairs,
                Reduce,
                Options
            )
    end.

% @pre -
% @post -
round(Process, ID, Round, InputPairs, Reduce, Options) ->
    Batches = get_batches(InputPairs),
    Dispatching = reduce_phase(ID, Batches, Reduce),

    % TODO: Add a retry mechanism
    % TODO: Get batch of limited size

    OutputPairs = receive_all([], Dispatching),
    case has_changed(InputPairs, OutputPairs) of
        true ->
            gen_server:cast(?MODULE, {free, ID}),
            Process ! {ok, OutputPairs};
        false ->
            start_round(
                Process,
                ID,
                Round + 1,
                OutputPairs,
                Reduce,
                Options
            )
    end.

% @pre -
% @post -
receive_all(Acc, Dispatching) ->
    case orddict:is_empty(Dispatching) of
        true -> Acc;
        false ->
            receive {ok, {Name, Pairs}} ->
                receive_all(
                    Pairs ++ Acc,
                    orddict:erase(Name, Dispatching)
                )
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
        timeout => 2000
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
        {ok, Pairs} ->
            Pairs;
        {error, Reason} ->
            io:format("Error: ~p~n", [Reason]),
            []
    end.

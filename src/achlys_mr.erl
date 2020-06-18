-module(achlys_mr).
-behavior(gen_server).
-define(TYPE, state_gset).
-define(GC_INTERVAL, 20000).

% achlys_mr:test().
% achlys_mr:debug().

-export([
    handle_cast/2,
    handle_call/3,
    handle_info/2,
    start_link/0,
    init/1
]).

% API:
-export([
    schedule/2,
    schedule/3,
    gc/0,
    debug/0
]).

-export([get_delay/0]).

% =============================================
% Records:
% =============================================

-record(observer_struct, {
    timer :: identifier(),
    round = 0 :: non_neg_integer(),
    pairs = [] :: list(),
    reduce :: function(),
    options :: map(),
    finished = false :: boolean()
}).

-record(master_struct, {
    timer :: identifier(),
    worker :: identifier(),
    round = 0 :: non_neg_integer(),
    finished = false :: boolean()
}).

% @pre -
% @post -
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% @pre -
% @post -
init([]) ->
    erlang:send_after(?GC_INTERVAL, ?MODULE, gc),
    {ok, orddict:new()}.

% Handle cast:

% @pre  Args is a list of arguments
% @post Start a new MapReduce and the current node is the master
handle_cast({schedule, [_Entries, _Reduce, _Options] = Args}, State) ->
    case lasp_unique:unique() of
        {ok, ID} ->
            case achlys_mr_job:start_link(ID, Args) of
                {ok, Worker} ->
                    io:format("The node becomes a master~n"),
                    {noreply, orddict:store(ID, #master_struct{
                        timer = erlang:spawn(fun() ->
                            repeat(fun() ->
                                % Send heartbeat message
                                achlys_plumtree_broadcast:broadcast(
                                    {achlys_mr, {notify, #{heartbeat => #{
                                        src => achlys_util:myself(),
                                        id => ID
                                    }}}},
                                    achlys_plumtree_backend
                                )
                            end, 1000)
                        end),
                        worker = Worker
                    }, State)}
            end
    end;

% @pre  ID is the identifier of the MapReduce
%       State is the state of the gen_server
% @post If the node is observer, it becomes master
handle_cast({continue, ID}, State) ->
    case orddict:find(ID, State) of
        {ok, #master_struct{}} ->
            % Local execution
            io:format("A process is already running~n"),
            {noreply, State};
        {ok, #observer_struct{
            timer = Timer,
            round = Round,
            pairs = Pairs,
            reduce = Reduce,
            options = Options
        }} ->
            % Remote execution
            io:format("The node becomes a master~n"),
            clear_timeout(Timer),
            Args = [Round, Pairs, Reduce, Options],
            case achlys_mr_job:start_link(ID, Args) of
                {ok, Worker} ->
                    {noreply, orddict:store(ID, #master_struct{
                        worker = Worker,
                        round = Round
                    }, State)}
            end;
        _ ->
            {noreply, State}
    end;

% @pre  ID is the identifier of a MapReduce
% @post Stop/Delete the MapReduce
handle_cast({stop, ID}, State) ->
    io:format("The reduction has been completed by another master~n"),
    logger:info("[MAPREDUCE][~p][F]", [ID]),
    case orddict:find(ID, State) of
        {ok, #master_struct{
            worker = Worker,
            timer = Timer
        }} ->
            % Local execution
            clear_timeout(Worker),
            clear_timeout(Timer),
            {noreply, orddict:erase(ID, State)};
        {ok, #observer_struct{
            timer = Timer
        }} ->
            % Remote execution
            clear_timeout(Timer),
            {noreply, orddict:erase(ID, State)};
        _ ->
            {noreply, State}
    end;

% @pre  ID is the identifier of the MapReduce
%       Args is a list of arguments: [Src, Round, Pairs, Reduce, Options]
% @post Update the state of the node after receiving a message from a master M:
%           If the  node is master: and M is more advanced, the node becomes an observer
%           If the node is an observer: the node restart the timer (to become master)
%           If the node is in the initial state: the node becomes an observer
handle_cast({update, ID, Args}, State) ->
    [Src, Round, Pairs, Reduce, Options] = Args,
    case orddict:find(ID, State) of
        % The node is a master for this MapReduce (ID) and it receive a message from another master
        {ok, #master_struct{
            worker = Worker,
            timer = Timer
        } = Struct} ->
            % Local execution
            case Round > Struct#master_struct.round of 
                true -> % The other master is more advanced in the reduction, this node become an observer
                    io:format("The node becomes an observer~n"),
                    logger:info("[MAPREDUCE][~p][M][~p]", [ID, Src]), % The node Src is the master
                    logger:info("[MAPREDUCE][~p][R][~p]", [ID, Round]),
                    clear_timeout(Worker),
                    clear_timeout(Timer),
                    Delay = get_delay(),
                    Timer = set_timeout(fun() ->
                        logger:info("[MAPREDUCE][~p][T][~p]", [ID, Src]),
                        gen_server:cast(?MODULE, {continue, ID})
                    end, [], Delay),
                    {noreply, orddict:store(ID, #observer_struct{
                        timer = Timer,
                        round = Round,
                        pairs = Pairs,
                        reduce = Reduce,
                        options = Options
                    }, State)};
                false -> % ignore the message
                    {noreply, State}
            end;
        % The node is an observer for this MapReduce (ID) and receive a message from a master
        {ok, #observer_struct{
            timer = Timer
        } = Struct} ->
            % Remote execution
            case Round > Struct#observer_struct.round of
                true ->
                    io:format("The timer has been reset~n"),
                    logger:info("[MAPREDUCE][~p][M][~p]", [ID, Src]),
                    logger:info("[MAPREDUCE][~p][R][~p]", [ID, Round]),
                    clear_timeout(Timer),
                    Delay = get_delay(),
                    NewTimer = set_timeout(fun() ->
                        logger:info("[MAPREDUCE][~p][T][~p]", [ID, Src]),
                        gen_server:cast(?MODULE, {continue, ID})
                    end, [], Delay),
                    {noreply, orddict:store(
                        ID,
                        Struct#observer_struct{
                            timer = NewTimer,
                            round = Round,
                            pairs = Pairs
                        },
                        State
                    )};
                false -> % ignore the message
                    {noreply, State}
            end;
        % The node receive a message from a master that start a new MapReduce (ID). 
        % The node become an observer
        _ ->
            io:format("A new MapReduce has been started by another node~n"),
            logger:info("[MAPREDUCE][~p][M][~p]", [ID, Src]),
            logger:info("[MAPREDUCE][~p][R][~p]", [ID, Round]),
            Delay = get_delay(),
            Timer = set_timeout(fun() ->
                logger:info("[MAPREDUCE][~p][T][~p]", [ID, Src]),
                gen_server:cast(?MODULE, {continue, ID})
            end, [], Delay),
            {noreply, orddict:store(ID, #observer_struct{
                timer = Timer,
                round = Round,
                pairs = Pairs,
                reduce = Reduce,
                options = Options
            }, State)}
    end;

% @pre  Message is a message casted by a remote node, 2 types of messages:
%       1. Result message: it is the result of the round.
%       2. Heartbeat message: is a keep alive message
% @post 1.  If the reduce is finish (Finished is true): get the result and stop the MapReduce
%           If the reduce is not finish: continue the reduction
%       2.  The node restart the timer (to become master)
handle_cast({notify, Message}, State) ->
    case Message of #{
        header := #{
            src := Src
        },
        payload := #{
            id := ID,
            round := Round,
            pairs := Pairs,
            reduce := Reduce,
            options := Options,
            finished := Finished
        }} ->
        case Finished of
            true ->
                io:format("A remote master has completed the reduction~n"),
                logger:info("[MAPREDUCE][~p][Result][~p]", [ID, Pairs]),
                OVar = maps:get(variable, Options),
                bind_output_var(OVar, Pairs),
                gen_server:cast(?MODULE, {stop, ID});
            false ->
                Args = [Src, Round, Pairs, Reduce, Options],
                gen_server:cast(?MODULE, {update, ID, Args})
        end,
        {noreply, State};
    #{heartbeat := #{src := Src, id := ID}} ->
        % io:format("Heartbeat ~n"),
        case orddict:find(ID, State) of
            {ok, #observer_struct{ timer = Timer } = Struct} ->
                % Reset the timer
                clear_timeout(Timer),
                Delay = get_delay(),
                NewTimer = set_timeout(fun() ->
                    logger:info("[MAPREDUCE][~p][T][~p]", [ID, Src]),
                    gen_server:cast(?MODULE, {continue, ID})
                end, [], Delay),
                {noreply, orddict:store(
                    ID,
                    Struct#observer_struct{
                        timer = NewTimer
                    },
                    State
                )};
            _ -> {noreply, State}
        end;
    _ ->
        io:format("Unknown message: Dropping the message"),
        {noreply, State}
    end;

% @pre -
% @post -
handle_cast(debug, State) ->
    io:format("State=~p~n", [State]),
    {noreply, State};

% @pre -
% @post -
handle_cast(_Request, State) ->
    {noreply, State}.

% Handle call:

% @pre -
% @post -
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

% Handle info:

% @pre -
% @post -
handle_info({new_round, ID, Round}, State) ->
    case orddict:find(ID, State) of
        {ok, #master_struct{} = Struct} ->
            io:format("A new round has been completed (~p)~n", [Round]),
            {noreply, orddict:store(ID, Struct#master_struct{
                round = Round
            }, State)};
        _ ->
            {noreply, State}
    end;

% @pre -
% @post -
handle_info({finish, ID, OVar, Pairs}, State) ->
    case orddict:find(ID, State) of
        {ok, #master_struct{} = Struct} ->
            bind_output_var(OVar, Pairs),
            {noreply, orddict:store(ID, Struct#master_struct{
                finished = true
            }, State)};
        _ ->
            {noreply, State}
    end;

% @pre -
% @post -
handle_info(gc, State) ->
    % io:format("Starting the garbage collection~n"),
    erlang:send_after(?GC_INTERVAL, ?MODULE, gc),
    {noreply, orddict:filter(fun(_Key, Struct) ->
        case Struct of
            #master_struct{
                finished = Finished
            } -> not Finished;
            #observer_struct{
                finished = Finished
            } -> not Finished
        end
    end, State)};

% @pre -
% @post -
handle_info(_Request, State) ->
    {noreply, State}.

% API:

% @pre  Entries is a list of Entrie composed of a variable and a map function {var, map_fun}
%       Reduce is a function
% @post Call schedule/3 by adding Options
schedule(Entries, Reduce) ->
    schedule(Entries, Reduce, #{
        max_round => 10,
        max_attempts => 3,
        max_batch_cardinality => 2,
        timeout => 3000,
        variable => {<<"ovar">>, state_gset}
    }).

% @pre  Entries is a list of tuples composed of a Variable and a map function {Var, fun}
%       Reduce is a reduce function
%       Options is map containing MapReduce options
% @post Start a MapReduce and get the result
schedule(Entries, Reduce, Options) ->
    gen_server:cast(?MODULE, {schedule,
        [Entries, Reduce, Options]
    }),
    maps:get(variable, Options).

% @pre -
% @post -
debug() ->
    gen_server:cast(?MODULE, debug).

% @pre -
% @post -
gc() ->
    Pid = erlang:whereis(?MODULE),
    erlang:send(Pid, gc).

% Helpers:

% @pre  Fun is the function to run when timeout
%       Args are the arguments of the function Fun
%       Delay is the time in ms before the timeout
% @post Spawn a new process with a timer, after the Delay (timeout), Fun is run
set_timeout(Fun, Args, Delay) ->
    erlang:spawn(fun() ->
        timer:sleep(Delay),
        erlang:apply(Fun, Args)
    end).

% @pre Timer is a process
% @post The process Timer is kill
clear_timeout(Timer) ->
    case erlang:is_process_alive(Timer) of
        true ->
            erlang:exit(Timer, kill);
        false ->
            {noreply}
    end.

% @pre -
% @post -
get_delay() ->
    Min = 1000,
    Max = 10000,
    Min + erlang:trunc(rand:uniform() * ((Max - Min) + 1)).

% @pre -
% @post -
bind_output_var(Var, Pairs) ->
    N = erlang:length(Pairs),
    lasp:bind(Var, {state_gset, lists:zip(
        lists:seq(1, N),
        lists:sort(Pairs)
    )}),
    io:format("Pairs=~p~n", [Pairs]).

% @pre -
% @post -
repeat(Fun, Delay) ->
    receive
    after Delay ->
        erlang:apply(Fun, []),
        repeat(Fun, Delay)
    end.

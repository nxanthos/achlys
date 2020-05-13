-module(achlys_mr).
-behavior(gen_server).
-define(TYPE, state_gset).

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
    test/0,
    debug/0
]).

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
    worker :: identifier(),
    round = 0 :: non_neg_integer()
}).

% @pre -
% @post -
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% @pre -
% @post -
init([]) ->
    {ok, orddict:new()}.

% Handle cast:

% @pre -
% @post -
handle_cast({schedule, [_Entries, _Reduce, _Options] = Args}, State) ->
    case lasp_unique:unique() of
        {ok, ID} ->
            case achlys_mr_job:start_link(ID, Args) of
                {ok, Worker} ->
                    {noreply, orddict:store(ID, #master_struct{
                        worker = Worker
                    }, State)}
            end
    end;

% @pre -
% @post -

handle_cast({continue, ID}, State) ->
    case orddict:find(ID, State) of
        {ok, #master_struct{}} ->
            io:format("A process is already running~n"),
            {noreply, State};
        {ok, #observer_struct{
            timer = Timer,
            round = Round,
            pairs = Pairs,
            reduce = Reduce,
            options = Options
        }} ->
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

% @pre -
% @post -
handle_cast({stop, ID}, State) ->
    io:format("The reduction is over~n"),
    case orddict:find(ID, State) of
        {ok, #master_struct{
            worker = Worker
        }} ->
            % Local execution
            erlang:exit(Worker, kill),
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

% @pre -
% @post -
handle_cast({update, ID, Args}, State) ->
    [Round, Pairs, Reduce, Options] = Args,
    case orddict:find(ID, State) of
        {ok, #master_struct{
            worker = Worker
        } = Struct} ->
            % Local execution
            case Round > Struct#master_struct.round of
                true ->
                    erlang:exit(Worker, kill),
                    Delay = get_delay(Options),
                    Timer = set_timeout(fun() ->
                        gen_server:cast(?MODULE, {continue, ID})
                    end, [], Delay),
                    {noreply, orddict:store(ID, #observer_struct{
                        timer = Timer,
                        round = Round,
                        pairs = Pairs,
                        reduce = Reduce,
                        options = Options
                    }, State)};
                false ->
                    {noreply, State}
            end;
        {ok, #observer_struct{
            timer = Timer
        } = Struct} ->
            % Remote execution
            case Round > Struct#observer_struct.round of
                true ->
                    clear_timeout(Timer),
                    Delay = get_delay(Options),
                    NewTimer = set_timeout(fun() ->
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
                false ->
                    {noreply, State}
            end;
        _ ->
            Delay = get_delay(Options),
            Timer = set_timeout(fun() ->
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

% @pre -
% @post -
handle_cast({notify, Message}, State) ->
    case Message of #{
        header := #{
            src := _Src
        },
        payload := #{
            id := ID,
            round := Round,
            pairs := Pairs,
            reduce := Reduce,
            options := Options,
            finished := Finished
        }
    } ->
        case Finished of
            true ->
                OVar = maps:get(variable, Options),
                bind_output_var(OVar, Pairs),
                gen_server:cast(?MODULE, {stop, ID});
            false ->
                Args = [Round, Pairs, Reduce, Options],
                gen_server:cast(?MODULE, {update, ID, Args})
        end,
        {noreply, State};
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
    io:format("New round started: Round n°~p~n", [Round]),
    case orddict:find(ID, State) of
        {ok, #master_struct{} = Struct} ->
            io:format("Updating the round~n"),
            {noreply, orddict:store(ID, Struct#master_struct{
                round = Round
            }, State)};
        _ ->
            {noreply, State}
    end;

% @pre -
% @post -
handle_info(_Request, State) ->
    {noreply, State}.

% API:

% @pre -
% @post -
schedule(Entries, Reduce) ->
    schedule(Entries, Reduce, #{
        max_round => 10,
        max_attempts => 3,
        max_batch_cardinality => 2,
        timeout => 3000,
        variable => {<<"ovar">>, state_gset}
    }).

% @pre -
% @post -
schedule(Entries, Reduce, Options) ->
    gen_server:cast(?MODULE, {schedule,
        [Entries, Reduce, Options]
    }),
    maps:get(variable, Options).

% @pre -
% @post -
debug() ->
    gen_server:cast(?MODULE, debug).

% Helpers:

% @pre -
% @post -
set_timeout(Fun, Args, Delay) ->
    erlang:spawn(fun() ->
        timer:sleep(Delay),
        erlang:apply(Fun, Args)
    end).

% @pre -
% @post -
clear_timeout(Timer) ->
    erlang:exit(Timer, kill).

% @pre -
% @post -
get_delay(Options) ->
    Min = 1000,
    Max = 3000,
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
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
    debug/0
]).

% TODO: Define record and use erlang:is_record/2

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
                    {noreply, orddict:store(ID, #{
                        worker => Worker,
                        round => 0
                    }, State)}
            end
    end;

% @pre -
% @post -
handle_cast({continue, ID}, State) ->
    case orddict:find(ID, State) of
        {ok, #{worker := _Worker}} ->
            io:format("A process is already running~n"),
            {noreply, State};
        {ok, #{
            round := Round,
            pairs := Pairs,
            reduce := Reduce,
            options := Options
        } = Entry} ->
            io:format("The node becomes a master~n"),
            clear_timeout(maps:get(timer, Entry)),
            Args = [Round, Pairs, Reduce, Options],
            case achlys_mr_job:start_link(ID, Args) of
                {ok, Worker} ->
                    {noreply, orddict:store(ID, #{
                        worker => Worker,
                        round => Round
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
        {ok, #{worker := Worker}} ->
            % Local execution
            erlang:exit(Worker, kill),
            {noreply, orddict:erase(ID, State)};
        {ok, #{timer := Timer}} ->
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
        {ok, #{worker := Worker} = Entry} ->
            % Local execution
            case Round > maps:get(round, Entry) of
                true ->
                    erlang:exit(Worker, kill),
                    Delay = get_delay(Options),
                    Timer = set_timeout(fun() ->
                        gen_server:cast(?MODULE, {continue, ID})
                    end, [], Delay),
                    {noreply, orddict:store(ID, #{
                        timer => Timer,
                        round => Round,
                        pairs => Pairs,
                        reduce => Reduce,
                        options => Options
                    }, State)};
                false ->
                    {noreply, State}
            end;
        {ok, Entry} ->
            % Remote execution
            case Round > maps:get(round, Entry) of
                true ->
                    clear_timeout(maps:get(timer, Entry)),
                    Delay = get_delay(Options),
                    Timer = set_timeout(fun() ->
                        gen_server:cast(?MODULE, {continue, ID})
                    end, [], Delay),
                    {noreply, orddict:store(
                        ID,
                        maps:merge(Entry, #{
                            timer => Timer,
                            round => Round,
                            pairs => Pairs
                        }),
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
            {noreply, orddict:store(ID, #{
                timer => Timer,
                round => Round,
                pairs => Pairs,
                reduce => Reduce,
                options => Options
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
        {ok, #{worker := Worker}} ->
            io:format("Updating the round~n"),
            {noreply, orddict:store(ID, #{
                worker => Worker,
                round => Round
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
    1000.

% @pre -
% @post -
bind_output_var(Var, Pairs) ->
    N = erlang:length(Pairs),
    lasp:bind(Var, {state_gset, lists:zip(
        lists:seq(1, N),
        lists:sort(Pairs)
    )}),
    io:format("Pairs=~p~n", [Pairs]).
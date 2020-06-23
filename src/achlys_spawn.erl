-module(achlys_spawn).
-behaviour(gen_server).

-define(MAX_RUNNING, 1).
-define(MAX_SCHEDULE, 1).

-export([
    init/1,
    start_link/0,
    handle_cast/2,
    handle_call/3,
    handle_info/2,
    terminate/2
]).

-export([
    schedule/2,
    schedule/3,
    debug/0
]).

% =============================================
% Records:
% =============================================

-record(task, {
    id :: any(),
    function :: function(),
    arguments = [] :: list(),
    callback :: function(),
    hops = [] :: list()
}).

-record(forwarded_task, {
    task :: #task{},
    timer :: identifier()
}).

-record(running_task, {
    task :: #task{},
    pid :: identifier()
}).

-record(header, {
    src :: any()
}).

-record(message, {
    header :: #header{},
    body :: any()
}).


% scheduled_tasks = [task1, task2]
% running_tasks = orddict(#{ID1 => running_task, ID2 => running_task, ...})
% forwarded_tasks = orddict(#{ID1 => forwarded_task, ID2 => forwarded_task, ...})

% @pre -
% @post -
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% @pre -
% @post -
init([]) ->
    {ok, #{
        scheduled_tasks => queue:new(),
        running_tasks => orddict:new(),
        forwarded_tasks => orddict:new()
    }}.

% @pre -
% @post -
schedule_task(Task, State) ->
    maps:update_with(scheduled_tasks, fun(Q) ->
        queue:in(Task, Q)
    end, State).

% @pre -
% @post -
create_running_task(Task) ->
    case Task of #task{
        id = ID,
        function = Fun,
        arguments = Args
    } ->
        Pid = erlang:spawn(fun() ->
            Result = erlang:apply(Fun, Args),
            gen_server:cast(?MODULE, {return, ID, Result}),
            io:format("The task will be executed~n")
        end),
        #running_task{
            task = Task,
            pid = Pid
        }
    end.

% @pre -
% @post -
run_task(N, State) when N > 0 ->
    Q1 = maps:get(scheduled_tasks, State),
    case queue:out(Q1) of
        {empty, _Q2} ->
            State;
        {{value, Task}, Q2} ->
            ID = Task#task.id,
            run_task(N - 1, State#{
                running_tasks := orddict:store(
                    ID,
                    create_running_task(Task),
                    maps:get(running_tasks, State)
                ),
                scheduled_tasks := Q2
            })
    end;
run_task(_N, State) -> State.

% @pre -
% @post -
run_tasks(State) ->
    case State of
        #{running_tasks := Running} ->
            N = ?MAX_RUNNING - orddict:size(Running),
            run_task(N, State)
    end.

% @pre -
% @post -
create_forwarded_task(Task) ->
    Myself = achlys_util:myself(),
    case achlys_spawn_monitor:choose_node(Task#task.hops) of
        Node when Node == Myself ->
            undefined;
        Node ->
            io:format("Forwarded Task~n"),
            send(Node, {schedule, Task#task{
                hops = [achlys_util:myself()|Task#task.hops]
            }}), 
            achlys_spawn_monitor:on_schedule(Node, Task#task.id),
            Timer = set_timeout(fun() ->
                io:format("The timer has timeout~n"),
                gen_server:cast(?MODULE, {retry, Task}),
                achlys_spawn_monitor:on_timeout(Node, Task#task.id)
            end, [], 3000),
            #forwarded_task{
                task = Task,
                timer = Timer
            }
    end.

% @pre -
% @post -
forward_task(N, State) when N > 0 ->
    Q1 = maps:get(scheduled_tasks, State),
    case queue:out_r(Q1) of
        {empty, _Q2} ->
            State;
        {{value, Task}, Q2} ->
            ID = Task#task.id,
            case create_forwarded_task(Task) of
                undefined ->
                    State;
                Forwarded_task ->
                    forward_task(N - 1, State#{
                        scheduled_tasks := Q2,
                        forwarded_tasks := orddict:store(
                            ID,
                            Forwarded_task,
                            maps:get(forwarded_tasks, State)
                        )
                    })
            end
    end;
forward_task(_N, State) -> State.

% @pre -
% @post -
forward_tasks(State) ->
    case State of
        #{scheduled_tasks := Q} ->
            N = queue:len(Q) - ?MAX_SCHEDULE,
            forward_task(N, State)
    end.

% @pre -
% @post -
remove_from_forwarded(ID, Result, State) ->
    case State of #{forwarded_tasks := Forwarded} ->
        case orddict:find(ID, Forwarded) of
            {ok, #forwarded_task{
                timer = Timer,
                task = #task{
                    callback = Callback,
                    hops = []
                }
            }} ->
                kill(Timer),
                erlang:apply(Callback, [Result]),
                forward_tasks(State#{
                    forwarded_tasks := orddict:erase(
                        ID,
                        Forwarded
                    )
                });
            {ok, #forwarded_task{
                timer = Timer,
                task = #task{
                    hops = [Node|_Nodes]
                }
            }} ->
                kill(Timer),
                send(Node, {return, ID, Result}),
                forward_tasks(State#{
                    forwarded_tasks := orddict:erase(
                        ID, 
                        Forwarded
                    )
                });
            error -> State
        end
    end.

% @pre -
% @post -
remove_from_running(ID, Result, State) ->
    case State of #{running_tasks := Running} ->
        case orddict:find(ID, Running) of
            {ok, #running_task{
                pid = Pid,
                task = #task{
                    callback = Callback,
                    hops = []
                }
            }} ->
                kill(Pid),
                erlang:apply(Callback, [Result]),
                run_tasks(State#{
                    running_tasks := orddict:erase(
                        ID,
                        Running
                    )
                });
            {ok, #running_task{
                pid = Pid,
                task = #task{
                    hops = [Node|_Nodes]
                }
            }} ->
                kill(Pid),
                send(Node, {return, ID, Result}),
                run_tasks(State#{
                    running_tasks := orddict:erase(
                        ID,
                        Running
                    )
                });
            error -> State
        end
    end.

% =================================================
% Handle cast:
% =================================================================

handle_cast(#message{header = Header, body = Body}, State) ->
    gen_server:cast(?MODULE, Body),
    case Body of
        {return, ID, _Result} ->
            Node = Header#header.src,
            achlys_spawn_monitor:on_return(Node, ID);
        _ -> ok
    end,
    {noreply, State};

% @pre -
% @post -
handle_cast({schedule, Task}, State) ->
    S1 = schedule_task(Task, State),
    S2 = run_tasks(S1),
    S3 = forward_tasks(S2),
    {noreply, S3};

% @pre -
% @post -
handle_cast({return, ID, Result}, State) ->
    S1 = remove_from_forwarded(ID, Result, State),
    S2 = remove_from_running(ID, Result, S1),
    {noreply, S2};

% @pre -
% @post -
handle_cast({retry, Task}, State) ->
    io:format("Re-scheduling the task~n"),
    case State of #{
        scheduled_tasks := Scheduled,
        forwarded_tasks := Forwarded
    } ->
        ID = Task#task.id,
        S1 = run_tasks(State#{
            scheduled_tasks := queue:in_r(Task, Scheduled),
            forwarded_tasks := orddict:erase(ID, Forwarded)
        }),
        S2 = forward_tasks(S1),
        {noreply, S2}
    end;

% @pre -
% @post print the State
handle_cast(debug, State) ->
    io:format("State: ~p~n", [State]),
    case State of #{running_tasks := Running} ->
        N = lists:foldl(fun({_, Running_task}, Count) ->
            Pid = Running_task#running_task.pid,
            case erlang:is_process_alive(Pid) of
                true -> Count + 1;
                false -> Count
            end
        end, 0, orddict:to_list(Running)),
        % io:format("Running queue size: ~p", []),
        % io:format("Scheduled queue size: ~p", []),
        % io:format("Forwarded queue size: ~p", []),
        io:format("Number of alive processes: ~p~n", [N])
    end,
    {noreply, State};

handle_cast(Message, State) ->
    io:format("Unknown message~p~n", [Message]),
    {noreply, State}.

% Call:

handle_call(_Request, _From, State) ->
    {noreply, State}.

% Info:

% @pre -
% @post -
handle_info(_Info, State) ->
    {noreply, State}.

% Terminate:

% @pre -
% @post -
terminate(_Reason, State) ->
    case State of #{
        running_tasks := Running,
        forwarded_tasks := Forwarded
    } ->
        ok
        % TODO
        % lists:foreach(fun(RunningTask) -> end, Running)
        % lists:foreach(fun(ForwardedTask) -> end, Forwarded),
    end.

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

% @pre Pid is a process
% @post The process Pid is killed
kill(Pid) ->
    case Pid of
        undefined -> {noreply};
        _ -> erlang:exit(Pid, kill)
    end.

% API:

% @pre  Fun is a function
%       Args is a list of arguments for Fun
%       Callback is a function
% @post Create a task containing Fun, Args and Callback and schedule it
schedule(Fun, Args, Callback) ->
    gen_server:cast(?MODULE, {schedule, #task{
        id = erlang:unique_integer(),
        function = Fun,
        arguments = Args,
        callback = Callback
    }}).

% @pre -
% @post -
schedule(Fun, Args) ->
    Self = self(),
    schedule(Fun, Args, fun(Result) ->
        Self ! Result
    end),
    receive Results ->
        Results
    end.

% @pre -
% @post -
send(Node, Message) ->
    partisan_peer_service:cast_message(Node, ?MODULE, #message{
        header = #header{
            src = achlys_util:myself()
        },
        body = Message
    }).

% @pre -
% @post -
debug() ->
    gen_server:cast(?MODULE, debug).

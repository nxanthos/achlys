-module(achlys_spawn).
-behaviour(gen_server).

-export([
    init/1,
    start_link/0,
    handle_cast/2,
    handle_call/3,
    handle_info/2
]).

-export([
    daemon/0,
    schedule/2,
    schedule/3,
    debug/0
]).

% =============================================
% Records:
% =============================================

-record(task, {
    function :: function(),
    arguments = [] :: list(),
    callback :: function()
}).

-record(forwarded_task, {
    id :: identifier(),
    timer :: identifier(),
    function :: function(),
    arguments = [] :: list(),
    hops = [] :: list(),
    callback :: function()
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    % Add timer here
    {ok, #{
        running_tasks => #{},
        scheduled_tasks => #{},
        forwarded_tasks => #{}
    }}.

% Cast:

% @pre Task is a new task without ID
% @post generate an ID and call schedule
handle_cast({schedule, Task}, State) ->
    gen_server:cast(?MODULE, {schedule, erlang:unique_integer(), Task}),
    {noreply, State};

% @pre 'Task' is a task or a forwarded_task. 
% @post The server will dispatch the task in one of the list of tasks:
%   'running': tasks proceed by the node (10 slots)
%   'scheduled': list of tasks waiting to be proceed (10 slots)
%   'forwarded': list of tasks that have been sent to other nodes (IFF the two other lists are full)
handle_cast({schedule, ID, Task}, State) ->
    case State of #{running_tasks := Running, scheduled_tasks := Scheduled, forwarded_tasks := Forwarded} ->
        io:format("A task has been scheduled~n"),
        case maps:size(Running) < 10 of
            true ->
                io:format("Task added in running list~n"),
                {noreply, State#{
                    running_tasks := maps:put(ID, Task, Running)
                }};
            false ->
                case maps:size(Scheduled) < 10 of
                    true ->
                        io:format("Task added in scheduled list~n"),
                        {noreply, State#{
                            scheduled_tasks := maps:put(ID, Task, Scheduled)
                        }};
                    false ->
                        io:format("Task added in forward list~n"),
                        case Task of
                            #task{} ->
                                Forwarded_task = #forwarded_task{
                                    id = ID,
                                    function = Task#task.function,
                                    arguments = Task#task.arguments,
                                    callback = Task#task.callback
                                },
                                {noreply, State#{forwarded_tasks := maps:put(ID, Forwarded_task, Forwarded)}};
                            #forwarded_task{} ->
                                {noreply, State#{forwarded_tasks := maps:put(ID, Task, Forwarded)}}
                        end
                end
        end
    end;

% @pre ID is the identifier of a task, State is the state of the gen_server
% @post Execute the task 'ID' locally
%       Create a new process for this task
handle_cast({execute, ID}, State) ->
    case State of #{running_tasks := Running} ->
        Task = maps:get(ID, Running),
        case Task of
            #task{function = Fun, arguments = Args} ->
                io:format("Executing the task~n"),
                erlang:spawn(fun() ->
                    Result = erlang:apply(Fun, Args),
                    gen_server:cast(?MODULE, {return, ID, Result})
                end);
            #forwarded_task{function = Fun, arguments = Args} ->
                io:format("Executing a forwarded task ~n"),
                erlang:spawn(fun() ->
                    Result = erlang:apply(Fun, Args),
                    gen_server:cast(?MODULE, {return, ID, Result})
                end)
        end,
        {noreply, State}
    end;
% @pre  ID is the identifier of a task
%       Result is the result of the task
% @post if the task was forwarded by a remote node 'Node': unicast the result to 'Node'
%       if the task is a local task: execute the callback and update the lists of tasks

handle_cast({return, Local_ID, Result}, State) ->
    case State of #{running_tasks := Running} ->
        Task = maps:get(Local_ID, Running),
        case Task of
            #forwarded_task{hops = [Node|_]} -> % if the task is not a local task but a task forwarded by the node Node
                Remote_ID = Task#forwarded_task.id,
                io:format("Returning the result to the original node~n"),
                % unicast the result (respond) to the node that sent the task
                partisan_peer_service:cast_message(
                    Node,
                    achlys_spawn,
                    {return_forward, Remote_ID, Result}
                );
            #task{callback = Callback} -> % local task
                io:format("Task ~p return a result: ~n", [Local_ID]),
                erlang:apply(Callback, [Result]),
                gen_server:cast(?MODULE, {update}); % update the lists of tasks
            _ -> io:format("ERROR: The task is not a TASK")
        end,
        {noreply, State#{running_tasks := maps:remove(Local_ID, Running)}} % remove the ended task
    end;

% @pre  ID is the identifier of a forwarded task
%       Result is the result of the forwarded task ID
% @post if the task was scheduled by another node 'Node': unicast the result to 'Node'
%       if the task was forwarded by this node: execute the callback and remove it from the forwarded_tasks
handle_cast({return_forward, Local_ID, Result}, State) ->
    case State of #{running_tasks := Running, scheduled_tasks := Scheduled, forwarded_tasks := Forwarded} ->
        Task =  maps:get(Local_ID, Forwarded),
        case Task of 
            #forwarded_task{hops = [Node|_]} -> % if the task is not a local task but a task forwarded by the node Node
                Remote_ID = Task#forwarded_task.id,
                io:format("Returning the result to the original node~n"),
                % unicast the result (respond) to the node that sent the task
                partisan_peer_service:cast_message(
                    Node,
                    achlys_spawn,
                    {return_forward, Remote_ID, Result}
                );
            #forwarded_task{timer = Timer, hops = [], callback = Callback} ->
                io:format("Forwarded task ~p return a result ~n", [Local_ID]),
                clear_timeout(Timer),
                erlang:apply(Callback, [Result]);
            _ -> io:format("ERROR: The task is not a FORWARDED_TASK")
        end,
        {noreply, State#{forwarded_tasks := maps:remove(Local_ID, Forwarded)}}
        % case maps:is_key(ID, Forwarded) of 
        %     true ->
        %       Task = map:get(ID, Forwarded), ...
        %     false ->
        %       io:format("The result is not accepted (wrong destination or delete task because timeout ~n)"),
        %       {noreply, State}
        % end
    end;

% @pre State is the state of the gen_server
% @post Update the lists of task:
%   if the scheduled_tasks is not empty: take the first task of scheduled_tasks and add it to running_tasks
%   if the scheduled_tasks is empty: do nothing

handle_cast({update}, State) ->
    case State of #{running_tasks := Running, scheduled_tasks := Scheduled} ->
        case maps:size(Scheduled) > 0 of
            true -> % there are tasks in Schedule, we have move a task from Scheduled to Running
                [New_ID|_] = maps:keys(Scheduled), % get the first task's key (=ID) of Scheduled
                New_running_task = maps:get(New_ID, Scheduled), % get the first task of Scheduled
                gen_server:cast(?MODULE, {execute, New_ID}), % execute the new task
                {noreply, State#{running_tasks := maps:put(New_ID, New_running_task, Running), scheduled_tasks := maps:remove(New_ID, Scheduled)}};
            
            false -> % There is no tasks in Schedule
                {noreply, State}
        end
    end;

% @pre  ID is the identifier of a task
%       Node is a neighbor of the current node
% @post the task 'ID' is forwarded to 'Node'

handle_cast({forward, ID, Node}, State) ->
    case State of #{forwarded_tasks := Forwarded} ->
        Task = maps:get(ID, Forwarded),
        % After timeout the task is delete from forwarded_tasks and rescheduled
        Timer = set_timeout(fun() -> 
            io:format("TIMEOUT ~n"),
            gen_server:cast(?MODULE, {remove, ID, forwarded_tasks}),
            gen_server:cast(?MODULE, {schedule, ID, Task#forwarded_task{
                hops = [Node|Task#forwarded_task.hops]
            }})
        end, [], 5000),
        % update the task by adding the timeout
        % gen_server:cast(?MODULE, {remove, ID, forwarded_tasks}),
        % gen_server:cast(?MODULE, {put, ID, Task#forwarded_task{timer = Timer}, forwarded_tasks}),
        gen_server:cast(?MODULE, {update, ID, Task#forwarded_task{timer = Timer}, forwarded_tasks}),
        partisan_peer_service:cast_message(
            Node,
            achlys_spawn, 
            {schedule, Task#forwarded_task{id = ID, hops = [get_name()|Task#forwarded_task.hops]}} % add this node to the hops
        ),
        io:format("The task has been forwarded~n")
    end,
    {noreply, State};

% @pre  ID is the identifier of a task
%       List is a list of tasks (running_tasks, scheduled_tasks or forwarded_tasks)
% @post Remove the Task ID from List

handle_cast({remove, ID, List}, State) ->
    case State of #{running_tasks := Running, scheduled_tasks := Scheduled, forwarded_tasks := Forwarded} ->
        case List of
            running_tasks -> {noreply, State#{running_tasks := maps:remove(ID, Running)}};
            scheduled_tasks -> {noreply, State#{scheduled_tasks := maps:remove(ID, Scheduled)}};
            forwarded_tasks -> {noreply, State#{forwarded_tasks := maps:remove(ID, Forwarded)}}
        end
    end;

% @pre  ID is the identifier of a task
%       List is a list of tasks (running_tasks, scheduled_tasks or forwarded_tasks)
% @post Add the Task ID from List
handle_cast({put, ID, Task, List}, State) ->
    case State of #{running_tasks := Running, scheduled_tasks := Scheduled, forwarded_tasks := Forwarded} ->
        case List of
            running_tasks -> {noreply, State#{running_tasks := maps:put(ID, Task, Running)}};
            scheduled_tasks -> {noreply, State#{scheduled_tasks := maps:put(ID, Task, Scheduled)}};
            forwarded_tasks -> {noreply, State#{forwarded_tasks := maps:put(ID, Task, Forwarded)}}
        end
    end;

handle_cast({update, ID, Task, List}, State) ->
    case State of #{running_tasks := Running, scheduled_tasks := Scheduled, forwarded_tasks := Forwarded} ->
        case List of
            running_tasks -> {noreply, State#{running_tasks := maps:update(ID, Task, Running)}};
            scheduled_tasks -> {noreply, State#{scheduled_tasks := maps:update(ID, Task, Scheduled)}};
            forwarded_tasks -> {noreply, State#{forwarded_tasks := maps:update(ID, Task, Forwarded)}}
        end
    end;

% @pre -
% @post Start the execution and forwarding of the task
handle_cast(daemon, State) ->
    io:format("Starting the deamon~n"),
    case State of #{running_tasks := Running, forwarded_tasks := Forwarded} ->
        % Execute tasks in running_tasks
        lists:foreach(fun({ID, _}) ->
            gen_server:cast(?MODULE, {execute, ID})
        end, maps:to_list(Running)),
        % Forward tasks in forwarded_tasks
        lists:foreach(fun({ID, Task}) ->
            case Task of #forwarded_task{hops = Hops} ->
                Node = choose_node(Hops),
                gen_server:cast(?MODULE, {forward, ID, Node})
            end
        end, maps:to_list(Forwarded)),
        {noreply, State}
    end;

% @pre -
% @post print the State
handle_cast(debug, State) ->
    io:format("State: ~p~n", [State]),
    {noreply, State};

handle_cast(_Message, State) ->
    {noreply, State}.

% Call:

handle_call(_Request, _From, State) ->
    {noreply, State}.

% Info:

handle_info(_Info, State) ->
    io:format("Info: ~p~n", [_Info]),
    {noreply, State}.


% Helpers:
% @pre -
% @post return the name of this node
get_name() ->
    Manager = partisan_peer_service:manager(),
    #{name := Name} = Manager:myself(),
    Name.

% @pre  BL is the list of nodes through which the task has passed (hops)
% @post Choose a node among the neighbors except the nodes in Blacklist
choose_node(BL) ->
    Blacklist = [get_name()|BL], % blacklist the current node
    {ok, Members} = partisan_peer_service:members(),
    Nodes = lists:filter(fun(Member) ->
        lists:member(Member, Blacklist) == false
    end, Members),
    Length = erlang:length(Nodes),
    Index = rand:uniform(Length),
    lists:nth(Index, Nodes).
    % 'achlys1@192.168.1.27'.

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
    erlang:exit(Timer, kill).

% API:
% @pre  Fun is a function
%       Callback is a function
% @post Create a task containing Fun and Callback and schedule it
schedule(Fun, Callback) ->
    achlys_spawn:schedule(Fun, [], Callback).

% @pre  Fun is a function
%       Args is a list of arguments for Fun
%       Callback is a function
% @post Create a task containing Fun, Args and Callback and schedule it
schedule(Fun, Args, Callback) ->
    gen_server:cast(?MODULE, {schedule, #task{
        function = Fun,
        arguments = Args,
        callback = Callback
    }}).

daemon() ->
     gen_server:cast(?MODULE, daemon).

debug() ->
    gen_server:cast(?MODULE, debug).
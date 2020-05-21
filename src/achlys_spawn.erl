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

% 'Task' is a new task. 
% The server will dispatch the task in one of the list of tasks:
% 'running': tasks proceed by the node (10 slots)
% 'scheduled': list of tasks waiting to be proceed (10 slots)
% 'forwarded': list of tasks that have been sent to other nodes (IFF the two other lists are full)
handle_cast({schedule, Task}, State) ->
    case State of #{running_tasks := Running, scheduled_tasks := Scheduled, forwarded_tasks := Forwarded} ->
        io:format("A task has been scheduled~n"),
        ID = erlang:unique_integer(),
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
                        {noreply, State#{
                            forwarded_tasks := maps:put(ID, Task, Forwarded)
                        }}
                end
            end;
        _ -> io:format("ERROR: the State is not valid~n")
    end;

% Execute the task 'ID' locally
% Create a new process for this task
handle_cast({execute, ID}, State) ->
    case State of #{running_tasks := Running, scheduled_tasks := _, forwarded_tasks := _} ->
        case maps:get(ID, Running) of
            #{function := Fun, arguments := Args} ->
                io:format("Executing the task~n"),
                erlang:spawn(fun() ->
                    Result = erlang:apply(Fun, Args),
                    gen_server:cast(?MODULE, {return, ID, Result})
                end)
        end,
        {noreply, State}
    end;

% Handle the result of a task 'ID' executed on this node
handle_cast({return, ID, Result}, State) ->
    case State of #{running_tasks := Running} ->
        Task = maps:get(ID, Running),
        case Task of
            #{hops := [Node|_]} -> % if the task is not a local task but a task forwarded by the node Node
                Remote_ID = maps:get(id, Task), 
                io:format("Returning the result to the original node~n"),
                % unicast the result (respond) to the node that sent the task
                partisan_peer_service:cast_message(
                    Node,
                    achlys_spawn,
                    {return_forward, Remote_ID, Result}
                );
            #{hops := [], callback := Callback} -> % local task
                io:format("Task ~p return a result ~n", [ID]),
                erlang:apply(Callback, [Result]),
                gen_server:cast(?MODULE, {update})
        end,
        {noreply, State#{running_tasks := maps:remove(ID, Running)}}
    end;

% Handle the result of a forwarded task 'ID'
handle_cast({return_forward, ID, Result}, State) ->
    case State of #{forwarded_tasks := Forwarded} ->
        Task = maps:get(ID, Forwarded),
        case Task of 
            #{hops := [Node|_]} -> % if the task is not a local task but a task forwarded by the node Node
                Remote_ID = maps:get(id, Task), 
                io:format("Returning the result to the original node~n"),
                % unicast the result (respond) to the node that sent the task
                partisan_peer_service:cast_message(
                    Node,
                    achlys_spawn,
                    {return_forward, Remote_ID, Result}
                );
            #{hops := [], callback := Callback} ->
                io:format("Forwarded task ~p return a result ~n", [ID]),
                erlang:apply(Callback, [Result])
        end,
        {noreply, State#{forwarded_tasks := maps:remove(ID, Forwarded)}}
    end;

% Update the running tasks list if necessary
handle_cast({update}, State) ->
    case State of #{running_tasks := Running, scheduled_tasks := Scheduled, forwarded_tasks := _} ->
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

% Forward the task 'ID' to a neighbor 'Node'
handle_cast({forward, ID, Node}, State) ->
    case State of #{forwarded_tasks := Forwarded} ->
        case maps:get(ID, Forwarded) of Task ->
            partisan_peer_service:cast_message(
                Node,
                achlys_spawn,
                {schedule, #{
                    id => ID,
                    function => maps:get(function, Task),
                    arguments => maps:get(arguments, Task),
                    hops => [get_name()|maps:get(hops, Task)]
                }}
            ),
            io:format("The task has been forwarded~n")
        end
    end,
    {noreply, State};

% Start the execution and forwarding of the task
handle_cast(daemon, State) ->
    io:format("Starting the deamon~n"),
    case State of #{running_tasks := Running, scheduled_tasks := _, forwarded_tasks := Forwarded} ->
        % Execute tasks in running_tasks
        lists:foreach(fun({ID, _}) ->
            gen_server:cast(?MODULE, {execute, ID})
        end, maps:to_list(Running)),
        % Forward tasks in forwarded_tasks
        lists:foreach(fun({ID, Task}) ->
            case Task of #{hops := Hops} ->
                Node = choose_node(Hops),
                gen_server:cast(?MODULE, {forward, ID, Node})
            end
        end, maps:to_list(Forwarded)),
        {noreply, State}
    end;

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

get_name() ->
    Manager = partisan_peer_service:manager(),
    #{name := Name} = Manager:myself(),
    Name.

% Choose a node among the neighbors except the nodes in Blacklist
choose_node(BL) ->
    Blacklist = [get_name()|BL],
    {ok, Members} = partisan_peer_service:members(),
    Nodes = lists:filter(fun(Member) ->
        lists:member(Member, Blacklist) == false
    end, Members),
    Length = erlang:length(Nodes),
    Index = rand:uniform(Length),
    lists:nth(Index, Nodes).

% API:

schedule(Fun, Callback) ->
    achlys_spawn:schedule(Fun, [], Callback).

schedule(Fun, Args, Callback) ->
    % send a cast request {schedule, Task}
    gen_server:cast(?MODULE, {schedule, #{
        function => Fun,
        arguments => Args,
        hops => [],
        callback => Callback
    }}).

daemon() ->
    gen_server:cast(?MODULE, daemon).

debug() ->
    gen_server:cast(?MODULE, debug).
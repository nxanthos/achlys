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
    spawn/2,
    spawn/3,
    debug/0
]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    % Add timer here
    {ok, #{
        tasks => #{}
    }}.

% Cast:

handle_cast({schedule, Task}, State) ->
    case State of #{tasks := Tasks} ->
        io:format("A task has been scheduled~n"),
        ID = erlang:unique_integer(),
        {noreply, State#{
            tasks := maps:put(ID, Task, Tasks)
        }}
    end;

handle_cast({execute, ID}, State) ->
    case State of #{tasks := Tasks} ->
        case maps:get(ID, Tasks) of
            #{function := Fun, arguments := Args} ->
                io:format("Executing the task~n"),
                erlang:spawn(fun() ->
                    Result = erlang:apply(Fun, Args),
                    io:format("Result of the task: ~p~n", [Result]),
                    gen_server:cast(?MODULE, {return, ID, Result})
                end)
        end,
        {noreply, State}
    end;

handle_cast({return, ID, Result}, State) ->
    case State of #{tasks := Tasks} ->
        Task = maps:get(ID, Tasks),
        case Task of
            #{hops := [Node|_]} ->
                Remote_ID = maps:get(id, Task), 
                io:format("Returning the result~n"),
                partisan_peer_service:cast_message(
                    Node,
                    achlys_spawn,
                    {return, Remote_ID, Result}
                );
            #{hops := [], callback := Callback} ->
                erlang:apply(Callback, [Result])
        end
    end,
    {noreply, State#{
        tasks := maps:remove(ID, Tasks)
    }};

handle_cast({forward, ID, Node}, State) ->
    case State of #{tasks := Tasks} ->
        case maps:get(ID, Tasks) of Task ->
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

handle_cast(daemon, State) ->
    io:format("Starting the deamon~n"),
    case State of #{tasks := Tasks} ->
        lists:foreach(fun({ID, Task}) ->
            case Task of #{hops := Hops} ->
                Node = choose_node(Hops),
                case Node == get_name() of
                    true ->
                        io:format("The task will be executed~n"),
                        gen_server:cast(?MODULE, {execute, ID});
                    false ->
                        io:format("The task will be forwarded~n"),
                        gen_server:cast(?MODULE, {forward, ID, Node})
                end
            end
        end, maps:to_list(Tasks)),
        {noreply, State}
    end;

handle_cast(debug, State) ->
    io:format("~p~n", [State]),
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

choose_node(Blacklist) ->
    {ok, Members} = partisan_peer_service:members(),
    Nodes = lists:filter(fun(Member) ->
        lists:member(Member, Blacklist) == false
    end, Members),
    Length = erlang:length(Nodes),
    Index = rand:uniform(Length),
    lists:nth(Index, Nodes).

% API:

spawn(Fun, Callback) ->
    achlys_spawn:spawn(Fun, [], Callback).

spawn(Fun, Args, Callback) ->
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
-module(achlys_spawn_monitor).
-behaviour(gen_server).

-export([
    init/1,
    start_link/0,
    handle_cast/2,
    handle_call/3,
    handle_info/2,
    terminate/2
]).

-export([
    on_schedule/2,
    on_return/2,
    on_delete/2,
    get_average_response_time/0,
    get_timeout_per_second/0,
    choose_node/1,
    test/0,
    debug/0
]).

% achlys_spawn_monitor:debug().
% achlys_spawn_monitor:test().
% achlys_spawn_monitor:get_average_response_time().

% response_times: nodes => [{x2, y}, {x1, y}, ...] avec x2 > x1

% TODO: Garbage collect les anciens logs 
% TODO: Envoyer des messages avec la taille de la queue.

% logs = #{ID => #{Node1 => Start}, ID2 => #{Node => Start}, ...}
% on_timeout(Node, ID).
% on_delete(Node, ID).

init([]) ->
    {ok, #{
        logs => orddict:new(),
        response_times => orddict:new()
    }}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

handle_cast({on_schedule, Node, ID}, State) ->
    case State of #{ logs := Logs } ->
        Key = {Node, ID},
        Info = orddict:from_list([
            {Key, erlang:system_time(millisecond)}
        ]),
        {noreply, State#{
            logs := orddict:merge(fun(_Key, _V1, V2) -> V2 end, Logs, Info)
        }}
    end;

handle_cast({on_return, Node, ID}, State) ->
    case State of #{ logs := Logs, response_times := ResponseTimes } ->
        Key = {Node, ID},
        case orddict:find(Key, Logs) of
            {ok, Timestamp} ->
                Now = erlang:system_time(millisecond),
                Measure = {Now, Now - Timestamp},
                {noreply, State#{
                    logs := orddict:erase(Key, Logs),
                    response_times := prepend(Node, Measure, ResponseTimes)
                }};
            error ->
                {noreply, State}
        end
    end;

handle_cast({on_delete, Node, ID}, State) ->
    case State of #{ logs := Logs } ->
        Key = {Node, ID},
        {noreply, State#{
            logs := orddict:erase(Key, Logs)
        }}
    end;

handle_cast({get_average_response_time, Node}, State) ->
    case State of #{ response_times := ResponseTimes } ->
        case orddict:find(Node, ResponseTimes) of
            {ok, Measures} ->
                Lifespan = 1000,
                Now = erlang:system_time(millisecond),
                L = remove_old_measures(Measures, Now, Lifespan),
                Average = get_EMA(L, Now, Lifespan),
                io:format("Average=~p~n", [Average]),
                io:format("Node=~p~n", [Node]),
                io:format("L=~p~n", [L]),
                {noreply, State#{
                    response_times := orddict:store(Node, L, ResponseTimes)
                }};
            error ->
                {noreply, State}
        end
    end;

handle_cast({get_timeout_per_second, Node}, State) ->
    case State of #{ timeouts := Timeouts } ->
        case orddict:find(Node, Timeouts) of
            {ok, Measures} ->
                Lifespan = 1000,
                Now = erlang:system_time(millisecond),
                L = lists:takewhile(fun(Time) ->
                    Now - Time < Lifespan
                end, Measures),
                io:format("Node=~p~n", [Node]),
                io:format("L=~p~n", [L]),
                % TODO: Calculer le temps de réponse
                {noreply, State#{
                    timeouts := orddict:store(Node, L, Timeouts)
                }};
            error ->
                {noreply, State}
        end
    end;

handle_cast(debug, State) ->
    io:format("State: ~p~n", [State]),
    {noreply, State};

handle_cast(_Message, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    ok.

% Helpers:

% @pre -
% @post -
prepend(Key, Value, Orddict) ->
    case orddict:find(Key, Orddict) of
        {ok, List} ->
            orddict:store(Key, [Value|List], Orddict);
        error ->
            orddict:store(Key, [Value|[]], Orddict)
    end.

% @pre -
% @post -
exp(X, Lambda) ->
    case X < 0 of
        true -> 0;
        false -> Lambda * math:exp(- Lambda * X)
    end.

% @pre -
% @post -
get_lambda(X, Percentage) ->
    Num = math:log(1 - Percentage),
    Denum = math:log(math:exp(1)),
    - (Num / Denum) / X.

% @pre -
% @post -
get_EMA(Measures, Now, Lifespan) ->
    Terms = lists:map(fun({X, Y}) ->
        Lambda = get_lambda(Lifespan, 0.5),
        Weight = exp(Now - X, Lambda),
        {Weight, Y}
    end, Measures),
    Total = lists:foldl(fun({Weight, _Y}, Sum) ->
        Sum + Weight
    end, 0, Terms),
    lists:foldl(fun({Weight, Y}, Average) ->
        Average + (Weight / Total) * Y
    end, 0, Terms).

% @pre -
% @post -
remove_old_measures(Measures, Now, Lifespan) ->
    lists:takewhile(fun({X, _Y}) ->
        Now - X < Lifespan
    end, Measures).

% API:

% @pre -
% @post -
on_schedule(Node, ID) ->
    gen_server:cast(?MODULE, {on_schedule, Node, ID}).

% @pre -
% @post -
on_return(Node, ID) ->
    gen_server:cast(?MODULE, {on_return, Node, ID}).

% @pre -
% @post -
on_delete(Node, ID) ->
    gen_server:cast(?MODULE, {on_delete, Node, ID}).

% @pre -
% @post -
get_average_response_time() ->
    lists:foreach(fun(Node) ->
        gen_server:cast(?MODULE, {get_average_response_time, Node})
    end, achlys_util:get_neighbors()).

get_timeout_per_second() ->
    lists:foreach(fun(Node) ->
        gen_server:cast(?MODULE, {get_timeout_per_second}, Node)
    end, achlys_util:get_neighbors()).

% @pre -
% @post return the name of this node
get_name() ->
    Manager = partisan_peer_service:manager(),
    #{name := Name} = Manager:myself(),
    Name.

% @pre  BL is the list of nodes through which the task has passed (hops)
% @post Choose a node among the neighbors except the nodes in Blacklist
choose_node(BL) ->
    Myself = get_name(),
    Blacklist = [Myself|BL], % blacklist the current node
    {ok, Members} = partisan_peer_service:members(),
    Nodes = lists:filter(fun(Member) ->
        lists:member(Member, Blacklist) == false
    end, Members),
    Length = erlang:length(Nodes),
    case Length of
        0 ->
            Myself;
        _ ->
            Index = rand:uniform(Length),
            lists:nth(Index, Nodes)
    end.

% @pre -
% @post -
debug() ->
    gen_server:cast(?MODULE, debug).

% @pre -
% @post -
test() ->
    spawn_test:test_async(100, 100),
    timer:sleep(500),
    get_average_response_time().
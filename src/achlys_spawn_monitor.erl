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
    get_average_response_times/1,
    choose_node/1,
    debug/0
]).

% spawn_test:test_async(1000, 100).
% achlys_spawn_monitor:debug().
% achlys_spawn_monitor:get_average_response_times(achlys_util:get_neighbors()).

% @pre -
% @post -
init([]) ->
    {ok, #{
        logs => orddict:new(),
        response_times => orddict:new()
    }}.

% @pre -
% @post -
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% @pre -
% @post -
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

% @pre -
% @post -
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

% @pre -
% @post -
handle_cast({on_delete, Node, ID}, State) ->
    case State of #{ logs := Logs } ->
        Key = {Node, ID},
        {noreply, State#{
            logs := orddict:erase(Key, Logs)
        }}
    end;

% @pre -
% @post -
handle_cast(debug, State) ->
    io:format("State: ~p~n", [State]),
    {noreply, State};

% @pre -
% @post -
handle_cast(_Message, State) ->
    {noreply, State}.

% @pre -
% @post -
handle_call({get_average_response_time, Node}, _From, State) ->
    case State of #{ response_times := ResponseTimes } ->
        case orddict:find(Node, ResponseTimes) of
            {ok, Measures} ->
                Lifespan = 1000,
                Now = erlang:system_time(millisecond),
                L = remove_old_measures(Measures, Now, Lifespan),
                AverageResponseTime = get_EMA(L, Now, Lifespan),
                % io:format("Average=~p~n", [AverageResponseTime]),
                % io:format("Node=~p~n", [Node]),
                % io:format("L=~p~n", [L]),
                {reply, AverageResponseTime, State#{
                    response_times := orddict:store(Node, L, ResponseTimes)
                }};
            error ->
                {reply, 0, State}
        end
    end;

% @pre -
% @post -
handle_call(_Request, _From, State) ->
    {noreply, State}.

% @pre -
% @post -
handle_info(_Info, State) ->
    {noreply, State}.

% @pre -
% @post -
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
get_average_response_time(Node) ->
    gen_server:call(?MODULE, {get_average_response_time, Node}).

% @pre -
% @post -
get_average_response_times(Nodes) ->
    orddict:from_list(
        lists:map(fun(Node) ->
            {Node, get_average_response_time(Node)}
        end, Nodes)
    ).

% @pre  Hops is the list of nodes through which the task has passed (hops)
% @post Choose a node among the neighbors except the nodes in Blacklist
choose_node(Hops) ->

    Myself = achlys_util:myself(),
    Blacklist = [Myself|Hops],
    {ok, Members} = partisan_peer_service:members(),
    
    Nodes = lists:filter(fun(Member) ->
        not lists:member(Member, Blacklist)
    end, Members),

    case Nodes of
        [] ->
            Orddict = get_average_response_times(Nodes),
            io:format("ARS = ~p~n", [Orddict]),
            io:format("Chosen node = ~p~n", [Myself]),
            io:format("~n"), 
            Myself;
        _ ->
            Orddict = get_average_response_times(Nodes),
            {Node, _} = orddict:fold(fun(K1, V1, Min) ->
                case Min of
                    undefined -> {K1, V1};
                    {K2, V2} when V1 < V2 -> {K1, V1};
                    _ -> Min
                end
            end, undefined, Orddict),
            io:format("ARS = ~p~n", [Orddict]),
            io:format("Chosen node = ~p~n", [Node]),
            io:format("~n"),
            Node
    end.

% @pre -
% @post -
debug() ->
    gen_server:cast(?MODULE, debug).

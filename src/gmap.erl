-module(gmap).

-behaviour(gen_server).

-export([start_link/0]).
-export([schedule_task/0]).
-export([debug/0]).
-export([
    init/1 ,
    handle_call/3 ,
    handle_cast/2 ,
    handle_info/2 ,
    terminate/2 ,
    code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {}).

start_link() ->
    gen_server:start_link({local , ?SERVER} , ?MODULE , [] , []).

init([]) ->
    {ok , #state{}}.

handle_call(_Request, _From , State) ->
    {reply , ok , State}.

handle_cast(_Request, State) ->
    {noreply , State}.

handle_info(_Info, State) ->
    case _Info of
        {task, Task} -> 
            io:format("Starting task ~n", []),
            achlys:bite(Task)
    end,
    {noreply , State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok , State}.

% Helpers :

timestamp() ->
    erlang:unique_integer([monotonic, positive]).

generatePoint(M, P) ->
    X = rand:uniform() * 10 - 5, % [-5, 5]
    Noise = rand:uniform() * 4 - 2, % [-2, 2]
    { X, (M * X + P) + Noise}.

choice(List) ->
    Length = erlang:length(List),
    Index = rand:uniform(Length),
    lists:nth(Index, List).

derivative(Point, Intercept, Slope) ->
    derivative(Point, Intercept, Slope, 0.01).

derivative(Point, Intercept, Slope, LearningRate) ->
    {X,Y} = Point,
    % Derivative with respect of Intercept
    DIntercept = -2 * (Y - (Intercept + Slope * X)),
    StepSizeI = DIntercept * LearningRate,
    NewIntercept = Intercept - StepSizeI,

    % Derivative with respect of Slope
    DSlope = -2 * X * (Y - (Intercept + Slope * X)),
    StepSizeS = DSlope * LearningRate,
    NewSlope = Slope - StepSizeS,

    timer:sleep(200),
    % derivative(NewPoint, NewIntercept, NewSlope, LearningRate, N-1)
    {NewIntercept, NewSlope}.

addInterceptInMap(Intercept) ->
    KeyI = <<"intercept">>,
    ValI = Intercept,
    lasp:update({<<"gmap">>, {state_gmap, [state_lwwregister]}}, {
        apply, KeyI, {set, timestamp(), ValI}
    }, self()).

addSlopeInMap(Slope) ->
    KeyS = <<"slope">>,
    ValS = Slope,
    lasp:update({<<"gmap">>, {state_gmap, [state_lwwregister]}}, {
        apply, KeyS, {set, timestamp(), ValS}
    }, self()).

% Producer & Consumer :

producer(N) ->
    case N of
        _ when N =< 0 -> ok;
        _ when N > 0 ->
            Set = {<<"gset">>, state_gset},
            case generatePoint(1, 5) of Point ->
                % io:format("Point: ~p~n", [Point]),
                lasp:update(Set, {add, Point}, self())
            end,
            % timer:sleep(1000),
            producer(N - 1)
    end.

consumer() ->
    consumer(0, 1, 100).

consumer(Intercept, Slope, N) ->
    case N of
        _ when N =< 0 -> ok;
        _ when N > 0 ->
            io:format("{Intercept, Slope}= ~p~n", [{Intercept, Slope}]),
            Set = achlys_util:query({<<"gset">>, state_gset}),
            Sample = choice(Set), % Random point
            {NewIntercept, NewSlope} = derivative(Sample, Intercept, Slope),
            % add intercept in Map
            addInterceptInMap(NewIntercept),
            % add slope in Map
            addSlopeInMap(NewSlope),

            {_, Map} = lasp:query({<<"gmap">>, {state_gmap, [state_lwwregister]}}),
            GetIntercept = lists:keysearch(<<"intercept">>, 1, Map),
            {_,{_,I}} = GetIntercept,
            GetSlope = lists:keysearch(<<"slope">>, 1, Map), 
            {_,{_,S}} = GetSlope,
            io:format("{Intercept, Slope} = ~p~n", [{I, S}]),
            consumer(I, S, N-1)
    end.

schedule_task() ->
    Task = achlys:declare(mytask, all, single, fun() ->

        GMapType = {state_gmap, [state_lwwregister]},        
        GMap = {<<"gmap">>, GMapType},
        lasp:declare(GMap, GMapType),

        GSetType = state_gset,
        GSet = {<<"gset">>, GSetType},
        lasp:declare(GSet, GSetType),
        
        % Producer :

        spawn(fun() ->
           producer(1000)
        end),

        % Consumer :

        spawn(fun() ->
            consumer()
        end)
    end),

    erlang:send_after(100, ?SERVER, {task, Task}),
    ok.

debug() ->
    % Set = achlys_util:query({<<"gset">>, state_gset}),
    Map = lasp:query({<<"gmap">>, {state_gmap, [state_lwwregister]}}),
    % io:format("Set: ~p~n", [Set]),
    io:format("Map: ~p~n", [Map]).
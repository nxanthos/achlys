-module(compute_pi).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1 ,
         handle_call/3 ,
         handle_cast/2 ,
         handle_info/2 ,
         terminate/2 ,
         code_change/3]).

-define(SERVER , ?MODULE).

-record(state , {}).

start_link() ->
    gen_server:start_link({local , ?SERVER} , ?MODULE , [] , []).

init([]) ->
        ok = schedule_task(),
        {ok , #state{}}.

handle_call(_Request , _From , State) ->
    {reply , ok , State}.

handle_cast(_Request , State) ->
    {noreply , State}.

handle_info(_Info , State) ->
        achlys:bite(Task),
        {noreply , State};

handle_info(_Info , State) ->
        {noreply , State}.

terminate(_Reason , _State) ->
    ok.

code_change(_OldVsn , State , _Extra) ->
    {ok , State}.

compute() ->
{ok, {Inner, _, _, _}} = lasp:declare({<<"inner_counter">>, state_gcounter}, state_gcounter),
{ok, {Outer, _, _, _}} = lasp:declare({<<"outer_counter">>, state_gcounter}, state_gcounter),

lasp:stream({<<"inner_counter">>, state_gcounter}, fun(Inner) ->
  {Outer, _, _} = lasp:query({<<"outer_counter">>, state_gcounter}),
  io:format("~p\n", [4 * (Inner / (Inner + Outer))])
end),

lasp:stream({<<"outer_counter">>, state_gcounter}, fun(Outer) -> 
  {Inner, _, _} = lasp:query({<<"inner_counter">>, state_gcounter}),
  io:format("~p\n", [4 * (Inner / (Inner + Outer))])
end),

getDistanceFromOrigin(Point)->
  case Point of {X, Y} -> math:sqrt(X*X + Y*Y) 
end,


getRandomPoint() -> {random:uniform(), random:uniform()}


F = fun() -> 
  lists:foreach(fun(E) -> 
    Point = getRandomPoint(),
    Distance = getDistanceFromOrigin(Point),
    if
      (Distance > 1) -> 
        %% Here, we can increment the outer counter
        lasp:update(Outer, increment, self()),
      (Distance < 1) ->
        %% Here, we can increment the inner counter
        lasp:update(Inner, increment, self()),
    end  
  end,
  lists:seq(1, 5)),
end,

schedule_task() -> 
        Task = achlys:declare(mytask
        , all
        , single
        , F),
        erlang:send_after(5000, ?SERVER, {task, Task}),
        ok.
-module(compute_pi).

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([schedule_task/0]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  % schedule_task(),
  {ok, #state{}}.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info({task, Task}, State) ->
  %% Task propagation to the cluster, including self
  io:format("Starting task", []),
  achlys:bite(Task),
  {noreply, State};

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

schedule_task() ->
  F = fun() ->
    Set = {<<"set">>, state_gset},

      % lasp:declare(V1, state_gcounter),
      % lasp:declare(V2, state_gcounter),
      lasp:declare(Set, state_gset),

      Samples = 100,
      % Len = erlang:length([ io:format("value over 1.0 ~p ~n",[Y]) 
      Len = erlang:length([ Y 
          || Y <- [ math:sqrt(math:pow(rand:uniform(),2) + math:pow(rand:uniform(),2))
            || X <- lists:seq(1,Samples) ], Y > 1.0 ]),
      
      Node = erlang:node(),
      InOut = {Node, (Samples - Len), Len},
      Self = self(),
      % io:format("Inner ~p Outer ~p ~n",[(Samples - Len),Len]),
      % lasp:update(Set, {add, {node(), (Samples - Len), Len}, self()})
      spawn(fun() -> 
        Function = fun(V) -> io:format("stream ~p ~n", [V]) end
        , lasp:stream(Set, Function)
      end),
      {ok, {Id, _, _, _}} = lasp:update(Set , {add , InOut} , self())
  end,
  % F = fun() ->
      % V1 = {<<"inner_counter">>, state_gcounter},
      % V2 = {<<"outer_counter">>, state_gcounter},
      
    % end,
    Task = achlys:declare(mytask, all, single, F),
    %% Send the task to the current server module
    %% after a 3000ms delay
    erlang:send_after(3000, ?SERVER, {task, Task}),
    ok.
    % lasp:stream(V1, fun(Inner) ->
    %   {ok, Outer} = lasp:query(V2),
    %   io:format("~p\n", [4 * (Inner / (Inner + Outer))])
    % end),

    % lasp:stream(V2, fun(Outer) ->
    %     {ok, Inner} = lasp:query(V1),
    %     io:format("~p\n", [4 * (Inner / (Inner + Outer))])
    % end),
    % lists:foreach(fun(_) -> 
    %   Point = getRandomPoint(),
    %   Distance = getDistanceFromOrigin(Point),
    %   if
    %     (Distance > 1) ->
    %       %% Here, we can increment the outer counter
    %       lasp:update(V2, increment, self());
    %     (Distance =< 1) ->
    %       %% Here, we can increment the inner counter
    %       lasp:update(V1, increment, self())
    %   end
    % end, lists:seq(1, 100))


getDistanceFromOrigin(Point) ->
  case Point of {X, Y} -> math:sqrt(X * X + Y * Y) end.

getRandomPoint() -> {
  rand:uniform(),
  rand:uniform()
}.

-module(achlys_pi).

-compile(export_all).

compute() ->
  {ok, {Inner, _, _, _}} = lasp:declare({<<"inner_counter">>, state_gcounter}, state_gcounter),
  {ok, {Outer, _, _, _}} = lasp:declare({<<"outer_counter">>, state_gcounter}, state_gcounter),

  lasp:stream({<<"inner_counter">>, state_gcounter},
    fun(Inner) ->
      {Outer, _, _} = lasp:query({<<"outer_counter">>, state_gcounter}),
      io:format("~p\n", [4 * (Inner / (Inner + Outer))])
    end),

  lasp:stream({<<"outer_counter">>, state_gcounter},
    fun(Outer) ->
      {Inner, _, _} = lasp:query({<<"inner_counter">>, state_gcounter}),
      io:format("~p\n", [4 * (Inner / (Inner + Outer))])
    end).

getDistanceFromOrigin(Point) ->
  case Point of {X, Y} -> math:sqrt(X*X + Y*Y)
  end.

getRandomPoint() -> {rand:uniform(), rand:uniform()}.

increment() ->
  Point = getRandomPoint(),
  Distance = getDistanceFromOrigin(Point),
  if
    (Distance > 1) ->
      %% Here, we can increment the outer counter
      lasp:update({<<"outer_counter">>, state_gcounter}, increment, self());
    (Distance < 1) ->
      %% Here, we can increment the inner counter
      lasp:update({<<"inner_counter">>, state_gcounter}, increment, self())
  end.
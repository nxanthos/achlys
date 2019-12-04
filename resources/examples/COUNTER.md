# Counter

## Approximation of PI

In this example, we will try to estimate the value of π by using the Monte-Carlo approach. To do that, we will have to generate many points in a two dimensional space and maintaining two counters:

1. "Inner counter": This counter will be use to count the number of points for which the distance between the point and the origin of the axis is smaller than 1.
2. "Outer counter": This counter will be use to count the number of points for which the distance between the point and the origin of the axis is greater than 1.

To have a correct estimation of π, the function that generates the coordinates of the points will have to follow a uniform distribution and the coordinates x and y should be between 0 and 1.

Let X = "The x coordinate of the point"

Let Y = "The y coordinate of the point"

- X ~ Un(0, 1)
- Y ~ Un(0, 1)

Let

- S = "The surface of the circle"
- r = "The radius of the circle"

We know that the surface of a circle is given by the following equation:

S = πr²

If r is equal to 1, then:

S = π

π = 4 . (S / 4)

With the Monte-Carlo approach, the ratio between the inner counter and total number of observation will give us the approximation of the surface of a quadrant (i.e: S / 4).

With Achlys, we will first have to create two counters:

```erlang
{ok, {Inner, _, _, _}} = lasp:declare({<<"inner_counter">>, state_gcounter}, state_gcounter).
{ok, {Outer, _, _, _}} = lasp:declare({<<"outer_counter">>, state_gcounter}, state_gcounter). 
```

Then, we can define some helper functions :

```erlang
%% Return the euclidian distance between the point given as parameter and the origin (0, 0)
getDistanceFromOrigin(Point)->
  case Point of {X, Y} -> math:sqrt(X*X + Y*Y) end.

%% This function returns a random point in a two dimensional space. The coordinates of the point should be between 0 and 1
getRandomPoint() -> {random:uniform(), random:uniform()}
```

Then, we will have to create a task that will be executed by each node several time :

```erlang
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
  end, lists:seq(1, 5))
end
```

A node can listen the changes of a variable by using `lasp:stream`. This function takes another function as argument. This function will be called whenever the targeting CRDT variable is updated.

```erlang
%% Here, we add listeners to print the current approximation of pi when one of the counter is updated.

lasp:stream({<<"inner_counter">>, state_gcounter}, fun(Inner) ->
  { Outer, _, ... } = lasp:query({<<"outer_counter">>, state_gcounter}),
  io:format("~p\n", [4 * (Inner / (Inner + Outer))])
end)

lasp:stream({<<"outer_counter">>, state_gcounter}, fun(Outer) -> 
  { Inner, _, ...} = lasp:query({<<"inner_counter">>, state_gcounter}),
  io:format("~p\n", [4 * (Inner / (Inner + Outer))])
end)
```

We should have a sufficient number of points in order to reduce the approximation error.

Putting everything together we have the following code in the :

```erlang

```
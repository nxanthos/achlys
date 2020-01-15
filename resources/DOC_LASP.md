
# CRDT Variables

Lasp API :

- `lasp:declare/2`: Define a new variable to the CRDT
- `lasp:query/1`: Return the instantaneous value of the variable. The result is not guaranteed to be deterministic or monotonic.
- `lasp:update/3`: Update a variable.

## Set

### Grow-only-set

Mutations:

```
{add, Value}
```

see [source](https://github.com/lasp-lang/types/blob/master/src/state_gset.erl)

```erlang
Type = state_gset,
ID = {<<"set">>, Type},
lasp:declare(ID, Type),

{ok, A} = lasp:query(ID),

lasp:update(ID, {add, #{
    n => 0
}}, self()),

{ok, B} = lasp:query(ID),

lasp:update(ID, {add, #{
    n => 2
}}, self()),

{ok, C} = lasp:query(ID),

io:format("A=~p size=~p~n", [sets:to_list(A), sets:size(A)]),
io:format("B=~p size=~p~n", [sets:to_list(B), sets:size(B)]),
io:format("C=~p size=~p~n", [sets:to_list(C), sets:size(C)]),

% Delta:
io:format("A - B :~p~n", [sets:to_list(sets:subtract(A, B))]),
io:format("B - A :~p~n", [sets:to_list(sets:subtract(B, A))]),
io:format("A - C :~p~n", [sets:to_list(sets:subtract(A, C))]),
io:format("C - A :~p~n", [sets:to_list(sets:subtract(C, A))]),
io:format("A - D :~p~n", [sets:to_list(sets:subtract(A, D))]),
```

### Add-win-set

Mutations:

```
{add, Value}
{add_all, [Value_1, Value_2, ...]}
{rmv, Value}
{rmv_all, [Value_1, Value_2, ...]}
{filter, fun(E) -> ... end}
```

see [source](https://github.com/lasp-lang/types/blob/master/src/state_awset.erl)

```erlang
Type = state_awset,
ID = {<<"set">>, Type},
lasp:declare(ID, Type),

{ok, A} = lasp:query(ID),

lasp:update(ID, {add, #{
    n => 0
}}, self()),

{ok, B} = lasp:query(ID),

lasp:update(ID, {add, #{
    n => 2
}}, self()),

{ok, C} = lasp:query(ID),

lasp:update(ID, {rmv, #{
    n => 0
}}, self()),

{ok, D} = lasp:query(ID),

io:format("A=~p size=~p~n", [sets:to_list(A), sets:size(A)]),
io:format("B=~p size=~p~n", [sets:to_list(B), sets:size(B)]),
io:format("C=~p size=~p~n", [sets:to_list(C), sets:size(C)]),
io:format("D=~p size=~p~n", [sets:to_list(D), sets:size(D)]),

% Delta:
io:format("A - B :~p~n", [sets:to_list(sets:subtract(A, B))]),
io:format("B - A :~p~n", [sets:to_list(sets:subtract(B, A))]),
io:format("A - C :~p~n", [sets:to_list(sets:subtract(A, C))]),
io:format("C - A :~p~n", [sets:to_list(sets:subtract(C, A))]),
io:format("A - D :~p~n", [sets:to_list(sets:subtract(A, D))]),
io:format("D - A :~p~n", [sets:to_list(sets:subtract(D, A))]),
```

### Two-phased set

Mutations:

```
{add, Value}
{rmv, Value}
```

see [source](https://github.com/lasp-lang/types/blob/master/src/state_twopset.erl)

```erlang
Type = state_twopset,
ID = {<<"set">>, Type},
lasp:declare(ID, Type),
{ok, A} = lasp:query(ID),

lasp:update(ID, {add, 1}, self()),
{ok, B} = lasp:query(ID),

lasp:update(ID, {add, 2}, self()),
{ok, C} = lasp:query(ID),

lasp:update(ID, {rmv, 1}, self()),
{ok, D} = lasp:query(ID),

lasp:update(ID, {add, 1}, self()),
{ok, E} = lasp:query(ID),

io:format("A=~p size=~p~n", [sets:to_list(A), sets:size(A)]),
io:format("B=~p size=~p~n", [sets:to_list(B), sets:size(B)]),
io:format("C=~p size=~p~n", [sets:to_list(C), sets:size(C)]),
io:format("D=~p size=~p~n", [sets:to_list(D), sets:size(D)]),
io:format("E=~p size=~p~n", [sets:to_list(E), sets:size(E)]),
```

## Observed-remove set

Mutations:

```
{add, Value}
{add_by_token, Key, Value}
{add_all, [Value_1, ..., Value_2]}
{rmv, Value}
{rmv_all, [Value_1, ..., Value_2]}
```

see [source](https://github.com/lasp-lang/types/blob/master/src/state_orset.erl)

```erlang
Type = state_orset,
ID = {<<"set">>, Type},
lasp:declare(ID, Type),
{ok, A} = lasp:query(ID),

lasp:update(ID, {add, 1}, self()),
{ok, B} = lasp:query(ID),

lasp:update(ID, {add_by_token, a, 5}, self()),
{ok, C} = lasp:query(ID),

lasp:update(ID, {add_by_token, b, 5}, self()),
{ok, D} = lasp:query(ID),

lasp:update(ID, {rmv, 6}, self()),
{ok, E} = lasp:query(ID),

io:format("A=~p~n", [sets:to_list(A)]),
io:format("B=~p~n", [sets:to_list(B)]),
io:format("C=~p~n", [sets:to_list(C)]),
io:format("D=~p~n", [sets:to_list(D)]),
io:format("E=~p~n", [sets:to_list(E)]),
```

## Map

## Add-win-map

Mutations:

```
{apply, Key, Value}
{rmv, Key}
```

see [source](https://github.com/lasp-lang/types/blob/master/src/state_awmap.erl)

```erlang
Type = {state_awmap, [state_mvregister]},
ID = {<<"set">>, Type},
lasp:declare(ID, Type),

{ok, A} = lasp:query(ID),

lasp:update(ID, {apply,
    2, {set, nil, 1}
}, self()),

{ok, B} = lasp:query(ID),

lasp:update(ID, {apply,
    2, {set, nil, 3}
}, self()),

{ok, C} = lasp:query(ID),

lasp:update(ID, {rmv, 2}, self()),

{ok, D} = lasp:query(ID),

Format = fun(L) ->
    lists:map(fun(E) -> 
        case E of {K, V} ->
            {K, sets:to_list(V)}
        end
    end, L)
end,

io:format("A=~p~n", [Format(A)]),
io:format("B=~p~n", [Format(B)]),
io:format("C=~p~n", [Format(C)]),
io:format("D=~p~n", [Format(D)]),
```

`declare/2`

`lasp:declare({<<"set_name">>, state_orset}, state_orset).`

In this example, `declare` will allows you to define a new variable to the CRDT.
`<<"set_name">>` correspond the name of the variable

`state_orset` corresponds to the type of the variable that we want to declare. Lasp support many different variables.

Abstract data type: set, map, counter, register, flag, variable (single-assignment variable).
Strategies: grow-only, positive-negative, or, ...

CRDT types: G-Counter (Grow-only Counter), G-Set (Grow-only set), PN-Counter (Positive-Negative Counter), OR-Set (Observed-Remove Set)...

Here are some examples CRDT types in laps:
`state_orset, state_gset, ...` ‹

`lasp:query/1`

`lasp:query({<<"set">>, state_orset}).`

`query` allows to read variables at a particular point in time. This command will return the instantaneous value of the variable, and is not guaranteed to be deterministic or monotonic.

```
Function = fun(V) -> V end, 
lasp:stream({<<"set">>, state_orset}, Function).
```

`stream` allow to execute a function every time a variable changes monotonically. However, the function will does not guarantee that the value provided will be monotonic.

`lasp:update/3`

`lasp:update({Variable identifier}, {Mutations}, Actor identifier).`
`lasp:update({<<"set">>, state_orset}, {add, 1}, self()).`

- Variable identifier should be the identifier returned from a declare operation.
- Mutations are possible mutations for the datatype: this is data type specific. For example, on the `state_orset`, `add` and `rmv` are possible mutations.
- Actor identifier should identify this actor uniquely in the system: this is used to detect concurrent operations from different actors in the system, and it's assumed that each actor acts sequentially.

`lasp:product/3.`

`lasp:product(LeftId, RightId, ProductId).`

`product` allow to compute the Cartesian product of two collections (`LeftId` and `RightId`) and place the output in `ProductId`
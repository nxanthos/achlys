-module(path).
-export([
    schedule/0,
    debug/0    
]).


get_weight(I, J) ->
    NodeI = get_id(I), % id of node i
    {_, Weight} = maps:find(NodeI, J), % get the weight of the edge I-J 
    Weight.

get_value(I) ->
    {_, Value} = maps:find(value, I),
    Value.

get_id(I) ->
    {_, ID} = maps:find(id, I),
    ID.

% I1 - Input variable
% I2 - Input variable
add_edge(I1, I2, O) ->
    lasp_process:start_dag_link([
        [
            {I1, fun(ID, Threshold) ->
                lasp:read(ID, Threshold)
            end},
            {I2, fun(ID, Threshold) ->
                lasp:read(ID, Threshold)
            end}
        ],
        fun(R1, R2) ->
            {_, _, _, {_, {_, V1}}} = R1,
            {_, _, _, {_, {_, V2}}} = R2,
            Score = get_value(V1) + get_weight(V1, V2),
            case get_value(V2) > Score of
                true -> 
                    Updated_value = maps:update(value, Score, V2),
                    Updated_successor = maps:update(successor, get_id(V1), Updated_value),
                    Updated_successor;
                false -> V2
            end
        end,
        {O, fun(ID, Value) ->
            lasp:update(ID, {set, get_timestamp(), Value}, self())
        end}
    ]).
    
get_timestamp() -> 
    erlang:unique_integer([monotonic, positive]).

schedule() ->
    Type = state_lwwregister,
    {ok, {A, _, _, _}} = lasp:declare({<<"a">>, Type}, Type),
    {ok, {B, _, _, _}} = lasp:declare({<<"b">>, Type}, Type),
    {ok, {C, _, _, _}} = lasp:declare({<<"c">>, Type}, Type),
    {ok, {D, _, _, _}} = lasp:declare({<<"d">>, Type}, Type),
    {ok, {E, _, _, _}} = lasp:declare({<<"e">>, Type}, Type),

    % intermediate nodes
    {ok, {B2, _, _, _}} = lasp:declare({<<"b2">>, Type}, Type),
    {ok, {C2, _, _, _}} = lasp:declare({<<"c2">>, Type}, Type),
    {ok, {D2, _, _, _}} = lasp:declare({<<"d2">>, Type}, Type),
    {ok, {D3, _, _, _}} = lasp:declare({<<"d3">>, Type}, Type),
    {ok, {D4, _, _, _}} = lasp:declare({<<"d4">>, Type}, Type),
    {ok, {E2, _, _, _}} = lasp:declare({<<"e2">>, Type}, Type),
    {ok, {E3, _, _, _}} = lasp:declare({<<"e3">>, Type}, Type),

    lasp:update(A, {set, get_timestamp(), #{id=>a, a=>0, value=>0, successor=>undefined}}, self()), % destination node
    lasp:update(B, {set, get_timestamp(), #{id=>b, a=>3, c=>4, d=>2, value=>infinity, successor=>undefined}}, self()),
    lasp:update(C, {set, get_timestamp(), #{id=>c, b=>4, d=>5, e=>6, value=>infinity, successor=>undefined}}, self()),
    lasp:update(D, {set, get_timestamp(), #{id=>d, a=>7, b=>2, c=>5, e=>4, value=>infinity, successor=>undefined}}, self()),
    lasp:update(E, {set, get_timestamp(), #{id=>e, c=>6, d=>4, value=>infinity, successor=>undefined}}, self()),

    Task = achlys:declare(mytask, all, single, fun() ->
        add_edge(A, B, B2),
        add_edge(A, D, D2),
        add_edge(B2, C, C2),
        add_edge(B2, D2, D3),
        add_edge(C2, D3, D4),
        add_edge(C2, E, E2),
        add_edge(D4, E2, E3)
    end),
    achlys:bite(Task),
    ok.

debug() ->
    Type = state_lwwregister,
    {ok, A} = lasp:query({<<"a">>, Type}),
    {ok, B} = lasp:query({<<"b2">>, Type}),
    {ok, C} = lasp:query({<<"c2">>, Type}),
    {ok, D} = lasp:query({<<"d4">>, Type}),
    {ok, E} = lasp:query({<<"e3">>, Type}),
    io:format("A= ~w B= ~w C= ~w D= ~w E= ~w~n", [A, B, C, D, E]).
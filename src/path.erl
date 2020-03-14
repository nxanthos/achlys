-module(path).
-export([
    schedule/0,
    debug/0    
]).

% I - Node
% J - Node
get_weight(I, J) ->
    ID1 = get_id(I),
    Links = maps:get(links, J),
    Predicate = fun({ID2, _}) -> ID1 == ID2 end,
    case lists:search(Predicate, Links) of
        false -> infinite;
        {value, {_, Weight}} -> Weight
    end.

get_value(I) ->
    {_, Value} = maps:find(value, I),
    Value.

get_id(I) ->
    {_, ID} = maps:find(id, I),
    ID.

% I1 - Input variable
% I2 - Input variable
add_edge(I1, I2) ->
    lasp_process:start_dag_link([
        [{I1, fun(ID, Threshold) ->
            lasp:read(ID, Threshold)
        end}],
        fun(R1) ->
            {_, _, _, {_, {_, V1}}} = R1,
            {_, V2} = lasp:query(I2),
            Terms = [get_value(V1), get_weight(V1, V2)],
            case lists:any(fun(X) -> X =:= infinite end, Terms) of
                true -> infinite;
                false -> lists:foldl(fun(A, B) -> A + B end, 0, Terms)
            end
        end,
        {I2, fun(ID, Score) ->
            case Score of
                infinite -> ok;
                _ ->
                    {_,V1} = lasp:query(I1),
                    {_,V2} = lasp:query(I2),
                    case get_value(V2) > Score of
                        true -> 
                            Updated_value = maps:update(value, Score, V2),
                            Updated_successor = maps:update(successor, get_id(V1), Updated_value),
                            lasp:update(ID, {set, get_timestamp(), Updated_successor}, self());
                        false -> ok
                    end
            end
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

    lasp:update(A, {set, get_timestamp(), #{id=>a, links=>[{a,0}], value=>0, successor=>undefined}}, self()), % destination node
    lasp:update(B, {set, get_timestamp(), #{id=>b, links=>[{a,3}, {c,4}, {d,2}], value=>infinite, successor=>undefined}}, self()),
    lasp:update(C, {set, get_timestamp(), #{id=>c, links=>[{b,4}, {d,5}, {e,6}], value=>infinite, successor=>undefined}}, self()),
    lasp:update(D, {set, get_timestamp(), #{id=>d, links=>[{a,7}, {b,2}, {c,5}, {e,4}], value=>infinite, successor=>undefined}}, self()),
    lasp:update(E, {set, get_timestamp(), #{id=>e, links=>[{c,6}, {d,4}], value=>infinite, successor=>undefined}}, self()),

    Task = achlys:declare(mytask, all, single, fun() ->
        add_edge(A, B),
        add_edge(A, D),
        add_edge(B, C),
        add_edge(B, D),
        add_edge(C, D),
        add_edge(C, E),
        add_edge(D, E),

        timer:sleep(5000),
        lasp:update({<<"b">>,state_lwwregister}, {set, erlang:unique_integer([monotonic, positive]), #{id=>b, links=>[{a,3}, {c,4}, {d,5}], value=>infinite, successor=>undefined}}, self()),
        lasp:update({<<"d">>,state_lwwregister}, {set, erlang:unique_integer([monotonic, positive]), #{id=>d, links=>[{a,7}, {b,5}, {c,5}, {e,4}], value=>infinite, successor=>undefined}}, self())
    end),
    achlys:bite(Task),

    % erlang:spawn(fun() ->
        
    % end),

    ok.

% update_weights() ->
%     lasp:update(B, {set, get_timestamp(), #{id=>b, links=>[{a,3}, {c,4}, {d,5}], value=>infinite, successor=>undefined}}, self()),
%     lasp:update(D, {set, get_timestamp(), #{id=>b, links=>[{a,7}, {b,5}, {c,5}, {e,4}], value=>infinite, successor=>undefined}}, self()),
%     add_edge(B,D,),
%     add_edge(D,B,)

debug() ->
    Type = state_lwwregister,
    {ok, A} = lasp:query({<<"a">>, Type}),
    {ok, B} = lasp:query({<<"b">>, Type}),
    {ok, C} = lasp:query({<<"c">>, Type}),
    {ok, D} = lasp:query({<<"d">>, Type}),
    {ok, E} = lasp:query({<<"e">>, Type}),
    io:format("A= ~w B= ~w C= ~w D= ~w E= ~w~n", [A, B, C, D, E]).
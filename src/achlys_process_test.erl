-module(achlys_process_test).
-export([
    schedule/0,
    add_values/2,
    debug/0
]).

% @pre -
% @post -
union({state_gset, _}=GSet1, {state_gset, _}=GSet2) ->
    state_gset:merge(GSet1, GSet2).

% @pre -
% @post -
schedule() ->

    Type = state_gset,
    A = {<<"a">>, Type},
    B = {<<"b">>, Type},
    C = {<<"c">>, Type},

    lasp:declare(A, Type),
    lasp:declare(B, Type),
    lasp:declare(C, Type),

    lasp:stream(C, fun(Value) ->
        io:format("C=~w~n", [sets:to_list(Value)])  
    end),

    achlys_process:start_dag_link(
        [A, B], C,
        fun(Results) ->
            case Results of [GSet1, GSet2] ->
                union(GSet1, GSet2)
            end
        end
    ),

    timer:sleep(1000),
    lasp:update(A, {add, 1}, self()),
    lasp:update(B, {add, 2}, self()),
    ok.

% @pre -
% @post -
add_values(Name, Values) ->
    Type = state_gset,
    ID = {erlang:list_to_binary(erlang:atom_to_list(Name)), Type},
    erlang:spawn(fun() ->
        lists:foreach(fun(N) ->
            lasp:update(ID, {add, N}, self()),
            timer:sleep(500)
        end, Values)
    end).

% @pre -
% @post -
debug() ->
    Names = [<<"a">>, <<"b">>, <<"c">>],
    Type = state_gset,
    Values = lists:map(fun(Name) ->
        ID = {Name, Type},
        case lasp:query(ID) of
            {ok, Value} -> sets:to_list(Value)
        end
    end, Names),
    io:format("Values=~w~n", [Values]).
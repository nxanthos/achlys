-module(dataflow).
-export([
    test_map/0,
    test_filter/0,
    test_fold/0,
    test_distributed_fold/0,
    test_dag_link/0,
    test_process/0,
    test_min_with_dag/0,
    debug/2
]).

% @pre -
% @post -
test_map() ->

    IVar = {<<"ivar">>, state_orset},
    OVar = {<<"ovar">>, state_orset},

    lasp:declare(IVar, state_orset),
    lasp:declare(OVar, state_orset),

    N = 10,
    lasp:update(IVar, {add_all, lists:seq(1, N)}, self()),
    lasp:read(IVar, {cardinality, N}),

    Fun = fun(K) -> K + 1 end,
    lasp:map(IVar, Fun, OVar),
    lasp:read(OVar, {cardinality, N}),

    debug(IVar, "ivar"),
    debug(OVar, "ovar"),

    lasp:update(IVar, {rmv, rand:uniform(N)}, self()),
    lasp:read(IVar, {cardinality, N - 1}),

    debug(IVar, "ivar"),
    debug(OVar, "ovar"),

    % dataflow:test_map().
    ok.

% @pre -
% @post -
test_filter() ->

    IVar = {<<"ivar">>, state_orset},
    OVar = {<<"ovar">>, state_orset},

    lasp:declare(IVar, state_orset),
    lasp:declare(OVar, state_orset),

    N = 10,
    lasp:update(IVar, {add_all, lists:seq(1, N)}, self()),
    lasp:read(IVar, {cardinality, N}),

    Fun = fun(Value) -> Value rem 2 == 0 end,
    lasp:filter(IVar, Fun, OVar),
    lasp:read(OVar, {cardinality, erlang:trunc(N / 2)}),
    
    debug(IVar, "ivar"),
    debug(OVar, "ovar"),

    lasp:update(IVar, {rmv, 2}, self()),
    lasp:read(IVar, {cardinality, erlang:trunc(N / 2) - 1}),

    debug(IVar, "ivar"),
    debug(OVar, "ovar"),

    % dataflow:test_filter().
    ok.

% @pre -
% @post -
test_fold() ->

    {ok, {IVar, _, _, _}} = lasp:declare(orset),
    {ok, {OVar, _, _, _}} = lasp:declare(pncounter),

    io:format("IVar: ~p~n", [lasp:query(IVar)]),
    io:format("OVar: ~p~n", [lasp:query(OVar)]),

    N = 10,
    achlys_util:repeat(N, fun(K) ->
        lasp:update(IVar, {add, K}, self())
    end),

    lasp:stream(IVar, fun(Set) ->
        List = sets:to_list(Set),
        io:format("List: ~w~n", [List])
    end),
    lasp:stream(OVar, fun(Sum) ->
        io:format("Sum: ~p~n", [Sum])
    end),

    Fun = fun(X, Acc) ->
        % io:format("X=~p Acc=~p~n", [X, Acc]),
        [{increment, X}] % Sum of all values
    end,

    % lasp:update(OVar, decrement, self()),
    % lasp:update(OVar, {decrement, 5}, self()),
    % lasp:update(OVar, increment, self()),
    % lasp:update(OVar, {increment, 5}, self()),

    lasp:fold(IVar, Fun, OVar),
    timer:sleep(1000),
    lasp:update(IVar, {rmv, 5}, self()),
    timer:sleep(1000),
    lasp:update(IVar, {rmv, 2}, self()),
    timer:sleep(1000),
    lasp:update(IVar, {add, 20}, self()),

    % dataflow:test_fold().
    ok.

% @pre -
% @post -
test_distributed_fold() ->

    {ok, {IVar, _, _, _}} = lasp:declare(orset),
    {ok, {OVar, _, _, _}} = lasp:declare(pncounter),

    io:format("IVar=~p~nOVar=~p~n", [IVar, OVar]),

    N = 10,
    achlys_util:repeat(N, fun(K) ->
        lasp:update(IVar, {add, K}, self())
    end),

    Fun = fun(X, _) -> [{increment, X}] end,
    lasp:fold(IVar, Fun, OVar),

    Task = achlys:declare(mytask, all, single, fun() ->
        timer:sleep(1000),
        lasp:update(IVar, {rmv, 5}, self()),
        timer:sleep(1000),
        lasp:update(IVar, {rmv, 2}, self()),
        timer:sleep(1000),
        debug(OVar, "OVar")
    end),

    achlys:bite(Task),

    % dataflow:test_distributed_fold().
    ok.

% @pre -
% @post -
test_dag_link() ->

    IVar = {<<"ivar">>, state_orset},
    OVar = {<<"ovar">>, state_orset},

    lasp:declare(IVar, state_orset),
    lasp:declare(OVar, state_orset),

    lasp_process:start_dag_link([
        [{IVar, fun(ID, Threshold) -> % Read function
            io:format("Read ID: ~p~n", [ID]),
            io:format("Read Threshold: ~p~n", [Threshold]),
            lasp:read(ID, Threshold) % Synchronous read
        end}],
        fun(Tuple) -> % Transformation function
            io:format("Tuple: ~p~n", [Tuple]),
            case Tuple of {ID, Type, Metadata, Values} ->
                io:format("Transform ID: ~p~n", [ID]),
                io:format("Transform Type: ~p~n", [Type]),
                io:format("Transform Metadata: ~p~n", [Metadata]),
                io:format("Transform Values: ~p~n", [Values]),
                Values % Identity transformation
            end
        end,
        {OVar, fun(ID, Values) -> % Write function
            io:format("Write OVar: ~p~n", [ID]),
            io:format("Write Value: ~p~n", [Values]),
            lasp:bind(ID, Values)
            % lasp:update(ID, {add, Values}, self())
        end}
    ]),

    timer:sleep(500),
    lasp:update(IVar, {add, 1}, self()),
    lasp:update(IVar, {add, 2}, self()),
    timer:sleep(500),

    debug(IVar, "ivar"),
    debug(OVar, "ovar"),

    % dataflow:test_dag_link().
    ok.

get_timestamp() -> 
    erlang:unique_integer([monotonic, positive]).    

test_min_with_dag() ->

    Type = state_lwwregister,
    {ok, {A, _, _, _}} = lasp:declare({<<"a">>, Type}, Type),
    {ok, {B, _, _, _}} = lasp:declare({<<"b">>, Type}, Type),
    {ok, {C, _, _, _}} = lasp:declare({<<"c">>, Type}, Type),
    {ok, {D, _, _, _}} = lasp:declare({<<"d">>, Type}, Type),

    lasp_process:start_dag_link([ % Read function
        [{A, fun(ID, Threshold) ->
            lasp:read(ID, Threshold)
        end},
        {B, fun(ID, Threshold) ->
            lasp:read(ID, Threshold)
        end},
        {C, fun(ID, Threshold) ->
            lasp:read(ID, Threshold)
        end}],
        fun(T1, T2, T3) -> % Transformation function
            {_, _, _, V1} = T1,
            {_, _, _, V2} = T2,
            {_, _, _, V3} = T3,
            [V1, V2, V3]
        end,
        {D, fun(ID, Values) -> % Write function
            io:format("Write OVar: ~p~n", [ID]),
            io:format("Write Value: ~p~n", [Values]),
            L = lists:filter(fun(Value) ->
                case Value of {_, {_, E}} ->
                    not (E == undefined)
                end
            end, Values),
            case L of [H|T] ->
                Min = lists:foldl(fun(Current, Minimum) ->
                    case {Current, Minimum} of {
                        {_, {_, E1}},
                        {_, {_, E2}}
                    } ->
                        case E1 < E2 of
                            true -> Current;
                            false -> Minimum
                        end
                    end
                end, H, T),
                lasp:bind(ID, Min)
            end
        end}
    ]),

    % Update the variable: 

    Debug = fun() ->
        debug(A, "a"),
        debug(B, "b"),
        debug(C, "c"),
        debug(D, "d")
    end,

    lasp:update(A, {set, get_timestamp(), 0}, self()),
    lasp:update(B, {set, get_timestamp(), 1}, self()),
    lasp:update(C, {set, get_timestamp(), 2}, self()),
    
    Debug(),

    timer:sleep(2000),
    lasp:update(A, {set, get_timestamp(), -42}, self()),
    timer:sleep(2000),

    Debug(),

    % dataflow:test_min_with_dag().
    ok.

% @pre -
% @post -
test_process() ->

    IVar = {<<"ivar">>, state_orset},
    OVar = {<<"ovar">>, state_orset},

    lasp:declare(IVar, state_orset),
    lasp:declare(OVar, state_orset),

    {ok, S1} = lasp_process:init([
        [{IVar, fun(ID, Threshold) -> % Read function
            io:format("Read ID: ~p~n", [ID]),
            io:format("Read Threshold: ~p~n", [Threshold]),
            lasp:read(ID, Threshold) % Synchronous read
        end}],
        fun(Key, Value) -> % Transformation function
            io:format("Transform Value: ~p~n", [Value]),
            #{Key => #{value => Value, parents => [], children => []}}
        end,
        {OVar, fun(ID, Value) -> % Write function
            io:format("Write ID: ~p~n", [ID]),
            io:format("Write Value: ~p~n", [Value]),
            lasp:update(ID, {add, Value}, self())
        end}
    ]),

    io:format("State S1: ~w~n", [S1]),

    {Key1, Value1} = {ten, 10},
    {Key2, Value2} = {twenty, 20},

    % The first argument of `lasp_process:process` is a list containing the
    % arguments that will be passed to the transformation function. The arity
    % of the transformation function will have to correspond to the number of
    % items present in the list.

    {ok, R1} = lasp_process:process([Key1, Value1], S1),
    io:format("R1: ~w~n", [R1]),
    {ok, R2} = lasp_process:process([Key2, Value2], S1),
    io:format("R2: ~w~n", [R2]),

    lasp:update(IVar, {add, {Key1, Value1}}, self()),
    lasp:update(IVar, {add, {Key2, Value2}}, self()),

    % Synchronous read
    case lasp_process:read(S1) of {ok, Functions, S2} ->
        lists:foreach(fun(Function) ->
            Info = erlang:fun_info(Function, arity),
            Value = erlang:apply(Function, [undefined]),
            io:format("Info: ~p~n", [Info]),
            io:format("State S2: ~p~n", [S2]),
            io:format("Value: ~p~n", [Value])
        end, Functions)
    end,

    timer:sleep(500),
    debug(IVar, "ivar"),
    debug(OVar, "ovar"),
    ok.

% @pre -
% @post -
debug({_, _} = ID, Name) ->
    {ok, Result} = lasp:query(ID),
    case sets:is_set(Result) of
        true ->
            List = sets:to_list(Result),
            io:format("Variable (~p) -> ~w~n", [Name, List]);
        false ->
            io:format("Variable (~p) -> ~w~n", [Name, Result])
    end.
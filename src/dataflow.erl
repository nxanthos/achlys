-module(dataflow).
-export([
    test_map/0,
    test_filter/0,
    test_fold/0,
    test_distributed_fold/0,
    test_dag_link/0,
    test_process/0,
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
            case Tuple of {ID, Type, Metadata, {_, Values}} ->
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
            lasp:update(ID, {add, Values}, self())
        end}
    ]),

    timer:sleep(500),
    lasp:update(IVar, {add, 1}, self()),
    lasp:update(IVar, {add, 2}, self()),
    timer:sleep(500),

    debug(IVar, "ivar"),
    debug(OVar, "ovar"),
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
        fun(Value) -> % Transformation function
            io:format("Transform Value: ~p~n", [Value]),
            Value
        end,
        {OVar, fun(ID, Value) -> % Write function
            io:format("Write ID: ~p~n", [ID]),
            io:format("Write Value: ~p~n", [Value]),
            lasp:update(ID, {add, Value}, self())
        end}
    ]),

    io:format("State S1: ~w~n", [S1]),

    % The first argument of `lasp_process:process` is a list containing the
    % arguments that will be passed to the transformation function. The arity
    % of the transformation function will have to correspond to the number of
    % items present in the list.

    {ok, R} = lasp_process:process([42], S1),
    io:format("R: ~w~n", [R]),
    
    lasp:update(IVar, {add, 10}, self()),

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
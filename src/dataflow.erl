-module(dataflow).
-export([
    test_map/0,
    test_filter/0,
    test_fold/0,
    test_dag_link/0,
    test_process/0,
    debug/0
]).

% @pre -
% @post -
test_map() ->

    IVar = {<<"ivar-process">>, state_orset},
    OVar = {<<"ovar-process">>, state_orset},

    lasp:declare(IVar, state_orset),
    lasp:declare(OVar, state_orset),

    N = 10,
    lasp:update(IVar, {add_all, lists:seq(1, N)}, self()),
    lasp:read(IVar, {cardinality, N}),

    Fun = fun(K) -> K + 1 end,
    lasp:map(IVar, Fun, OVar),
    lasp:read(OVar, {cardinality, N}),

    debug("ivar-process"),
    debug("ovar-process"),

    lasp:update(IVar, {rmv, rand:uniform(N)}, self()),
    lasp:read(IVar, {cardinality, N - 1}),

    debug("ivar-process"),
    debug("ovar-process"),

    % dataflow:test_map().
    ok.

% @pre -
% @post -
test_filter() ->

    IVar = {<<"ivar-process">>, state_orset},
    OVar = {<<"ovar-process">>, state_orset},

    lasp:declare(IVar, state_orset),
    lasp:declare(OVar, state_orset),

    N = 10,
    lasp:update(IVar, {add_all, lists:seq(1, N)}, self()),
    lasp:read(IVar, {cardinality, N}),

    Fun = fun(Value) -> Value rem 2 == 0 end,
    lasp:filter(IVar, Fun, OVar),
    lasp:read(OVar, {cardinality, erlang:trunc(N / 2)}),
    
    debug("ivar-process"),
    debug("ovar-process"),

    lasp:update(IVar, {rmv, 2}, self()),
    lasp:read(IVar, {cardinality, erlang:trunc(N / 2) - 1}),

    debug("ivar-process"),
    debug("ovar-process"),

    % dataflow:test_filter().
    ok.

% @pre -
% @post -
test_fold() ->

    {ok, {IVar, _, _, _}} = lasp:declare(orset),
    {ok, {OVar, _, _, _}} = lasp:declare(pncounter),

    N = 10,
    lasp:update(IVar, {add_all, lists:seq(1, N)}, self()),
    lasp:read(IVar, {cardinality, N}),

    Fun = fun(X, Acc) ->
        % X + Acc
        % io:format("X=~p Acc=~p~n", [X, Acc]),
        [increment, increment]
    end,

    % lasp:update(OVar, decrement, self()),
    % lasp:update(OVar, increment, self()),

    lasp:fold(IVar, Fun, OVar),
    timer:sleep(1000),

    {ok, S1} = lasp:query(IVar),
    {ok, S2} = lasp:query(OVar),
    io:format("IVar: ~w~n", [sets:to_list(S1)]),
    io:format("OVar: ~w~n", [S2]),

    lasp:update(IVar, {rmv, 5}, self()),
    timer:sleep(1000),

    {ok, S3} = lasp:query(IVar),
    {ok, S4} = lasp:query(OVar),
    io:format("IVar: ~w~n", [sets:to_list(S3)]),
    io:format("OVar: ~w~n", [S4]),

    % dataflow:test_fold().
    ok.

% @pre -
% @post -
test_dag_link() ->

    IVar = {<<"ivar-process">>, state_orset},
    OVar = {<<"ovar-process">>, state_orset},

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

    debug("ivar-process"),
    debug("ovar-process"),
    ok.

% @pre -
% @post -
test_process() ->

    IVar = {<<"ivar-process">>, state_orset},
    OVar = {<<"ovar-process">>, state_orset},

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

    % The first argument of `lasp_process:process` is a list containing the arguments that will be passed to the transformation function. The arity of the transformation function will have to correspond to the number of items present in the list.

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
    debug("ivar-process"),
    debug("ovar-process"),
    ok.

% @pre -
% @post -
debug() ->
    debug("ivar-process"),
    debug("ovar-process").

% @pre -
% @post -
debug(Name) ->
    {ok, Set} = lasp:query({
        erlang:list_to_binary(Name),
        state_orset
    }),
    Content = sets:to_list(Set),
    io:format("Variable (~p) -> ~w~n", [Name, Content]).
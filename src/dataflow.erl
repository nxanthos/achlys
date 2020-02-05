-module(dataflow).
-export([
    test_map/0,
    test_fold/0,
    test_dag_link/0,
    test_process/0,
    debug/0
]).

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
            io:format("Write OVar: ~p~n", [OVar]),
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
test_map() ->

    IVar = {<<"ivar-process">>, state_orset},
    OVar = {<<"ovar-process">>, state_orset},

    lasp:declare(IVar, state_orset),
    lasp:declare(OVar, state_orset),

    achlys_util:repeat(10, fun(K) ->
        lasp:update(IVar, {add, K}, self())
    end),

    Fun = fun(K) -> K + 1 end,
    lasp:map(IVar, Fun, OVar),
    timer:sleep(500),

    debug("ivar-process"),
    debug("ovar-process"),
    ok.

% @pre -
% @post -
test_fold() ->

    IVar = {<<"ivar-process">>, state_orset},
    OVar = {<<"ovar-process">>, state_orset},

    lasp:declare(IVar, state_orset),
    lasp:declare(OVar, state_orset),

    achlys_util:repeat(10, fun(K) ->
        lasp:update(IVar, {add, K}, self())
    end),

    % TODO:
    % Fun = fun(...) -> ... end,
    % lasp:fold(IVar, Fun, OVar),
    % timer:sleep(500),

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
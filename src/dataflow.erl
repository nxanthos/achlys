-module(dataflow).
-export([
    test_dag_link/0
]).

% @pre -
% @post -
test_dag_link() ->

    Type = state_gset,
    IVar = {<<"ivar-process">>, Type},
    OVar = {<<"ovar-process">>, Type},

    lasp:declare(IVar, state_gset),
    lasp:declare(OVar, state_gset),

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
        {OVar, fun(OVar, Values) -> % Write function
            io:format("Write OVar: ~p~n", [OVar]),
            io:format("Write Value: ~p~n", [Values]),
            lasp:update(OVar, {add, Values}, self())
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
debug(Name) ->
    {ok, Set} = lasp:query({
        erlang:list_to_binary(Name),
        state_gset
    }),
    Content = sets:to_list(Set),
    io:format("Variable (~p) -> ~w~n", [Name, Content]).
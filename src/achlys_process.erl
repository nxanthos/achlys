-module(achlys_process).
-behaviour(gen_flow).
-export([
    init/1,
    process/2,
    read/1
]).

% API:

-export([
    start_dag_link/3
]).

% @pre -
% @post -
init([IVars, OVar, Transform]) ->
    {ok, #{
        ivars => IVars,
        ovar => OVar,
        transform => Transform
    }}.

% @pre -
% @post -
process(Cache, State) ->
    Predicate = fun(X) -> X =:= undefined end,
    Processed = case lists:any(Predicate, Cache) of
        true -> false;
        false ->
            case State of #{
                ovar := OVar,
                transform := Transform
            } ->
                Result = apply_transform(Cache, Transform),
                case lasp:bind(OVar, Result) of
                    {error, not_found} ->
                        io:format("Could not set the output variable (~p)!~n", [OVar]),
                        false;
                    {ok, {_, _, _, _}} -> true
                end
            end
    end,
    {ok, {Processed, State}}.

% @pre -
% @post -
read(State) ->
    {ok, lists:map(fun(IVar) ->
        fun(CacheValue) ->
            % io:format("Cache value: ~w~n", [CacheValue]),
            Value = case CacheValue of
                undefined -> undefined;
                {_, _, _, V} -> V
            end,
            case lasp:read(IVar, {strict, Value}) of
                {ok, Result} ->
                    % io:format("Read result: ~w~n", [Result]),
                    Result;
                {error, not_found} ->
                    io:format("Unkonwn variable~n"),
                    error
            end
        end
    end, maps:get(ivars, State, [])), State}.

% Helpers:

% @pre -
% @post -
apply_transform(Cache, Function) ->
    erlang:apply(Function, [
        lists:map(fun(Tuple) ->
            case Tuple of
                {_, _, _, Value} -> Value;
                _ -> Tuple
            end
        end, Cache)
    ]).

% API:

% @pre -
% @post -
start_dag_link(IVars, OVar, Transform) ->
    Task = achlys:declare(mytask, all, single, fun() ->
        io:format("Starting the task to preseve the symetrie~n"),
        achlys_process_sup:start_child([IVars, OVar, Transform])
    end),
    io:format("~w~n", [achlys:bite(Task)]),
    ok.
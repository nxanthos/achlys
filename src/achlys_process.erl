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
init([IVars, OVar, Fun]) ->
    {ok, #{
        ivars => IVars,
        ovar => OVar,
        function => Fun
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
                function := Fun
            } ->
                Result = erlang:apply(Fun, [get_values(Cache)]),
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
    IVars = maps:get(ivars, State, []),
    {ok, lists:map(fun(IVar) ->
        gen_read_fun(IVar)
    end, IVars), State}.

% Helpers:

% @pre -
% @post -
gen_read_fun(IVar) ->
    fun(CacheValue) ->
        Value = case CacheValue of
            undefined -> undefined;
            {_, _, _, V} -> V
        end,
        case lasp:read(IVar, {strict, Value}) of
            {ok, Result} ->
                Result;
            {error, not_found} ->
                io:format("Unknown variable~n"),
                error
        end
    end.

% @pre -
% @post -
get_values(Cache) ->
    lists:map(fun(Tuple) ->
        case Tuple of
            {_, _, _, Value} -> Value;
            _ -> Tuple
        end
    end, Cache).

% API:

% @pre -
% @post -
start_dag_link(IVars, OVar, Fun) ->        
    case lasp_unique:unique() of
        {ok, Name} ->
            Task = achlys:declare(Name, all, single, fun() ->
                achlys_process_sup:start_child([IVars, OVar, Fun])
            end),
            achlys:bite(Task)
    end.
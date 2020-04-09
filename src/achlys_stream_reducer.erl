-module(achlys_stream_reducer).
-export([
    start_link/1,
    init/2
]).

% API:

-export([
    start/3
]).

% @pre -
% @post -
start_link(Args) ->
    proc_lib:start_link(?MODULE, init, [self(), Args]).

% @pre -
% @post -
init(Parent, Args) ->
    process_flag(trap_exit, true),
    proc_lib:init_ack(Parent, {ok, self()}),
    case Args of
        [Entries, State] ->
            Cache = init_cache(Entries),
            loop(Cache, State, #{});
        [Entries, State, Callback] ->
            Cache = init_cache(Entries),
            loop(Cache, State, #{
                callback => Callback
            })
    end.

% @pre -
% @post -
start(Entries, State, Options) ->
    achlys_stream_reducer_sup:start_child([Entries, State, Options]).

% @pre -
% @post -
init_cache(Entries) ->
    lists:foldl(fun(Entry, Cache) ->
        {ID, Fun} = Entry,
        case orddict:is_key(ID, Cache) of
            true ->
                orddict:update(ID, fun(Map) ->
                    maps:update_with(functions, fun(Funs) ->
                        [Fun|Funs]
                    end, Map)
                end, Cache);
            false ->
                orddict:store(ID, #{
                    var_state => undefined,
                    functions => [Fun]
                }, Cache)
        end
    end, orddict:new(), Entries).

% @pre -
% @post -
terminate(Pids) ->
    lists:foreach(fun(Pid) ->
        erlang:exit(Pid, kill)
    end, Pids).

% @pre -
% @post -
loop(Cache, State, Options) ->

    Self = self(),
    Pids = lists:map(fun({ID, #{var_state := OldVarState}}) ->
        erlang:spawn(fun() ->
            case lasp:read(ID, {strict, OldVarState}) of
                {ok, {_, _, _, NewVarState}} ->
                    Self ! {ok, ID, NewVarState};
                {error, not_found} ->
                    io:format("Unknown variable~n"),
                    error
            end
        end)
    end, orddict:to_list(Cache)),

    receive
        {ok, ID, NewVarState} ->
            terminate(Pids),
            case orddict:find(ID, Cache) of {ok, #{
                    var_state := OldVarState,
                    functions := Funs
                }} ->

                    {_, Type} = ID,
                    
                    UpdatedCache = orddict:update(ID, fun(Map) ->
                        maps:merge(Map, #{
                            var_state => NewVarState
                        })
                    end, Cache),

                    UpdatedState = lists:foldl(fun(Fun, NextState) ->
                        Operations = get_delta_operations(
                            Type,
                            OldVarState,
                            NewVarState
                        ),
                        lists:foldl(fun(Operation, Acc) ->
                            erlang:apply(Fun, [Acc, Operation])
                        end, NextState, Operations)
                    end, State, Funs),

                    case maps:find(callback, Options) of
                        {ok, Callback} ->
                            erlang:apply(Callback, [UpdatedState]);
                        _ -> ok
                    end,

                    loop(UpdatedCache, UpdatedState, Options);

                error ->
                    io:format("The variable does not exist")
            end;
        _ ->
            io:format("Other message~n~n")
    end.

% @pre -
% @post -
get_delta_operations(Type, undefined, NewVarState) ->
    get_delta_operations(Type, lasp_type:new(Type), NewVarState);
get_delta_operations(Type, OldVarState, NewVarState) ->
    Types = [
        {state_gcounter, state_gcounter_ext},
        {state_pncounter, state_pncounter_ext},
        {state_gset, state_gset_ext},
        {state_orset, state_orset_ext},
        {state_twopset, state_twopset_ext}
    ],
    Predicate = fun({Current, _}) -> Current == Type end,
    case lists:search(Predicate, Types) of {value, {_, Module}} ->
        Module:delta_operations(OldVarState, NewVarState);
    _ -> [] end.
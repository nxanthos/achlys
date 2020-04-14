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
        [Entries, State, Options] ->
            Store = init_store(Entries),
            loop(Store, State, Options)
    end.

% @pre -
% @post -
start(Entries, State, Callback) ->
    achlys_stream_reducer_sup:start_child([Entries, State, #{
        callback => Callback
    }]).

% @pre -
% @post -
init_store(Entries) ->
    lists:foldl(fun(Entry, Store) ->
        {ID, Fun} = Entry,
        case orddict:is_key(ID, Store) of
            true ->
                orddict:update(ID, fun(Orddict) ->
                    orddict:append(functions, Fun, Orddict)
                end, Store);
            false ->
                Orddict = orddict:from_list([
                    {cache, undefined},
                    {functions, []}
                ]),
                orddict:store(ID, orddict:append(functions, Fun, Orddict), Store)
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
spawn_read_funs(Self, Store) ->
    L = orddict:to_list(Store),
    lists:map(fun({ID, Orddict}) ->
        OldVarState = orddict:fetch(cache, Orddict),
        erlang:spawn(fun() ->
            case lasp:read(ID, {strict, OldVarState}) of
                {ok, {_, _, _, NewVarState}} ->
                    Self ! {ok, ID, NewVarState};
                {error, not_found} ->
                    io:format("Unknown variable~n"),
                    error
            end
        end)
    end, L).

% @pre -
% @post -
update_state(ID, Operations, {Store, State}) ->
    Orddict = orddict:fetch(ID, Store),
    Funs = orddict:fetch(functions, Orddict),
    lists:foldl(fun(Fun, Acc1) ->
        lists:foldl(fun(Operation, Acc2) ->
            erlang:apply(Fun, [Acc2, Operation])
        end, Acc1, Operations)
    end, State, Funs).

% @pre -
% @post -
update_store(ID, Orddict1, Store) ->
    orddict:update(ID, fun(Orddict2) ->
        orddict:merge(fun(_, V1, _) ->
            V1
        end, Orddict1, Orddict2)
    end, Store).

% @pre -
% @post -
loop(Store, State, Options) ->

    Self = self(),
    Pids = spawn_read_funs(Self, Store),

    receive
        {ok, ID, NewVarState} ->

            terminate(Pids),
            Orddict = orddict:fetch(ID, Store),

            {_, Type} = ID,
            OldVarState = orddict:fetch(cache, Orddict),
            Operations = achlys_util:get_delta_operations(
                Type,
                OldVarState,
                NewVarState
            ),

            UpdatedState = update_state(ID, Operations, {Store, State}),
            UpdatedStore = update_store(ID, orddict:from_list([
                {cache, NewVarState}
            ]), Store),

            % Callback:

            case maps:find(callback, Options) of
                {ok, Callback} ->
                    erlang:apply(Callback, [UpdatedState]);
                _ -> ok
            end,

            loop(UpdatedStore, UpdatedState, Options);
        _ ->
            terminate(Pids),
            loop(Store, State, Options)
    end.

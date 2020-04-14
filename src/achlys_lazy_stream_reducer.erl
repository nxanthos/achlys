-module(achlys_lazy_stream_reducer).
-export([
    start_link/1,
    init/2
]).

% API:

-export([
    start/2,
    get_state/1
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
            Store = init_store(Entries),
            loop(Store, State)
    end.

% @pre -
% @post -
start(Entries, State) ->
    achlys_lazy_stream_reducer_sup:start_child([Entries, State]).

% @pre -
% @post -
init_store(Entries) ->
    lists:foldl(fun({ID, Fun}, Store) ->
        case orddict:is_key(ID, Store) of
            true ->
                orddict:update(ID, fun(Orddict) ->
                    orddict:append(functions, Fun, Orddict)
                end, Store);
            false ->
                Orddict = orddict:from_list([
                    {cache, undefined},
                    {functions, []},
                    {clock, lasp_vclock:fresh()}
                ]),
                orddict:store(ID, orddict:append(functions, Fun, Orddict), Store)
        end
    end, orddict:new(), Entries).

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
update_store(ID, Orddict1, {Store, _}) ->
    orddict:update(ID, fun(Orddict2) ->
        orddict:merge(fun(_, V1, _) ->
            V1
        end, Orddict1, Orddict2)
    end, Store).

% @pre -
% @post -
update(Store, State) ->
    L = orddict:to_list(Store),
    lists:foldl(fun({ID, Orddict}, Acc) ->
        case lasp:read(ID, undefined) of
            {ok, {_, _, Metadata, NewVarState}} ->

                OldClock = orddict:fetch(clock, Orddict),
                NewClock = orddict:fetch(clock, Metadata),

                case lasp_vclock:equal(OldClock, NewClock) of
                    true -> Acc;
                    false ->

                        {_, Type} = ID,
                        OldVarState = orddict:fetch(cache, Orddict),
                        Operations = achlys_util:get_delta_operations(
                            Type,
                            OldVarState,
                            NewVarState
                        ),

                        UpdatedState = update_state(ID, Operations, Acc),
                        UpdatedStore = update_store(ID, orddict:from_list([
                            {cache, NewVarState},
                            {clock, NewClock}
                        ]), Acc),

                        {UpdatedStore, UpdatedState}
                end
        end
    end, {Store, State}, L).

% @pre -
% @post -
loop(Store, State) ->
    receive
        {get_state, From} ->
            {UpdatedStore, UpdatedState} = update(Store, State),
            From ! {ok, UpdatedState},
            loop(UpdatedStore, UpdatedState)
    end.

% @pre -
% @post -
get_state(Pid) ->
    Self = self(),
    Pid ! {get_state, Self},
    receive {ok, State} -> State end.
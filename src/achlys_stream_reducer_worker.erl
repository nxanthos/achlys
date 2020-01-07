-module(achlys_stream_reducer_worker).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-export([
    start_link/0,
    init/1,
    handle_cast/2,
    handle_call/3
]).

% Starting function:

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, #{}}.

% Cast:

handle_cast({reduce, ID, Reducer, Acc, Callback}, _State) ->
    lasp:stream(ID, fun(Values) ->
        gen_server:cast(?SERVER, {on_change, ID, Values}),
        gen_server:cast(?SERVER, {on_update, Callback})
    end),
    {noreply, #{
        reducer => Reducer,
        cache => [],
        acc => Acc
    }};

handle_cast({on_change, ID, Values}, State) ->
    case State of #{reducer := Reducer, cache := V1, acc := Acc} ->
        {_, Type} = ID,
        V2 = lists:sort(sets:to_list(Values)),
        B = {Type, V2},
        A = {state, {Type, V1}},
        {_, Delta} = lasp_type:delta(Type, B, A),
        {noreply, State#{
            cache := V2,
            acc := lists:foldl(Reducer, Acc, Delta)
        }}
    end;

handle_cast({on_update, Callback}, State) ->
    case State of #{acc := Acc} ->
        Callback(Acc),
        {noreply, State}
    end.

% Call:

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

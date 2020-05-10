-module(test_broadcast).

-behaviour(plumtree_broadcast_handler).
-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% plumtree_broadcast_handler callbacks
-export([broadcast_data/1,
         merge/2,
         is_stale/1,
         graft/1,
         exchange/1]).
%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([start_link/0,
         test/0
]).

-record(state, {}).
-type state()           :: #state{}.

-spec start_link() -> ok.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Return a two-tuple of message id and payload from a given broadcast
-spec broadcast_data(any()) -> {any(), any()}.
broadcast_data(Data) ->
    io:format("Send: ~p~n", [Data]),
    MsgId = {node(), erlang:unique_integer([monotonic])},
    {MsgId, Data}.

%% Given the message id and payload, merge the message in the local state.
%% If the message has already been received return `false', otherwise return `true'
-spec merge(any(), any()) -> boolean().
merge({_, Id} = MsgId, Payload) ->
    case is_stale(MsgId) of
        true ->
            false;
        false ->
            io:format("Receive: ~p~n", [Payload]),
            gen_server:call(?MODULE,{merge, MsgId, Payload}, infinity),
            true
    end.

%% Return true if the message (given the message id) has already been received.
%% `false' otherwise
-spec is_stale(any()) -> boolean().
is_stale({_, Id} = MsgId) ->
    gen_server:call(?MODULE, {is_stale, MsgId}, infinity).

%% Return the message associated with the given message id. In some cases a message
%% has already been sent with information that subsumes the message associated with the given
%% message id. In this case, `stale' is returned.
-spec graft(any()) -> stale | {ok, any()} | {error, any()}.
graft(MsgId) ->
    {ok, MsgId}.

%% Trigger an exchange between the local handler and the handler on the given node.
%% How the exchange is performed is not defined but it should be performed as a background
%% process and ensure that it delivers any messages missing on either the local or remote node.
%% The exchange does not need to account for messages in-flight when it is started or broadcast
%% during its operation. These can be taken care of in future exchanges.
-spec exchange(node()) -> {ok, pid()} | {error, term()}.
exchange(_Node) ->
    {ok, self()}.

test() ->
    erlang:spawn(fun() -> plumtree_broadcast:broadcast("hello", ?MODULE) end).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([[any()], ...]) -> {ok, state()}.
init([]) ->
    {ok, orddict:new()}.

%% @private
-spec handle_call(term(), {pid(), term()}, state()) -> {reply, term(), state()}.
handle_call({is_stale, Id}, _From, State) ->
    {reply, orddict:is_key(Id, State), State};

handle_call({graft, Id}, _From, State) ->
    io:format("GRAFT~n"),
    Result = case orddict:is_key(Id, State) of
        false ->
            {error, {not_found, Id}};
        true ->
            {ok, Id}
    end,
    {reply, Result, State};

handle_call({merge, Id, Payload}, _From, State) ->
    State2 = orddict:append(Id, Payload, State),
    {reply, ok, State2};

handle_call(Msg, _From, State) ->
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
-spec handle_info({'DOWN', _, 'process', _, _}, state()) ->
    {noreply, state()}.
handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State) ->
    {noreply, State}.

%% @private
-spec terminate(term(), state()) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
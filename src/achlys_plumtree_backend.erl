-module(achlys_plumtree_backend).

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

%% transmission callbacks
-export([extract_log_type_and_payload/1]).

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
    MsgId = {node(), erlang:unique_integer([monotonic])},
    {MsgId, Data}.

%% Given the message id and payload, merge the message in the local state.
%% If the message has already been received return `false', otherwise return `true'
-spec merge(any(), any()) -> boolean().
merge({_, Id} = MsgId, {Module, Msg} = Message) ->
    case is_stale(Id) of
        true ->
            false;
        false ->
            gen_server:call(?MODULE,{merge, Id, Message}, infinity),
            gen_server:cast(Module, Msg),
            true
    end.

%% Return true if the message (given the message id) has already been received.
%% `false' otherwise
-spec is_stale(any()) -> boolean().
is_stale(Id) ->
    gen_server:call(?MODULE, {is_stale, Id}, infinity).

%% Return the message associated with the given message id. In some cases a message
%% has already been sent with information that subsumes the message associated with the given
%% message id. In this case, `stale' is returned.
-spec graft(any()) -> stale | {ok, any()} | {error, any()}.
graft({_, Id} = MsgId) ->
    gen_server:call(?MODULE, {graft, Id}, infinity).

%% Trigger an exchange between the local handler and the handler on the given node.
%% How the exchange is performed is not defined but it should be performed as a background
%% process and ensure that it delivers any messages missing on either the local or remote node.
%% The exchange does not need to account for messages in-flight when it is started or broadcast
%% during its operation. These can be taken care of in future exchanges.
-spec exchange(node()) -> {ok, pid()} | {error, term()}.
exchange(_Node) ->
    {ok, self()}.

test() ->
    Message = {achlys_mr, {notify, #{
                        header => #{
                            src => achlys_util:myself()
                        },
                        payload => #{
                            id => 0,
                            round => 1,
                            reduce => fun() -> 1 end,
                            pairs => [],
                            finished => false,
                            options => []
                        }}}},
    erlang:spawn(fun() -> achlys_plumtree_broadcast:broadcast(Message, ?MODULE) end).

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
    Result = case orddict:is_key(Id, State) of
        false ->
            {error, {not_found, Id}};
        true ->
            {ok, orddict:take(Id, State)}
    end,
    {reply, Result, State};

handle_call({merge, Id, Message}, _From, State) ->
    State1 = orddict:append(Id, Message, State),
    {reply, ok, State1};

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

%%%===================================================================
%%% Transmission functions
%%%===================================================================

extract_log_type_and_payload({prune, Root, From}) ->
    [{broadcast_protocol, {Root, From}}];
extract_log_type_and_payload({ignored_i_have, MessageId, _Mod, Round, Root, From}) ->
    [{broadcast_protocol, {MessageId, Round, Root, From}}];
extract_log_type_and_payload({graft, MessageId, _Mod, Round, Root, From}) ->
    [{broadcast_protocol, {MessageId, Round, Root, From}}];
extract_log_type_and_payload({broadcast, MessageId, Timestamp, _Mod, Round, Root, From}) ->
    [{broadcast_protocol, {Timestamp, MessageId, Round, Root, From}}];
extract_log_type_and_payload({i_have, MessageId, _Mod, Round, Root, From}) ->
    [{broadcast_protocol, {MessageId, Round, Root, From}}];
extract_log_type_and_payload(Message) ->
    lager:info("No match for extracted payload: ~p", [Message]),
    [].
-module(achlys_es).
-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).
-export([
    start_link/3,
    read/0,
    update/1,
    debug/0
]).

-define(SERVER, ?MODULE).

% @pre -
% @post -
start_link(ID, State, Handler) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [
        ID,
        State,
        Handler
    ], []).

% ID - ID of the variable
% State - Initial state
% Handler - Update function
init([ID, State, Handler]) ->
    lasp:declare(ID, state_gset),
    lasp:stream(ID, fun(Events) ->
        gen_server:cast(?SERVER, {propagate, Events})
    end),
    {ok, #{
        variable => ID,
        cache => sets:new(),
        state => State,
        handler => Handler
    }}.

% Synchronous:

handle_call(read, _From, Data) ->
    case Data of #{state := State} ->
        {reply, State, Data}
    end;
handle_call(_Request, _From, Data) ->
    {noreply, Data}.

% Asynchronous:

handle_cast({add_event, Event}, Data) ->
    case Data of #{variable := ID} ->
        lasp:update(ID, {add, Event}, self()),
        {noreply, Data}
    end;
handle_cast({propagate, E2}, Data) ->
    case Data of #{
        cache := E1,
        state := State,
        handler := Handler
    } ->
        Events = sets:to_list(sets:subtract(E2, E1)),
        {noreply, maps:merge(Data, #{
            cache => E2,
            state => lists:foldl(fun(Event, Current) ->
                erlang:apply(Handler, [Event, Current])
            end, State, Events)
        })}
    end;
handle_cast(debug, Data) ->
    io:format("Data=~p~n", [Data]),
    {noreply, Data};
handle_cast(_Request, Data) ->
    {noreply, Data}.

handle_info(_Info, Data) ->
    {noreply, Data}.

% API:

% @pre -
% @post -
read() ->
    gen_server:call(?SERVER, read).

% @pre -
% @post -
update(Fun) ->
    State = read(),
    erlang:apply(Fun, [State, fun(Event) ->
        gen_server:cast(?SERVER, {add_event, Event})
    end]).

% @pre -
% @post -
debug() ->
    gen_server:cast(?SERVER, debug).
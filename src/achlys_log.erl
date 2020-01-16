-module(achlys_log).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-export([
    start_link/0,
    init/1,
    handle_cast/2,
    terminate/2
]).

% Starting function:

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, #{
        fd => file:open("test.txt", [write])
    }}.

% Cast:

handle_cast({log, Text}, State) ->
    case State of #{fd := Fd} ->
        file:write(Fd, Text ++ "\n"),
        {noreply, State}
    end.

% Terminate:

terminate(normal, State) ->
    case State of #{fd := Fd} ->
        file:close(Fd)
    end.

% API:

benchmark() ->
    {Time, Value} = timer:tc(lists, seq, [1, 10]),
    gen_server:cast(?SERVER, {log, Time}).

% write() ->
%     {ok, Fd} = file:open("test.txt", [write]),
%     file:write(Fd, "Hello\n"),
%     file:write(Fd, "world"),
%     file:close(Fd).
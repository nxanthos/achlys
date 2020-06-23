-module(achlys_spawn_sup).
-behaviour(supervisor).
-export([
    init/1
]).
-export([
    start_link/0
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

% Supervisor:

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 1,
        period => 5
    },
    ChildSpecs = [
        #{
            id => achlys_spawn,
            start => {achlys_spawn, start_link, []},
            restart => permanent
        },
        #{
            id => achlys_spawn_monitor,
            start => {achlys_spawn_monitor, start_link, []},
            restart => permanent
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
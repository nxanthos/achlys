-module(achlys_process_sup).
-behaviour(supervisor).
-export([
    init/1
]).
-export([
    start_link/0,
    start_child/1
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Args) ->
    supervisor:start_child(?MODULE, [Args]).

% Supervisor:

init([]) ->
    {ok, {
        {simple_one_for_one, 10, 10}, [
            {gen_flow,
                {gen_flow, start_link, [achlys_process]},
                transient,
                5000,
                worker,
                [gen_flow]
            }
        ]}
    }.
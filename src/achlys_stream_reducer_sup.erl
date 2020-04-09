-module(achlys_stream_reducer_sup).
-behaviour(supervisor).
-export([
    init/1,
    start_child/1,
    start_link/0
]).

% @pre -
% @post -
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

% @pre -
% @post -
start_child(Args) ->
    supervisor:start_child(?MODULE, [Args]).

% @pre -
% @post -
init([]) ->
    % 1 restart per 5 seconds
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 1,
        period => 5
    },
    ChildSpecs = [#{
        id => achlys_stream_reducer,
        start => {achlys_stream_reducer, start_link, []},
        restart => transient,
        type => worker
    }],
    {ok, {SupFlags, ChildSpecs}}.
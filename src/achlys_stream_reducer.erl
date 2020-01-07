-module(achlys_stream_reducer).
-export([
    reduce/4
]).

reduce(ID, Reducer, Acc, Callback) ->
    % TODO: Instantiate a new gen-server for each call
    Name = achlys_stream_reducer_worker,
    achlys_stream_reducer_worker:start_link(),
    gen_server:cast(Name, {reduce,
        ID,
        Reducer,
        Acc,
        Callback
    }).
-module(achlys_mr_job).

% API:
-export([
    start_link/2
]).

-record(options, {
    max_batch_size :: integer()
}).

% @pre  ID is a unique ID, identifying the MapReduce
%       Entries is a list of tuples composed of a Variable and a map function {Var, fun}
%       Reduce is a reduce function
%       Options is map containing MapReduce options
% @post start the map phase and broadcast the data for the reduction
%       return the Pid of the master
start_link(ID, [Entries, Reduce, Options]) ->
    Parent = self(),
    Pid = erlang:spawn(fun() ->
        Pairs = map_phase(Entries),
        broadcast(achlys_mr, {notify, #{
            header => #{
                src => achlys_util:myself()
            },
            payload => #{
                id => ID,
                round => 0,
                reduce => Reduce,
                pairs => Pairs,
                finished => false,
                options => Options
            }
        }}),
        Round = 1,
        start_round(Parent, ID, Round, Pairs, Reduce, Options)
    end),
    logger:info("[MAPREDUCE][~p][M][~p]", [ID, achlys_util:myself()]),
    {ok, Pid};

% @pre -
% @post -
start_link(ID, [Round, Pairs, Reduce, Options]) ->
    Parent = self(),
    Pid = erlang:spawn(fun() ->
        start_round(Parent, ID, Round, Pairs, Reduce, Options)
    end),
    logger:info("[MAPREDUCE][~p][M][~p]", [ID, achlys_util:myself()]),
    {ok, Pid}.

% Map phase:

% @pre Entries is a list of tuples composed of a Variable and a map function {Var, fun}
% @post Return the result of the map phase
map_phase(Entries) ->
    lists:flatmap(fun({IVar, Map}) ->
        Values = achlys_util:query(IVar),
        lists:flatmap(fun(Value) ->
            erlang:apply(Map, [Value])
        end, Values)
    end, Entries).

% Reduce phase:

% @pre  Message is a message
%       Module is a module that handle the message Message
% e.g. broadcast(achlys_mr, {notify, #{}})
% @post Broadcast Msg
broadcast(Module, {notify, #{header := Header, payload := Payload}} = Message) ->
    %% Broadcast with plumtree to all other nodes
    io:format("Broadcasting the solution~n"),
    Msg = {Module, Message},
    achlys_plumtree_broadcast:broadcast(Msg, achlys_plumtree_backend).

    %% Broadcast with partisan to all (direct) neighbors
    % {ok, Neighbors} = achlys:members(),
    % lists:foreach(fun(Node) ->
    %     partisan_peer_service:cast_message(Node, Module, Message)
    % end, Neighbors).

% @pre -
% @post -
is_irreductible(InputPairs, OutputPairs) ->
    lists:sort(InputPairs) == lists:sort(OutputPairs).

% @pre -
% @post -
start_round(Parent, ID, Round, InputPairs, Reduce, Options) ->
    logger:info("[MAPREDUCE][~p][R][~p]", [ID, Round]),
    MaxRound = maps:get(max_round, Options),
    case Round =< MaxRound of
        false ->
            {error, "Max round reached"};
        true ->
            case get_reduction(InputPairs, Reduce, Options) of
                {error, Reason} ->
                    {error, Reason};
                {ok, OutputPairs} ->
                    Finished = is_irreductible(
                        InputPairs,
                        OutputPairs
                    ),

                    erlang:send(Parent, {new_round, ID, Round}),
                    broadcast(achlys_mr, {notify, #{
                        header => #{
                            src => achlys_util:myself()
                        },
                        payload => #{
                            id => ID,
                            round => Round,
                            reduce => Reduce,
                            pairs => OutputPairs,
                            finished => Finished,
                            options => Options
                        }
                    }}),

                    case Finished of
                        true ->
                            logger:info("[MAPREDUCE][~p][F]", [ID]),
                            OVar = maps:get(variable, Options),
                            erlang:send(Parent, {finish,
                                ID,
                                OVar,
                                OutputPairs
                            });
                        false ->
                            start_round(
                                Parent,
                                ID,
                                Round + 1,
                                OutputPairs,
                                Reduce,
                                Options
                            )
                    end
            end
    end.

% @pre -
% @post -
% Diviser en batch
% Retourner {error, Reason} si la fonction crash
% Retourner {ok, Pairs}
get_reduction(InputPairs, Reduce, Options) ->
    % TODO: Add method fork
    achlys_mr_dispatcher:start(InputPairs, Reduce, #options{
        max_batch_size = 10
    }).
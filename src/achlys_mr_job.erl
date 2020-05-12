-module(achlys_mr_job).

% API:
-export([
    start_link/2
]).

% @pre -
% @post -
start_link(ID, [Entries, Reduce, Options]) ->

    Parent = self(),
    Pid = erlang:spawn(fun() ->

        Pairs = map_phase(Entries),
        Round = 1,

        erlang:send(Parent, {new_round, ID, Round}),
        broadcast(achlys_mr, {notify, #{
            header => #{
                src => achlys_util:myself()
            },
            payload => #{
                id => ID,
                round => Round,
                reduce => Reduce,
                pairs => Pairs,
                finished => false,
                options => Options
            }
        }}),
        start_round(Parent, ID, Round, Pairs, Reduce, Options)
    end),
    {ok, Pid};

% @pre -
% @post -
start_link(ID, [Round, Pairs, Reduce, Options]) ->
    Parent = self(),
    Pid = erlang:spawn(fun() ->
        start_round(Parent, ID, Round, Pairs, Reduce, Options)
    end),
    {ok, Pid}.

% Map phase:

% @pre -
% @post -
map_phase(Entries) ->
    lists:flatmap(fun({IVar, Map}) ->
        Values = achlys_util:query(IVar),
        lists:flatmap(fun(Value) ->
            erlang:apply(Map, [Value])
        end, Values)
    end, Entries).

% Reduce phase:

% @pre -
% @post -
broadcast(Module, Message) ->
    % TODO: Broadcast with plumtree
    % Neighbors = achlys_util:get_neighbors(),
    {ok, Neighbors} = achlys:members(),
    lists:foreach(fun(Node) ->
        io:format("Sending solution to ~p~n", [Node]),
        partisan_peer_service:cast_message(Node, Module, Message)
    end, Neighbors).

% @pre -
% @post -
is_irreductible(InputPairs, OutputPairs) ->
    lists:sort(InputPairs) == lists:sort(OutputPairs).

% @pre -
% @post -
start_round(Parent, ID, Round, InputPairs, Reduce, Options) ->
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
                            OVar = maps:get(variable, Options),
                            bind_output_var(OVar, OutputPairs);
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
% @post - This function should return a list of tuples:
%       [
%           {ID, [{Key 1, Value 1}, {Key 2, Value 2}]},
%           {ID, [{Key 3, Value 3}]}
%       ]
get_batches(InputPairs, Options) ->
    Groups = lists:foldl(fun(Pair, Orddict) ->
        case Pair of {Key, _} ->
            orddict:append(Key, Pair, Orddict)
        end
    end, orddict:new(), InputPairs),
    Fun = fun({_, Pairs}) ->
        Name = erlang:unique_integer([monotonic, positive]),
        {Name, Pairs}
    end,
    lists:map(Fun, orddict:to_list(Groups)).

% @pre -
% @post -
group_pairs_per_key(Pairs) ->
    Groups = lists:foldl(fun(Pair, Orddict) ->
        case Pair of {Key, Value} ->
            orddict:append(Key, Value, Orddict)
        end
    end, orddict:new(), Pairs),
    orddict:to_list(Groups).

% @pre -
% @post -
reduce(InputPairs, Reduce) ->
    Groups = group_pairs_per_key(InputPairs),
    lists:flatmap(fun({Key, Pairs}) ->
        erlang:apply(Reduce, [Key, Pairs])
    end, Groups).

% @pre -
% @post -
get_tasks(Batches, Reduce) ->
    lists:foldl(fun({Name, Pairs}, Acc) ->
        [{fun() ->
            % TODO: Implements achlys_spawn
            % This module will have the responsibility to execute
            % the given function or send it to another node. The
            % module should retransmit the task to another node if
            % no response is given.
            timer:sleep(3000),
            reduce(Pairs, Reduce)
        end, []}|Acc]
    end, [], Batches).

% @pre -
% @post -
get_reduction(InputPairs, Reduce, Options) ->
    Batches = get_batches(InputPairs, Options),
    Tasks = get_tasks(Batches, Reduce),
    case promise:all(Tasks) of
        {ok, Results} ->
            L = orddict:to_list(Results),
            OutputPairs = lists:flatmap(fun({_, Pair}) -> Pair end, L),
            {ok, OutputPairs};
        max_attempts_reached ->
            {error, max_attempts_reached}
    end.

% @pre -
% @post -
bind_output_var(Var, Pairs) ->
    N = erlang:length(Pairs),
    lasp:bind(Var, {state_gset, lists:zip(
        lists:seq(1, N),
        lists:sort(Pairs)
    )}),
    io:format("Pairs=~p~n", [Pairs]).

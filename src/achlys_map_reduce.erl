-module(achlys_map_reduce).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-export([
    start_link/0,
    init/1,
    handle_cast/2,
    handle_call/3
]).

-export([
    map/3,
    reduce/2,
    schedule/0,
    get_size/0,
    print_state/0,
    debug/1
]).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, #{
        variables => []
    }}.

% Cast:

handle_cast({map, ID, Fun}, State) ->
    case State of #{variables := Variables} ->
        Variable = #{
            id => ID,
            function => Fun % Map function
        },
        {noreply, maps:put(
            variables,
            [Variable|Variables],
            State
        )}
    end;

handle_cast({reduce, Fun}, State) ->
    {noreply, maps:put(
        function,
        Fun, % Reduce function
        State
    )};

handle_cast(schedule, State) ->
    case State of #{variables := Variables, function := Reduce} ->

        Total = map_phase(Variables),
        lasp:read({
            <<"pairs">>,
            state_twopset
        }, {cardinality, Total}),

        % debug("pairs"),
        % io:format("Size=~p~n", [get_size()]),
        
        Round = 1,
        Count = shuffle_phase(Round),
        Keys = maps:keys(Count),

        lists:foreach(fun(Key) ->
            lasp:read({
                get_input_var(Round, Key),
                state_twopset
            }, {
                cardinality,
                maps:get(Key, Count)
            }),
            N = reduce_phase(Round, Key, Reduce),
            io:format("~p~n", [N])
        end, Keys),

        % Task = achlys:declare(mytask, all, single, fun() ->
        % end),
        % achlys:bite(Task),
        {noreply, State}
    end;

handle_cast(debug, State) ->
    {noreply, State};

handle_cast(_, State) ->
    {noreply, State}.

% Call:

handle_call(Request, _From, State) ->
    {reply, ok, State}.

% Helpers:

get_input_var(Round, Key) ->
    erlang:list_to_binary(
        erlang:integer_to_list(Round) ++ "-" ++ erlang:atom_to_list(Key)
    ).

get_output_var(Round) ->
    <<"ok">>.

% Map phase :

add_pair(Pair, ID) ->
    case Pair of {Key, Value} ->
        Name = <<"pairs">>,
        lasp:update({Name, state_twopset}, {add, #{
            id => ID,
            key => Key,
            value => Value
        }}, self())
    end.

map_phase(Variables) ->
    lists:foldl(fun(Variable, Acc1) ->
        case Variable of #{id := ID, function := Map} ->
            {ok, Set} = lasp:query(ID),
            Values = sets:to_list(Set),
            lists:foldl(fun(Value, Acc2) ->
                try
                    Pairs = erlang:apply(Map, [Value]),
                    lists:foldl(fun(Pair, Acc3) ->
                        add_pair(Pair, Acc3),
                        Acc3 + 1
                    end, Acc2, Pairs)
                catch _ -> Acc2 end
            end, Acc1, Values)
        end
    end, 1, Variables) - 1.

% Shuffle phase :

shuffle_phase(Round) ->
    {ok, Set} = lasp:query({<<"pairs">>, state_twopset}),
    lists:foldl(fun(Pair, Acc) ->
        case Pair of #{key := Key} ->
            Name = get_input_var(Round, Key), 
            ID = {Name, state_twopset},
            lasp:update(ID, {add, Pair}, self()),
            maps:put(Key, maps:get(Key, Acc, 0) + 1, Acc)
        end
    end, #{}, sets:to_list(Set)).

% Reduce phase :

extract_values([]) -> [];
extract_values([Pair|Pairs]) ->
    case Pair of #{value := Value} ->
        [Value|extract_values(Pairs)]
    end.

get_max_id([]) -> none;
get_max_id([Pair|Pairs]) ->
    case Pair of #{id := ID} ->
        get_max_id(Pairs, ID)
    end.
get_max_id([], Max) -> Max + 1;
get_max_id([Pair|Pairs], Max) ->
    case Pair of #{id := ID} ->
        case ID > Max of
            true -> get_max_id(Pairs, ID);
            false -> get_max_id(Pairs, Max)
        end
    end.

get_reduced_pairs(Reduce, Args) ->
    Pairs = erlang:apply(Reduce, Args),
    lists:sort(Pairs).

add_reduced_pairs(_, [], {_, Index}) -> Index;
add_reduced_pairs(Name, [Pair|Pairs], {Key, Index}) ->
    case Pair of
        {Key, Value} ->
            ID = {Name, state_twopset},
            lasp:update(ID, {add, #{
                id => {Key, Index},
                key => Key,
                value => Value
            }}, self()),
            add_reduced_pairs(Name, Pairs, {Key, Index + 1});
        _ ->
            add_reduced_pairs(Name, Pairs, {Key, Index})
    end.

reduce_phase(Round, Key, Reduce) ->

    {ok, Set} = lasp:query({
        get_input_var(Round, Key),
        state_twopset
    }),

    case sets:size(Set) of
        Size when Size == 0 -> 0;
        Size when Size > 0 ->
            Pairs = sets:to_list(Set),
            Values = extract_values(Pairs),
            A = get_max_id(Pairs) + 1,
            B = add_reduced_pairs(
                <<"coucou">>,
                get_reduced_pairs(Reduce, [Key, Values]),
                {Key, A}
            ),
            B - A
    end.

% Debugging:

get_size() ->
    {ok, Set} = lasp:query({<<"pairs">>, state_twopset}),
    sets:size(Set).

debug(Name) ->
    % io:format("~p~n", [erlang:list_to_binary(Name)]).
    {ok, Set} = lasp:query({
        erlang:list_to_binary(Name),
        state_twopset
    }),
    Content = sets:to_list(Set),
    io:format("Content: ~p~n", [Content]).

% API:

map(Name, Variable, Map) ->
    gen_server:cast(?SERVER, {map, Variable, Map}).

reduce(Name, Reduce) ->
    gen_server:cast(?SERVER, {reduce, Reduce}).

schedule() ->
    gen_server:cast(?SERVER, schedule).

print_state() ->
    gen_server:cast(?SERVER, debug).

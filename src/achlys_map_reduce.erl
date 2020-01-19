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
    debug/0
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
        Pairs = map_phase(Variables),
        N = shuffle_phase(Pairs),
        reduce_phase(N, Reduce),
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

% Map phase :

map_phase(Variables) -> map_phase(Variables, []).
map_phase([], Pairs) -> Pairs;
map_phase([Variable|Variables], Pairs) ->
    case Variable of #{id := ID, function := Map} ->
        {ok, Set} = lasp:query(ID),
        List = sets:to_list(Set),
        map_phase(Variables, lists:foldl(fun(Elem, Acc) ->
            Pair = erlang:apply(Map, [Elem]),
            Pair ++ Acc
        end, Pairs, List))
    end.

% Shuffle phase :

shuffle_phase(Pairs) -> shuffle_phase(Pairs, 0).
shuffle_phase([], K) -> K;
shuffle_phase([Pair|Pairs], K) ->
    case Pair of
        {Key, Value} ->
            ID = {<<"pairs">>, state_twopset},
            lasp:update(ID, {add, #{
                id => K,
                key => Key,
                value => Value
            }}, self()),
            shuffle_phase(Pairs, K + 1);
        _ -> shuffle_phase(Pairs, K)
    end.

% Reduce phase :

get_pairs() ->
    {ok, Set} = lasp:query({<<"pairs">>, state_twopset}),
    List = sets:to_list(Set),
    lists:foldl(fun(Pair, Acc) ->
        case Pair of #{id := ID, key := Key, value := Value} ->
            maps:put(ID, #{
                key => Key,
                value => Value
            }, Acc)
        end
    end, #{}, List).

get_constraints(K) ->
    get_constraints(lists:seq(0, K - 1), K).
get_constraints(L, K) ->
    case L of
        [H1|[H2|T]] ->
            Constraint = {{H1, H2}, K},
            [Constraint|get_constraints(
                T ++ [K],
                K + 1
            )];
        [_|[]] -> [];
        [] -> []
    end.

reduction_loop(Constraints, Reduce) ->
    timer:sleep(rand:uniform(100)), % Simulate delay
    Pairs = get_pairs(),
    case maps:size(Pairs) of
        Size when Size =< 1 ->
            debug(),
            ok;
        Size when Size > 1 ->
            reduction_step(Constraints, Pairs, Reduce),
            reduction_loop(Constraints, Reduce)
    end.

reduction_step(Constraints, Pairs, Reduce) ->
    ID = {<<"pairs">>, state_twopset},
    case Constraints of
        [{{A, B}, C}|T] ->
            case {
                maps:is_key(A, Pairs),
                maps:is_key(B, Pairs)
            } of {true, true} ->
                P1 = maps:get(A, Pairs),
                P2 = maps:get(B, Pairs),
                {Key, Value} = erlang:apply(Reduce, [P1, P2]),
                lasp:update(ID, {add, #{
                    id => C,
                    key => Key,
                    value => Value
                }}, self()),
                lasp:update(ID, {rmv, maps:put(id, A, P1)}, self()),
                lasp:update(ID, {rmv, maps:put(id, B, P2)}, self()),
                io:format("Node left: ~p~n", [get_size()]);
            _ -> reduction_step(T, Pairs, Reduce) end;
        [] -> ok
    end.

reduce_phase(N, Reduce) ->
    Task = achlys:declare(mytask, all, single, fun() ->
        io:format("Waiting convergence...~n"),
        ID = {<<"pairs">>, state_twopset},
        lasp:read(ID, {cardinality, N}),
        debug(),
        io:format("Size=~p~n", [get_size()]),
        Constraints = get_constraints(N),
        io:format("Constraints: ~p~n", [Constraints]),
        io:format("Starting the task...~n"),
        reduction_loop(Constraints, Reduce)
    end),
    achlys:bite(Task).

% Debugging:

get_size() ->
    {ok, Set} = lasp:query({<<"pairs">>, state_twopset}),
    sets:size(Set).

debug() ->
    {ok, Set} = lasp:query({<<"pairs">>, state_twopset}),
    Pairs = sets:to_list(Set),
    io:format("Pairs: ~p~n", [Pairs]).

% API:

map(Name, Variable, Map) ->
    gen_server:cast(?SERVER, {map, Variable, Map}).

reduce(Name, Reduce) ->
    gen_server:cast(?SERVER, {reduce, Reduce}).

schedule() ->
    gen_server:cast(?SERVER, schedule).

print_state() ->
    gen_server:cast(?SERVER, debug).

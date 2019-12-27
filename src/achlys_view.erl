-module(achlys_view).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-export([
    start_link/0,
    init/1,
    handle_cast/2,
    handle_call/3
]).

% API:

-export([
    map/3,
    reduce/2,
    add_listener/2,
    debug/0
]).

% Starting function:

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, #{
        pairs => [],
        listeners => [],
        variables => #{}
    }}.

% Cast:

handle_cast({map, ID, Fun}, State) ->
    {Name, _} = ID,
    Key = erlang:binary_to_list(Name),
    case State of #{variables := Variables} ->
        lasp:stream(ID, fun(Values) ->
            gen_server:cast(?SERVER, {on_change, ID, Values})
        end),
        {noreply, State#{
            variables := maps:put(Key, #{
                last_values => [],
                map => Fun
            }, Variables)
        }}
    end;

handle_cast({emit, Key, Value}, State) ->
    io:format("key=~p value=~p~n", [Key, Value]),
    {noreply, State};

handle_cast({reduce, Fun}, State) ->
    case State of #{pairs := Pairs, listeners := Listeners} ->
        Groups = shuffle_phase(Pairs),
        Results = maps:fold(fun(Key, Values, Acc) ->
            maps:put(Key, reduce_phase(Values, Fun), Acc)
        end, #{}, Groups),
        lists:foreach(fun(Listener) ->
            Listener(Results)
        end, Listeners)
    end,
    {noreply, State};

handle_cast({add_listener, Listener}, State) ->
    case State of #{listeners := Listeners} ->
        {noreply, State#{
            listeners := Listeners ++ [Listener]
        }}
    end;

handle_cast({on_change, ID, Values}, State) ->
    case State of #{variables := Variables} ->
        {Name, Type} = ID,
        Key = erlang:binary_to_list(Name),
        Variable = maps:get(Key, Variables),
        New_values = lists:sort(sets:to_list(Values)),
        case Variable of #{last_values := Last_values, map := Fun} ->
            A = {Type, New_values},
            B = {state, {Type, Last_values}},
            {_, Delta} = lasp_type:delta(Type, A, B),
            map_phase(Delta, Fun),
            {noreply, State#{
                variables := maps:put(Key, Variable#{
                    last_values := New_values
                }, Variables)
            }}
        end
    end;

handle_cast(debug, State) ->
    io:format("~p~n", ["Debugging"]),
    case State of #{last_values := Hashmap} ->
        io:format("~p~n", [Hashmap]),
        {noreply, State}
    end.

% Call:

handle_call(_Request , _From , State) ->
    {reply, ok, State}.

% Helpers:

shuffle_phase(L) -> shuffle_phase(#{}, L).
shuffle_phase(Groups, []) -> Groups;
shuffle_phase(Groups, [{Key, Value}|T]) ->
    case maps:is_key(Key, Groups) of
        true ->
            L = [Value|maps:get(Key, Groups)],
            M = maps:put(Key, L, Groups),
            shuffle_phase(M, T);
        false ->
            L = [Value],
            M = maps:put(Key, L, Groups),
            shuffle_phase(M, T)
    end.

map_phase([], _) -> ok;
map_phase([H|T], Fun) ->
    Fun(H, fun(Key, Value) -> % Emit function
        gen_server:cast(?SERVER, {emit, Key, Value})
    end),
    map_phase(T, Fun).

reduce_phase([H], _) -> H;
reduce_phase([H1|[H2|T]], Fun) ->
    reduce_phase([Fun(H1, H2)|T], Fun).

% API:

map(Name, Variable, Map) ->
    gen_server:cast(?SERVER, {map, Variable, Map}).

reduce(Name, Reduce) ->
    gen_server:cast(?SERVER, {reduce, Reduce}).

add_listener(Name, Listener) ->
    gen_server:cast(?SERVER, {add_listener, Listener}).

debug() ->
    gen_server:cast(?SERVER, debug).
-module(achlys_view).
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
    add_listener/2,
    debug/0
]).

% Starting function:

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, #{
        listeners => [],
        variables => #{},
        tree => #{
            mapping => #{},
            roots => #{},
            nodes => #{}
        }
    }}.

% Cast:

handle_cast({map, ID, Fun}, State) ->
    case State of #{variables := Variables} ->
        lasp:stream(ID, fun(Values) ->
            gen_server:cast(?SERVER, {on_change, ID, Values})
        end),
        {Name, _} = ID,
        Key = erlang:binary_to_list(Name),
        {noreply, mapz:deep_put(
            [variables, Key],
            #{last_values => [], map => Fun},
            State
        )}
    end;

handle_cast({on_change, ID, Values}, State) ->
    case maps:is_key(reduce, State) of
        true ->
            case State of #{variables := Variables} ->
                {Name, Type} = ID,
                Key = erlang:binary_to_list(Name),
                Variable = maps:get(Key, Variables),
                case Variable of #{last_values := Last_values, map := Fun} ->
                    % Delta = sets:subtract(Values, Last_values),
                    % io:format("~p~n", [Delta]),
                    % map_phase(Delta, Fun),
                    map_phase([], Fun),
                    notify(),
                    {noreply, mapz:deep_put(
                        [variables, Key, last_values],
                        Values,
                        State
                    )}
                end
            end;
        false -> {noreply, State}
    end;

handle_cast({emit, Key, Value}, State) ->
    % io:format("key=~p value=~p~n", [Key, Value]),
    case State of #{tree := Tree, reduce := Reduce} ->
        Updated_Tree = achlys_ct:add(Tree, {Key, Value}, Reduce),
        {noreply, maps:update(
            tree,
            Updated_Tree,
            State
        )}
    end;

handle_cast({reduce, Fun}, State) ->
    {noreply, maps:put(reduce, Fun, State)};

handle_cast({add_listener, Listener}, State) ->
    case State of #{listeners := Listeners} ->
        {noreply, maps:update(
            listeners,
            [Listener|Listeners],
            State
        )}
    end;

handle_cast(notify, State) ->
    % case State of #{tree := Tree, listeners := Listeners} ->
    %     lists:foreach(fun(Listener) ->
    %         erlang:apply(Listener, achlys_ct:get_all(Tree))
    %     end, Listeners),
    % end;
    {noreply, State};

handle_cast(debug, State) ->
    io:format("State= ~p~n", [State]),
    {noreply, State}.

% Call:

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

% Helpers:

map_phase([], _) -> ok;
map_phase([H|T], Fun) ->
    Fun(H, fun(Key, Value) -> % Emit function
        gen_server:cast(?SERVER, {emit, Key, Value})
    end),
    map_phase(T, Fun).

notify() ->
    gen_server:cast(?SERVER, notify).

% API:

map(Name, Variable, Map) ->
    gen_server:cast(?SERVER, {map, Variable, Map}).

reduce(Name, Reduce) ->
    gen_server:cast(?SERVER, {reduce, Reduce}).

add_listener(Name, Listener) ->
    gen_server:cast(?SERVER, {add_listener, Listener}).

debug() ->
    gen_server:cast(?SERVER, debug).
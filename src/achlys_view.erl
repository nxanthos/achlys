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
    add_variable/2,
    set_reduce/1,
    add_listener/1,
    debug/0
]).

% Starting function:

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, #{
        pairs => [],
        listeners => []
    }}.

% Cast:

handle_cast({add_variable, Variable, Map}, State) ->
    % lasp:stream(Variable, build_listener(Variable)),
    % io:format("~p~n", [PID]),
    case State of #{pairs := Pairs} ->
        {noreply, State#{
            pairs := Pairs ++ Map(Variable)
        }}
    end;
handle_cast({reduce, Reduce}, State) ->
    case State of #{pairs := Pairs, listeners := Listeners} ->
        Groups = shuffle(#{}, Pairs),
        Results = maps:fold(fun(Key, Values, Acc) ->
            maps:put(Key, reduce(Values, Reduce), Acc)
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
handle_cast(debug, State) ->
    io:format("~p~n", ["Debugging"]),
    case State of #{pairs := Pairs} ->
        io:format("~p~n", [Pairs]),
        {noreply, State}
    end.

% Call:

handle_call(_Request , _From , State) ->
    {reply, ok, State}.

% Helpers:

build_listener(Variable) ->
    fun(Value) ->
        { Name, _ } = Variable,
        io:format("Name of the variable that changed ~p~n", [Name]),
        io:format("Value of the variable~p~n", [Value])
    end.

shuffle(Groups, []) -> Groups;
shuffle(Groups, [{Key, Value}|T]) ->
    case maps:is_key(Key, Groups) of
        true ->
            L = [Value|maps:get(Key, Groups)],
            M = maps:put(Key, L, Groups),
            shuffle(M, T);
        false ->
            L = [Value],
            M = maps:put(Key, L, Groups),
            shuffle(M, T)
    end.

reduce([H], _) -> H;
reduce([H1|[H2|T]], Reduce) ->
    reduce([Reduce(H1, H2)|T], Reduce).

% API:

add_variable(Variable, Map) ->
    gen_server:cast(?SERVER , {add_variable, Variable, Map}).

set_reduce(Reduce) ->
    gen_server:cast(?SERVER , {reduce, Reduce}).

add_listener(Listener) ->
    gen_server:cast(?SERVER , {add_listener, Listener}).

debug() ->
    gen_server:cast(?SERVER, debug).
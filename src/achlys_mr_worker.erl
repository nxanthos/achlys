-module(achlys_mr_worker).
-behavior(gen_server).
-export([
    handle_cast/2,
    handle_call/3,
    init/1
]).
-export([
    start_link/0
]).

% @pre -
% @post -
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% @pre -
% @post -
init([]) ->
    {ok, #{}}.

% @pre -
% @post -
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

% @pre -
% @post -
handle_cast({reduce, Message}, State) ->
    case Message of #{
        header := #{
            src := Node
        },
        payload := #{
            id := ID,
            batch := Name,
            pairs := Pairs,
            reduce := Reduce
        }
    } ->
        Groups = group_pairs_per_key(Pairs),
        partisan_peer_service:cast_message(
            Node,
            achlys_mr,
            {on_reduce, #{
                header => #{
                    src => achlys_util:myself()
                },
                payload => #{
                    id => ID,
                    batch => Name,
                    pairs => reduce(Groups, Reduce)
                }
            }}
        )
    end,
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

% @pre -
% @post -
reduce(Groups, Reduce) ->
    lists:flatmap(fun({Key, Pairs}) ->
        erlang:apply(Reduce, [Key, Pairs])
    end, Groups).

% @pre -
% @post -
group_pairs_per_key(Pairs) ->
    Groups = lists:foldl(fun(Pair, Orddict) ->
        io:format("Pair=~p~n", [Pair]),
        case Pair of {Key, Value} ->
            orddict:append(Key, Value, Orddict)
        end
    end, orddict:new(), Pairs),
    orddict:to_list(Groups).
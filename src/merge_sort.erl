-module(merge_sort).
-export([
    debug/0,
    schedule/0
]).

% Helpers :

% @pre -
% @post -
shuffle(L) ->
    lists:sort(fun(_, _) ->
        rand:uniform() > 0.5
    end, L).

% @pre -
% @post -
insert([], E) -> [E];
insert([H|T], E) when E < H -> [E|[H|T]];
insert([H|T], E) when E >= H -> [H|insert(T, E)].

% @pre -
% @post -
get_association([], _) -> none;
get_association([_|[]], _) -> none;
get_association([H1|[H2|T]], N) when H2 - H1 > 1 ->
    get_association([H2|T], N);
get_association([H1|[H2|T]], N) when H2 - H1 == 1 ->
    case ((H1 - 1) rem 2) == 0 of
        false -> get_association([H2|T], N);
        true -> {{H1, H2}, H1 + N - erlang:trunc(H1 / 2)}
    end.

% @pre -
% @post -
get_node([], _) -> none;
get_node([Node|Nodes], I) ->
    case Node of #{id := J} ->
        case I == J of
            true -> Node;
            false -> get_node(Nodes, I)
        end
    end.

% @pre -
% @post -
merge([], []) -> [];
merge([], L2) -> L2;
merge(L1, []) -> L1;
merge([H1|T1], [H2|T2]) when H1 > H2 ->
    [H2|merge([H1|T1], T2)];
merge([H1|T1], [H2|T2]) when H1 =< H2 ->
    [H1|merge(T1, [H2|T2])].

% @pre -
% @post -
event_listener({reduce, Pred, Node}, State) ->
    case State of #{ids := IDS, nodes := Nodes} ->
        case Node of #{id := ID} ->
            L = erlang:tuple_to_list(Pred),
            maps:merge(State, #{
                ids => insert(lists:filter(fun(K) ->
                    not lists:member(K, L)
                end, IDS), ID),
                nodes => [Node|Nodes]
            })
        end
    end;
event_listener(_, State) -> State.

% @pre -
% @post -
update(State, Emit) ->
    case State of #{ids := IDs, n := N, nodes := Nodes} ->
        case get_association(IDs, N) of
            {Pred, Succ} ->
                {ID1, ID2} = Pred,
                case {
                    get_node(Nodes, ID1),
                    get_node(Nodes, ID2)
                } of {
                    #{values := V1},
                    #{values := V2}
                } ->
                    Emit({reduce, Pred, #{
                        id => Succ,
                        values => merge(V1, V2)
                    }})
                end;
            _ -> ok
        end
    end.

% @pre -
% @post -
loop() ->
    case achlys_es:read() of
        #{ids := [_|[_|_]]} -> % At least 2 values
            achlys_es:update(fun update/2),
            timer:sleep(100),
            loop();
        #{ids := _} -> ok
    end.

% @pre -
% @post -
schedule() ->
    N = 30,
    List = lists:seq(1, N),
    ID = {<<"events">>, state_gset},
    State = #{
        n => N,
        ids => List,
        nodes => lists:map(fun(K) ->
            #{id => K, values => [K]}
        end, shuffle(List))
    },
    io:format("~w~n", [State]),
    Task = achlys:declare(mytask, all, single, fun() ->
        io:format("Execution of the task~n"),
        achlys_es:start_link(ID, State,
            fun event_listener/2
        ),
        loop(),
        case achlys_es:read() of #{ids := [K], nodes := Nodes} ->
            case get_node(Nodes, K) of #{values := Values} ->
                io:format("Solution: ~p~n", [Values])
            end
        end
    end),
    achlys:bite(Task).

% @pre -
% @post -
debug() ->
    ID = {<<"events">>, state_gset},
    {ok, Set} = lasp:query(ID),
    Content = sets:to_list(Set),
    io:format("Variable: ~p~n", [Content]),
    io:format("State: ~p~n",[achlys_es:read()]).
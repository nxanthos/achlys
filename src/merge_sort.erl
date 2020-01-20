-module(merge_sort).

-behaviour(gen_server).

-export([start_link/0]).
-export([
    start_achlys_map_reduce/0
]).
-export([debug/0]).
-export([
    init/1 ,
    handle_call/3 ,
    handle_cast/2 ,
    handle_info/2 ,
    terminate/2 ,
    code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {}).

start_link() ->
    gen_server:start_link({local , ?SERVER} , ?MODULE , [] , []).

init([]) ->
    {ok , #state{}}.

handle_call(_Request, _From , State) ->
    {reply , ok , State}.

handle_cast(_Request, State) ->
    {noreply , State}.

handle_info(_Info, State) ->
    case _Info of
        {task, Task} -> 
            io:format("Starting task ~n", []),
            achlys:bite(Task)
    end,
    {noreply , State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok , State}.

% Helpers :

start_achlys_map_reduce() ->
    
    N = 10,
    ID = {<<"set">>, state_gset},
    lists:foreach(fun(K) ->
        lasp:update(ID, {add, K}, self())
    end, lists:seq(1, N)),
    lasp:read(ID, {cardinality, N}),

    achlys_map_reduce:start_link(),

    % Map function
    achlys_map_reduce:map("get-samples", ID, fun(Value) ->
        [{sort, Value}]
    end),

    % Reduce function
    achlys_map_reduce:reduce("get-samples", fun(P1, P2) ->
        io:format("P1: ~p~n", [P1]),
        io:format("P2: ~p~n", [P2]),
        case {P1, P2} of {#{value := V1}, #{value := V2}} ->
            {sort, msort(V1, V2)}
        end
    end),

    achlys_map_reduce:schedule().


debug() ->
    Type = state_gset,
    Set = achlys_util:query({<<"set">>, Type}),
    io:format("Set: ~p~n", [Set]).



msort([L]) -> [L];
msort(L)   ->
    {L1,L2} = lists:split(length(L) div 2, L),
    msort(msort(L1), msort(L2)).

msort(L1, L2) -> 
    case {L1, L2} of 
        {[H1|T1], [H2|T2]} ->
            msort(L1, L2, []);
        {H1, [H2|T2]} ->
            msort([L1], L2, []);
        {[H1|T1], H2} ->
            msort(L1, [L2], []);
        _ -> msort([L1], [L2], [])
    end.
    
msort([], L2, A) -> A++L2;
msort(L1, [], A) -> A++L1;
msort([H1|T1], [H2|T2], A) when H2>=H1 -> msort(T1, [H2|T2], A++[H1]);
msort([H1|T1], [H2|T2], A) when H1>H2  -> msort([H1|T1], T2, A++[H2]);
msort([], [], A) -> A.
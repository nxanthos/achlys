-module(path_with_counters).
-export([
    schedule/0  
]).

% Network configuration:

get_links() ->
    % Format:
    % [
    %   {Destination, Source, Cost},
    %   {Destination, Source, Cost},
    %   ...,
    % ]
    [
        {a, a, 0},
        {b, a, 3},
        {b, c, 4},
        {b, d, 2},
        {c, b, 4},
        {c, d, 5},
        {c, e, 6},
        {d, a, 7},
        {d, b, 2},
        {d, c, 5},
        {d, e, 4},
        {e, c, 6},
        {e, d, 4}
    ].


get_predecessors() ->
    % Format: 
    % [
    %   {Destination, [Source 1, Source 2, ...]},
    %   {Destination, [Source 1, Source 2, ...]},
    %   ...
    % ]
    Orddict = lists:foldl(fun({Dst, Src, _}, Acc) ->
        orddict:append(Dst, Src, Acc)
    end, orddict:new(), get_links()),
    orddict:to_list(Orddict).



% Helpers :

get_actor() -> 
    {
        partisan_remote_reference,
        partisan_peer_service_manager:mynode(),
        partisan_util:gensym(self())
    }.

args_to_list(Fun, 2) -> 
    fun(A, B) ->
        {_, _, _, V1} = A,
        {_, _, _, C1} = B,
        Fun([{V1, C1}])
    end;
args_to_list(Fun, 4) -> 
    fun(A, B, C, D) ->
        {_, _, _, I1} = A,
        {_, _, _, I2} = B,
        {_, _, _, C1} = C,
        {_, _, _, C2} = D,
        Fun([{I1, C1}, {I2, C2}])
    end;
args_to_list(Fun, 6) -> 
    fun(A, B, C, D, E, F) ->
        {_, _, _, I1} = A,
        {_, _, _, I2} = B,
        {_, _, _, I3} = C,
        {_, _, _, C1} = D,
        {_, _, _, C2} = E,
        {_, _, _, C3} = F,
        Fun([{I1, C1}, {I2, C2}, {I3, C3}])
    end;
args_to_list(Fun, 8) -> 
    fun(A, B, C, D, E, F, G, H) ->
        {_, _, _, I1} = A,
        {_, _, _, I2} = B,
        {_, _, _, I3} = C,
        {_, _, _, I4} = D,
        {_, _, _, C1} = E,
        {_, _, _, C2} = F,
        {_, _, _, C3} = G,
        {_, _, _, C4} = H,
        Fun([{I1, C1}, {I2, C2}, {I3, C3}, {I4, C4}])
    end.

gen_read_funs(Inputs, Costs) ->
    lists:map(fun(Var) ->
        {Var, fun(ID, Threshold) ->
            lasp:read(ID, Threshold)
        end}
    end, Inputs ++ Costs).

gen_tran_funs(N) ->
    args_to_list(fun(L) ->
        get_min(L)
    end, N).

gen_write_fun(Var) ->
    {Var, fun(ID, Value) ->
        case lasp:bind(ID, Value) of
            {error, not_found} ->
                io:format("Could not set the output variable (~p)!~n", [ID]);
            {ok, {_, _, _, PNCounter}} ->
                Count = state_pncounter:query(PNCounter),
                io:format("Var=~p N=~w~n", [ID, Count])
        end
    end}.

sum({state_pncounter, LValue}, {state_pncounter, RValue}) ->
    {state_pncounter, orddict:merge(
        fun(_, {Inc1, Dec1}, {Inc2, Dec2}) ->
            {Inc1 + Inc2, Dec1 + Dec2}
        end,
        LValue,
        RValue
    )}.

get_min(L) ->
    case L of [{I1, C1}|T] ->
        S1 = sum(I1, C1),
        N1 = state_pncounter:query(S1),
        {S3, _} = lists:foldl(fun({I2, C2}, {_, N3}=Acc) ->
            S2 = sum(I2, C2),
            N2 = state_pncounter:query(S2),
            case N2 of
                _ when N2 < N3 -> {S2, N2};
                _ -> Acc
            end
        end, {S1, N1}, T),
        S3
    end.

add_connection(Inputs, Costs, Destination) when length(Inputs) == length(Costs) ->
    N = length(Inputs) + length(Costs),
    ReadFuns = gen_read_funs(Inputs, Costs),
    TranFuns = gen_tran_funs(N),
    WriteFun = gen_write_fun(Destination),
    % io:format("ReadFuns=~p WriteFun=~p~n~n~n", [ReadFuns, WriteFun]),
    lasp_process:start_dag_link([
        ReadFuns,
        TranFuns,
        WriteFun
    ]).

get_node_id(Name, Level) ->
    erlang:list_to_binary(erlang:atom_to_list(Name) ++ erlang:integer_to_list(Level)).

get_cost_id(Dst, Src) ->
    erlang:list_to_binary(erlang:atom_to_list(Dst) ++ erlang:atom_to_list(Src)).

link_layer() ->

    Predecessors = get_predecessors(),
    Links = get_links(),

    Type = state_pncounter,
    Level = 1,

    % Initialize the input value:

    lists:foreach(fun({Name, _}) ->
        ID = {get_node_id(Name, Level), Type},
        lasp:declare(ID, Type),
        lasp:update(ID, increment, get_actor())
    end, Predecessors),

    % Initialize the cost :

    lists:foreach(fun({Dst, Src, Cost}) ->
        ID = {get_cost_id(Dst, Src), Type},
        lasp:declare(ID, Type),
        lasp:update(ID, {increment, Cost + 1}, get_actor())
    end, Links),

    % Add layers :

    N = erlang:length(Predecessors),
    lists:foreach(fun(K) ->
        lists:foreach(fun({Dst, Src}) ->
            add_connection(
                lists:map(fun(Name) -> % Inputs
                    {get_node_id(Name, K), Type}
                end, Src),
                lists:map(fun(Name) -> % Costs
                    {get_cost_id(Dst, Name), Type}
                end, Src),
                {get_node_id(Dst, K + 1), Type}
            )
        end, Predecessors)
    end, lists:seq(1, N - 1)),

    % Debug:

    timer:sleep(2000),

    % lists:foreach(fun(K) ->
    %     debug_layer(K)
    % end, lists:seq(1, N)),

    debug_layer(N),

    ok.

% ---

% Debug :

debug_layer(N) ->
    Type = state_pncounter,
    lists:foreach(fun({Name, _}) ->
        Identifier = get_node_id(Name, N),
        io:format("~p=~w~n", [Identifier, lasp:query({Identifier, Type})])
    end, get_predecessors()).

debug_cost() ->
    Type = state_pncounter,
    lists:foreach(fun({Dst, Src, _}) ->
        Identifier = get_cost_id(Dst, Src),
        io:format("~p=~w~n", [Identifier, lasp:query({Identifier, Type})])
    end, get_links()).

test_pncounter_read() ->
    
    Type = state_pncounter,
    ID = {<<"counter">>, Type},
    
    lasp:declare(ID, Type),
    
    erlang:spawn(fun() ->
        timer:sleep(500),
        lasp:update(ID, {increment, 1}, get_actor())
    end),
    
    {ok, {ID, Type, Metadata, Value}} = lasp:read(ID, {strict, state_pncounter:new()}),
    N = state_pncounter:query(Value),

    io:format("ID=~w~nType=~w~nMetadata=~w~nvalue=~w~n", [ID, Type, Metadata, Value]),
    io:format("N=~w~n", [N]),
    
    ok.

test_pncounter_dag() ->

    Type = state_pncounter,
    IVar = {<<"ivar">>, Type},
    CVar = {<<"cvar">>, Type},
    OVar = {<<"ovar">>, Type},

    lasp:declare(IVar, Type),
    lasp:declare(CVar, Type),
    lasp:declare(OVar, Type),

    add_connection([IVar], [CVar], OVar),

    lasp:stream(OVar, fun(Value) ->
        io:format("Ovar=~w~n", [Value])
    end),

    timer:sleep(100),
    lasp:update(IVar, increment, get_actor()),
    timer:sleep(100),
    lasp:update(CVar, increment, get_actor()),
    timer:sleep(100),
    lasp:update(CVar, increment, get_actor()),
    timer:sleep(500),

    io:format("IVar=~w~n", [lasp:query(IVar)]),
    io:format("CVar=~w~n", [lasp:query(CVar)]),
    io:format("OVar=~w~n", [lasp:query(OVar)]),
    ok.

test_counter_sum() ->

    Type = state_pncounter,
    A = {<<"a">>, Type},
    B = {<<"b">>, Type},
    C = {<<"c">>, Type},

    lasp:declare(A, Type),
    lasp:declare(B, Type),
    lasp:declare(C, Type),

    % Update:

    lasp:update(A, {increment, 1}, get_actor()),
    lasp:update(A, {decrement, 5}, get_actor()),
    lasp:update(B, {increment, 2}, get_actor()),
    lasp:update(B, {decrement, 2}, get_actor()),

    {ok, {_, _, _, LValue}} = lasp:read(A, {strict, undefined}),
    {ok, {_, _, _, RValue}} = lasp:read(B, {strict, undefined}),
    {ok, {_, _, _, Value}} = lasp:bind(C, sum(LValue, RValue)),

    N = state_pncounter:query(Value),
    io:format("N=~w~n", [N]),
    ok.

test_distributed_sum_counter() ->

    Type = state_pncounter,
    A = {<<"a">>, Type},
    B = {<<"b">>, Type},
    C = {<<"c">>, Type},

    lasp:declare(A, Type),
    lasp:declare(B, Type),
    lasp:declare(C, Type),

    lasp_process:start_dag_link([
        [
            {A, fun(ID, Threshold) ->
                lasp:read(ID, Threshold)
            end},
            {B, fun(ID, Threshold) ->
                lasp:read(ID, Threshold)
            end}
        ],
        fun(R1, R2) ->
            {_, _, _, LValue} = R1,
            {_, _, _, RValue} = R2,
            sum(LValue, RValue)
        end,
        {C, fun(ID, Value) -> % Write function
            lasp:bind(ID, Value)
        end}
    ]),

    lasp:stream(C, fun(Sum) ->
        io:format("OK~n"),
        io:format("Sum=~w~n", [Sum])    
    end),

    Task = achlys:declare(mytask, all, single, fun() ->
        io:format("Executing the task~n"),
        lasp:update(A, {increment, 1}, get_actor()),
        lasp:update(A, {decrement, 5}, get_actor()),
        lasp:update(B, {increment, 2}, get_actor()),
        lasp:update(B, {decrement, 2}, get_actor())
    end),

    achlys:bite(Task),
    ok.

schedule() ->
        
    % test_pncounter_read(),
    % test_pncounter_dag(),
    % test_counter_sum(),
    % test_distributed_sum_counter(),
    
    % io:format("~p~n", [get_node_id(a, 42)]),
    % io:format("~p~n", [get_cost_id(a, b)]),
    
    link_layer(),
    ok.

% path_with_counters:schedule().

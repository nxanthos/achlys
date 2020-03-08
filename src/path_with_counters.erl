-module(path_with_counters).
-export([
    schedule/0  
]).

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
    lasp_process:start_dag_link([
        gen_read_funs(Inputs, Costs),
        gen_tran_funs(N),
        {Destination, fun(ID, Value) ->
            case lasp:bind(ID, Value) of
                {error, not_found} ->
                    io:format("Could not set the output variable (~p)!~n", [ID]);
                {ok, {_, _, _, PNCounter}} ->
                    Count = state_pncounter:query(PNCounter),
                    io:format("Var=~p N=~w~n", [ID, Count])
            end,
            % io:format("Value=~w~n", [Value]),
            ok
        end}
    ]).

get_node_id(Name, Level) ->
    erlang:list_to_binary(erlang:atom_to_list(Name) ++ erlang:integer_to_list(Level)).

get_cost_id(A, B) ->
    case A < B of
        true ->
            erlang:list_to_binary(erlang:atom_to_list(A) ++ erlang:atom_to_list(B));
        false ->
            erlang:list_to_binary(erlang:atom_to_list(B) ++ erlang:atom_to_list(A))
    end.

link_layer() ->

    Links = [
        {a, [a]},
        {b, [a, c, d]},
        {c, [b, d, e]},
        {d, [a, b, c, e]},
        {e, [c, d]}
    ],

    Costs = [
        {a, a, 0},
        {a, b, 3},
        {a, d, 7},
        {b, d, 2},
        {b, c, 7},
        {c, d, 5},
        {c, e, 6},
        {d, e, 4}
    ],

    Type = state_pncounter,

    % Initialize the costs:

    Level = 1,

    % Initialize the value:

    lists:foreach(fun({Name, _}) ->
        ID = {get_node_id(Name, Level), Type},
        lasp:update(ID, increment, get_actor())
    end, Links),

    lists:foreach(fun({A, B, Cost}) ->
        ID = {get_cost_id(A, B), Type},
        lasp:update(ID, {increment, Cost}, get_actor())
    end, Costs),

    lists:foreach(fun({Dst, Src}) ->
        Destination = {get_node_id(Dst, 2), Type},
        add_connection(
            lists:map(fun(Name) ->
                {get_node_id(Name, Level), Type}
            end, Src),
            lists:map(fun(Name) ->
                {get_cost_id(Name, Dst), Type}
            end, Src),
            Destination
        ),
        ok
    end, Links),
    % debug(),
    ok.

debug() ->
    L = [a, b, c, d, e],
    Level = 1,
    lists:map(fun(Name) ->
        ID = {get_node_id(Name, Level), state_pncounter},
        {ok, N} = lasp:query(ID),
        io:format("Node=~w Level=~w Value=~w~n", [Name, Level, N])
    end, L).

% ---

% Debug :

test_pncounter_read() ->
    
    Type = state_pncounter,
    ID = {<<"counter">>, Type},
    
    lasp:declare(ID, Type),
    
    erlang:spawn(fun() ->
        timer:sleep(500),
        lasp:update(ID, increment, get_actor())
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

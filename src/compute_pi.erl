-module(compute_pi).

-behaviour(gen_server).

-export([start_link/0]).
-export([schedule_task/0]).
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

% Handler :

getDistance(Point) ->
    case Point of {X, Y} ->
        math:sqrt(X * X + Y * Y)
    end.

getRandomPoint() -> {
    rand:uniform(),
    rand:uniform()
}.

sampling(N) ->
    sampling(N, 0).
sampling(N, Acc) ->
    case N of
        _ when N > 0 ->
            case getDistance(getRandomPoint()) of
                Distance when Distance =< 1 -> sampling(N - 1, Acc + 1);
                _ -> sampling(N - 1, Acc)
            end;
        _ when N =< 0 -> Acc
    end.

add_sampling(Set) ->
    N = 5000,
    Success = sampling(N),
    lasp:update(Set, {add, #{
        n => N,
        success => Success
    }}, self()).

get_delta(Set_1) ->

    % {ID, Type, Metadata, {_, Value}} = Set_1,
    % io:format("=======================~n"),
    % io:format("ID:~p~n", [ID]),
    % io:format("Type:~p~n", [Type]),
    % io:format("Metadata:~p~n", [Metadata]),
    % io:format("Value:~p~n", [Value]),
    % io:format("=======================~n"),
    % io:format("Get delta:~n"),

    % Documentation:
    % see ctrl + p -> _build/test/lib/types/src/state_type.erl

    % Examples:
    % A = {state_gset, ["a", "b", "c"]},
    % B = {state, {state_gset, ["a", "b"]}},
    % Delta = state_gset:delta(A, B),
    % io:format("delta=~p~n", [Delta]).

    % Compare the states:
    % A = {state_gset, ["a", "b"]},
    % B = {state, {state_gset, Value}},
    % Delta = lasp_type:delta(Type, A, B),
    % io:format("delta=~p~n", [Delta]).

    {ID_1, _, _, _} = Set_1,
    {ok, Value_1} = lasp:query(ID_1),

    % We perform an update:

    {ok, Set_2} = lasp:update(ID_1, {add, #{
        n => 1,
        success => 1
    }}, self()),
    {ID_2, _, _, _} = Set_2,
    {ok, Value_2} = lasp:query(ID_2),

    io:format("=======================~n"),
    io:format("~p~n", [Set_1]),
    io:format("~p~n", [Set_2]),

    % We compare the results:

    Type = state_gset,
    A = {Type, sets:to_list(Value_2)},
    B = {state, {Type, sets:to_list(Value_1)}},
    Delta = lasp_type:delta(Type, A, B),

    io:format("delta=[~p]~n", [Delta]),
    io:format("=======================~n").

schedule_task() ->

    Task = achlys:declare(mytask, all, single, fun() ->

        % Declare variable :

        Type = state_gset,
        ID = {<<"set">>, Type},
        lasp:declare(ID, Type),
        % {ok, Set} = lasp:declare(ID, Type),
        % get_delta(Set),

        % Producer :

        spawn(fun() ->
            achlys_util:repeat(30, fun(_) ->
                timer:sleep(1000),
                add_sampling(ID)
            end)
        end),
        
        % Consumer :

        % Listener
        achlys_view:add_listener("get-samples", fun(Results) ->
            io:format("Reduce : ~p~n", [Results])
        end),

        % Map function
        achlys_view:map("get-samples", ID, fun(Value, Emit) ->
            case Value of #{n := _, success := _} ->
                Emit("sample", Value)
            end
        end),

        % Reduce function
        achlys_view:reduce("get-samples", fun(V1, V2) ->
            case V1 of #{n := N1, success := S1} ->
                case V2 of #{n := N2, success := S2} -> #{
                    n => N1 + N2,
                    success => S1 + S2
                }
                end
            end
        end)

    end),

    erlang:send_after(100, ?SERVER, {task, Task}),
    ok.

debug() ->
    Set = achlys_util:query({<<"set">>, state_gset}),
    io:format("Set: ~p~n", [Set]).
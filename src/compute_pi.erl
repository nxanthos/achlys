-module(compute_pi).

-behaviour(gen_server).

-export([start_link/0]).
-export([schedule_task/0]).
-export([start_achlys_stream_reducer/0]).
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
sampling(N, Acc) when N =< 0 -> Acc;
sampling(N, Acc) when N > 0 ->
    case getDistance(getRandomPoint()) of
        Distance when Distance =< 1 -> sampling(N - 1, Acc + 1);
        _ -> sampling(N - 1, Acc)
    end.

add_sampling(Set) ->
    N = 5000,
    Success = sampling(N),
    lasp:update(Set, {add, #{
        n => N,
        success => Success
    }}, self()).

schedule_task() ->

    Task = achlys:declare(mytask, all, single, fun() ->

        % Declare variable :

        Type = state_gset,
        ID = {<<"set">>, Type},
        lasp:declare(ID, Type),

        % Producer :

        spawn(fun() ->
            achlys_util:repeat(10, fun(_) ->
                timer:sleep(100),
                add_sampling(ID)
            end)
        end),

        % Consumer :

        % Listener
        achlys_view:add_listener("get-samples", fun(Results) ->
            io:format("Result : ~p~n", [Results])
        end),

        % Map function
        achlys_view:map("get-samples", ID, fun(Value, Emit) ->
            case Value of #{n := _, success := _} ->
                Emit("sample", Value)
            end
        end),

        % Reduce function
        achlys_view:reduce("get-samples", fun(V1, V2) ->
            case {V1, V2} of {
                #{n := N1, success := S1},
                #{n := N2, success := S2}
            } -> #{
                n => N1 + N2,
                success => S1 + S2
            } end
        end)
    end),

    erlang:send_after(100, ?SERVER, {task, Task}),
    ok.

start_achlys_stream_reducer() ->

    Type = state_gset,
    ID = {<<"set">>, Type},
    lasp:declare(ID, Type),

    % Producer :

    spawn(fun() ->
        achlys_util:repeat(30, fun(_) ->
            timer:sleep(1000),
            add_sampling(ID)
        end)
    end),
    
    % Consumer :

    Reducer = fun(V1, V2) ->
        case V1 of #{n := N1, success := S1} ->
            case V2 of #{n := N2, success := S2} -> #{
                n => N1 + N2,
                success => S1 + S2
            }
            end
        end
    end,

    Acc = #{ n => 0, success => 0 },

    Callback = fun(Result) ->
        io:format("Result: ~p~n", [Result]),
        case Result of #{n := N, success := Success} ->
            io:format("PI ≃ ~p~n", [4 * Success / N])
        end
    end,

    achlys_stream_reducer:reduce(
        ID,
        Reducer,
        Acc,
        Callback
    ).

debug() ->
    Type = state_gset,
    Set = achlys_util:query({<<"set">>, Type}),
    io:format("Set: ~p~n", [Set]).
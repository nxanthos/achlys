-module(naivemapreduce).

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

schedule_task() ->
    Task = achlys:declare(mytask, all, single, fun() ->

        % Declare variable :

        Type = state_gset,
        Set = {<<"set">>, Type},
        lasp:declare(Set, Type),

        % Sampling :

        add_sampling(Set),
        add_sampling(Set),
        add_sampling(Set),

        % Map reduce :

        achlys_view:start_link(),
        achlys_view:add_listener(fun(Results) ->
            io:format("Reduce : ~p~n", [Results])
        end),

        achlys_view:add_variable(Set, fun(Variable) -> % Map function
            {ok , S} = lasp:query(Variable),
            lists:map(fun(Sample) ->
                case Sample of #{n := _, success := _} -> % Filter
                    { "sample", Sample } % emit { Key, Value }
                end
            end, sets:to_list(S))
        end),

        achlys_view:set_reduce(fun(V1, V2) -> % Reduce function
            case V1 of #{n := N1, success := S1} ->
                case V2 of #{n := N2, success := S2} -> #{
                    n => N1 + N2,
                    success => S1 + S2
                }
                end
            end
        end),

        achlys_view:debug()
    end),

    erlang:send_after(100, ?SERVER, {task, Task}),
    ok.

debug() ->
    Set = achlys_util:query({<<"set">>, state_gset}),
    io:format("Set: ~p~n", [Set]).
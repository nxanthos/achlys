-module(linear_regression).

-define(SERVER, ?MODULE).
-define(TYPE, state_gset).
-define(SAMPLE_SIZE, 5).

-behaviour(gen_server).
-export([
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    start_link/1
]).

% linear_regression:start_link("data").
% linear_regression:get_params().
% linear_regression:predict(10).

% API:

-export([
    get_params/0,
    add_samples/2,
    predict/1
]).

start_link(Name) ->
    ID = {erlang:list_to_binary(Name), ?TYPE},
    gen_server:start_link({local, ?SERVER}, ?MODULE, [ID], []).

init([ID]) ->
    add_samples(ID, 1000),
    timer:send_after(100, self(), learn),
    {ok, #{
        id => ID,
        params => #{
            intercept => 0,
            slope => 0
        }
    }}.

% Call:

handle_call(get_params, _From, State) ->
    {reply, maps:get(params, State), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

% Cast:

handle_cast(_Request, State) ->
    {noreply, State}.

% Info:

handle_info(learn, State) ->
    timer:send_after(1000, self(), learn),
    ID = maps:get(id, State),
    S = maps:update_with(params, fun(Params) ->
        adjust_params(ID, Params, 0.01)
    end, State),
    io:format("New state =~p~n", [S]),
    {noreply, S}.

% API:

% @pre -
% @post -
get_params() ->
    gen_server:call(?SERVER, get_params).

% @pre -
% @post -
predict(X) ->
    case get_params() of #{
        intercept := Intercept,
        slope := Slope
    } -> X * Slope + Intercept end.

% Helpers:

% @pre -
% @post -
generate_point(Slope, Intercept) ->
    X = rand:uniform() * 10 - 5, % [-5, 5]
    Noise = rand:uniform() * 4 - 2, % [-2, 2]
    {X, (Slope * X + Intercept) + Noise}.

% @pre -
% @post -
add_samples(ID, N) ->
    lasp:bind(ID, {?TYPE, lists:foldl(fun(_, L) ->
        [generate_point(1, 5)|L]
    end, [], lists:seq(1, N))}).

% @pre -
% @post -
get_samples(ID) ->
    Values = achlys_util:query(ID),
    case erlang:length(Values) of
        Length when Length == 0 -> [];
        Length ->
            lists:map(fun(_) ->
                N = rand:uniform(Length),
                lists:nth(N, Values)
            end, lists:seq(1, ?SAMPLE_SIZE))
    end.

% @pre -
% @post -
adjust_params(ID, Params, Lambda) ->
    case get_samples(ID) of
        [] -> Params;
        Samples -> 
            case Params of #{
                intercept := Intercept,
                slope := Slope
            } ->
                L1 = lists:map(fun({X, Y}) -> Slope * X + Intercept - Y end, Samples),
                L2 = lists:map(fun({X, _}) -> X end, Samples),
                L3 = lists:map(fun({X, DY}) -> X * DY end, lists:zip(L2, L1)),
                #{
                    intercept => Intercept - 2 * Lambda * lists:sum(L1),
                    slope => Slope - 2 * Lambda * lists:sum(L3)
                }
            end
    end.

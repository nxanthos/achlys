-module(knn).
-export([
    predict/2,
    to_csv/0
]).

% @pre -
% @post -
choose_randomly(L) ->
    Length = erlang:length(L),
    Index = rand:uniform(Length),
    lists:nth(Index, L).

% @pre -
% @post -
get_random_point(Alpha, Beta) ->
    Angle = rand:uniform() * (2 * math:pi()), 
    Radius = rand:uniform(),
    X = Radius * math:cos(Angle),
    Y = Radius * math:sin(Angle),
    {
        X * math:cos(Alpha + Beta) + Y * math:cos(math:pi() / 2 - Alpha + Beta),
        X * math:sin(Alpha + Beta) + Y * math:sin(math:pi() / 2 - Alpha + Beta)
    }.

% @pre -
% @post -
produce_data(ID, N) ->
    Labels = [#{
        label => "a",
        alpha => 0.4, % Stretch
        beta => 0.5, % Rotation
        center => {10, 10},
        scale => {10, 10}
    }, #{
        label => "b",
        alpha => 0.4, % Stretch
        beta => 1.5, % Rotation
        center => {20, 10},
        scale => {8, 10}
    }],
    lists:foreach(fun(_) ->
        #{
            label := Label,
            alpha := Alpha,
            beta := Beta,
            center := {CX, CY},
            scale := {SX, SY}
        } = choose_randomly(Labels),
        {X, Y} = get_random_point(Alpha, Beta),
        lasp:update(ID, {add, {{
            CX + SX * X,
            CY + SY * Y
        }, Label}}, self())
    end, lists:seq(1, N)).

% @pre -
% @post -
to_csv() ->
    N = 1000,
    ID = {<<"data">>, state_gset},
    produce_data(ID, N),
    lasp:read(ID, {cardinality, N}),
    {ok, Set} = lasp:query(ID),
    Values = sets:to_list(Set),
    case file:open("utils/knn.csv", [write]) of
        {ok, Fd} ->
            file:write(Fd, io_lib:fwrite("~p;~p;~p\n", ["label", "x", "y"])),
            lists:foreach(fun(Value) ->
                {{X, Y}, Label} = Value,
                file:write(Fd, io_lib:fwrite("~p;~p;~p\n", [Label, X, Y]))
            end, Values),
            file:close(Fd);
        {error, _} ->
            io:format("Could not write or create the file")
    end.

% @pre -
% @post -
get_distance({X1, Y1}, {X2, Y2}) ->
    DX = X2 - X1,
    DY = Y2 - Y1,
    math:sqrt(DX * DX + DY * DY).

% @pre -
% @post -
add_to_ranking({X1, Y1, D1}, []) -> [{X1, Y1, D1}];
add_to_ranking({X1, Y1, D1}, [{X2, Y2, D2}|T]) when D1 >= D2 ->
    [{X1, Y1, D1}|[{X2, Y2, D2}|T]];
add_to_ranking({X1, Y1, D1}, [{X2, Y2, D2}|T]) when D1 < D2 ->
    [{X2, Y2, D2}|add_to_ranking({X1, Y1}, T)].

% @pre -
% @post -
predict(X, Y) ->
    % TODO
    ok.
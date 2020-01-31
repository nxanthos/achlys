-module(sql).
-export([
    select_test/0
]).

select_test() ->
    add_tables(),
    Rows = lasp_sql_materialized_view:get_value(table, [a, b]),
    io:format("Rows=~p~n", [Rows]),
    Query = "select b from table where a < 3",
    {ok, ID} = lasp_sql_materialized_view:create(Query),
    Result = lasp_sql_materialized_view:get_value(ID, [a, b]),
    io:format("ID=~p~n", [ID]),
    io:format("Result=~p~n", [Result]).

% ---------------------------------------------
% Data :
% ---------------------------------------------

% @pre -
% @post -
get_table() -> [
    [{b, "a"}, {a, 1}],
    [{b, "b"}, {a, 1}],
    [{b, "b"}, {a, 2}],
    [{b, "c"}, {a, 2}]
].

% @pre -
% @post -
add_tables() ->
    lasp_sql_materialized_view:create_table_with_values(table, get_table()),
    timer:sleep(100).
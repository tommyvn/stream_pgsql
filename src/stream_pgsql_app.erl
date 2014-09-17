-module(stream_pgsql_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
  ets:new(stream_pgsql_in_progress, [set, named_table, public]),
  {PGHost, PGUser, PGPassword, PGOptions} = case os:getenv("DATABASE_URL") of
    false ->
      {"localhost", "ttytube", "ttytube", [{database, "ttytube"}]};
    DatabaseUrl ->
      {uri,<<"postgres">>, UserPassword, Host, Port, DBName, _, _, _} = uri:from_string(DatabaseUrl),
      [User, Password] = string:tokens(binary_to_list(UserPassword), ":"),
%%       io:format("connecting to ~p on port ~p with username ~p and password ~p~n", [Host, Port, User, Password]),
      {binary_to_list(Host), User, Password, [{port, Port}, {database, tl(binary_to_list(DBName))}, {ssl, trye}]}
  end,
  {ok, PGConn} = pgsql:connect(PGHost, PGUser, PGPassword, PGOptions),
	stream_pgsql_sup:start_link(PGConn).

stop(_State) ->
	ok.
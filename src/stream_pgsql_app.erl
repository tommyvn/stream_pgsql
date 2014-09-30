-module(stream_pgsql_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
  ets:new(stream_pgsql_in_progress, [set, named_table, public]),
	stream_pgsql_sup:start_link().

stop(_State) ->
	ok.
-module(stream_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
  ets:new(stream_in_progress, [set, named_table, public]),
	stream_sup:start_link().

stop(_State) ->
	ok.

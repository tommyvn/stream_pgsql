-module(stream).
-export([open/2, delete/1, write/2, close/1, read/2]).
-export([start_all/0]).


open(Name, ModeList) when is_list(Name) orelse is_binary(Name), is_list(ModeList) ->
  stream_sup:start_child(Name, ModeList).

delete(Name) ->
  file:delete(Name).

write(IODevice, Bytes) ->
  try gen_server:call(IODevice, {write, Bytes}) of
    R -> R
  catch
    exit:{noproc,_} -> {error, terminated}
  end.

read(IODevice, Number) ->
  try gen_server:call(IODevice, {read, Number}) of
    R -> R
  catch
    exit:{noproc,_} -> {error, terminated}
  end.

close(IODevice) ->
  try gen_server:call(IODevice, close) of
    R -> R
  catch
    exit:{noproc,_} -> {error, terminated}
  end.

start_all() ->
  start_all(stream).
%%   observer:start().

start_all(App) ->
  case application:start(App) of
    {error,{not_started,DepApp}} ->
      io:format("starting ~p~n", [DepApp]),
      start_all(DepApp),
      start_all(App);
    R ->
      R
  end.

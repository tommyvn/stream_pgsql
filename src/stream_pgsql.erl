-module(stream_pgsql).
-export([open/2, delete/1, write/2, close/1, read/2]).
-export([start/0]).


-record(filedetails, {mode = nil, exclusive = nil, binary = nil}).


open(Name, ModeList) when is_list(Name) orelse is_binary(Name), is_list(ModeList) ->
  {ok, StreamPid} = stream_pgsql_sup:start_child(Name),
  case get_mode(ModeList) of
    {error, _Error} ->
      {error, badarg};
    Mode ->
      case gen_fsm:sync_send_event(StreamPid, {open, Mode}) of
        ok -> {ok, StreamPid};
        Error -> Error
      end
  end.

delete(Name) ->
  {ok, StreamPid} = stream_pgsql_sup:start_child(Name),
  gen_fsm:sync_send_event(StreamPid, delete).

write(IODevice, Bytes) ->
  try gen_fsm:sync_send_event(IODevice, {write, Bytes}) of
    R -> R
  catch
    exit:{noproc,_} -> {error, terminated}
  end.

read(IODevice, Number) ->
  try gen_fsm:sync_send_event(IODevice, {read, Number}) of
    R -> R
  catch
    exit:{noproc,_} -> {error, terminated}
  end.

close(IODevice) ->
  try gen_fsm:sync_send_event(IODevice, close) of
    R -> R
  catch
    exit:{noproc,_} -> {error, terminated}
  end.

start() ->
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

get_mode(ModeList) ->
  get_mode(ModeList, #filedetails{}).

get_mode([], #filedetails{mode = read, exclusive = nil, binary = true}) ->
  binary_read;

get_mode([], #filedetails{mode = write, exclusive = true, binary = true}) ->
  binary_exclusive_write;

get_mode([write | Rest], #filedetails{mode = nil} = FileDetails) ->
  get_mode(Rest, FileDetails#filedetails{mode = write});

get_mode([read | Rest], #filedetails{mode = nil} = FileDetails) ->
  get_mode(Rest, FileDetails#filedetails{mode = read});

get_mode([exclusive | Rest], #filedetails{exclusive = nil} = FileDetails) ->
  get_mode(Rest, FileDetails#filedetails{exclusive = true});

get_mode([binary | Rest], #filedetails{binary = nil} = FileDetails) ->
  get_mode(Rest, FileDetails#filedetails{binary = true});

get_mode(_, _FileDetails) ->
  {error, badarg}.
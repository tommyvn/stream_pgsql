-module(stream_pgsql).
-export([open/2, delete/1, write/2, close/1, read/2, list/0]).
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
  fsm_call_wrapper(IODevice, {write, Bytes}).

read(IODevice, Number) ->
  fsm_call_wrapper(IODevice, {read, Number}).

close(IODevice) ->
  fsm_call_wrapper(IODevice, close).

list() ->
  {ok, StreamPid} = stream_pgsql_sup:start_child(nil),
  gen_fsm:sync_send_event(StreamPid, list).

start() ->
  application:ensure_all_started(stream_pgsql).

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

fsm_call_wrapper(IODevice, Cmd) ->
  try gen_fsm:sync_send_event(IODevice, Cmd) of
    R -> R
  catch
    exit:{noproc,_} -> {error, terminated}
  end.

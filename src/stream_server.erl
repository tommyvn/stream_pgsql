%%%-------------------------------------------------------------------
%%% @author tom
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Sep 2014 20:12
%%%-------------------------------------------------------------------
-module(stream_server).
-author("tom").

-behaviour(gen_server).

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {name, mode, follow = true, tmp_file_pid}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Name :: term(), Modes :: term()) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Name, ModeList) ->
  gen_server:start_link( ?MODULE, [Name, ModeList], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([Name, ModeList]) ->
  case get_mode(ModeList) of
    {error, _Error} ->
      {stop, badarg};
    read = Mode ->
      case file:open(Name, [read]) of
        {ok, IODevice} ->
          case ets:lookup(stream_in_progress, Name) of
            [] -> ok;
            [{Name, Pid}] ->
              erlang:monitor(process, Pid)
          end,
          {ok, #state{name = Name, mode = Mode, tmp_file_pid = IODevice}};
        {error, enoent} ->
          {stop, enoent}
      end;
    exclusive_write = Mode ->
      case file:open(Name, [write, exclusive]) of
        {ok, IODevice} ->
          ets:insert(stream_in_progress, {Name, self()}),
          {ok, #state{name = Name, mode = Mode, tmp_file_pid = IODevice}};
        {error, eexist} ->
          {stop, eexist};
        {error, enoent} ->
          {stop, enoent}
      end
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).

handle_call({read, Number}, _From, State = #state{tmp_file_pid = IODevice, mode = read, follow = Follow}) ->
  case file:read(IODevice, Number) of
    {ok, Data} ->
      {reply, {ok, Data}, State};
    eof ->
      case Follow of
        false -> {reply, eof, State};
        true -> {reply, {ok, []}, State}
      end
  end;

handle_call({write, Bytes}, _From, State = #state{tmp_file_pid = IODevice, mode = exclusive_write}) ->
  ok = file:write(IODevice, Bytes),
  {reply, ok, State};

handle_call({write, _Bytes}, _From, State) ->
  {reply, {error, ebadf}, State};

handle_call(close, _From, State = #state{tmp_file_pid = IODevice, name = Name}) ->
  ets:delete(stream_in_progress, Name),
  ok = file:close(IODevice),
  {stop, normal, ok, State};

handle_call({read, _}, _From, State) ->
  {stop, normal, {error, ebadf}, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info({'DOWN', _Ref, process, _Pid, normal}, #state{name = Name} = State) ->
  ets:delete(stream_in_progress, Name),
  {noreply, State#state{follow = false}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_mode(ModeList) ->
  get_mode(ModeList, undefined).

get_mode([], FinalMode = read) ->
  FinalMode;
get_mode([], FinalMode = exclusive_write) ->
  FinalMode;
get_mode([], _) ->
  {error, badarg};

get_mode([ Mode | Rest ], FinalMode) ->
  case {FinalMode, Mode} of
    {write, exclusive} ->
      get_mode(Rest, exclusive_write);
    {exclusive, write} ->
      get_mode(Rest, exclusive_write);
    {undefined, read} ->
      get_mode(Rest, Mode);
    {undefined, write} ->
      get_mode(Rest, Mode);
    {undefined, exclusive} ->
      get_mode(Rest, Mode);
    _ ->
      {error, badarg}
  end.
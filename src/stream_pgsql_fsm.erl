%%%-------------------------------------------------------------------
%%% @author tom
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Sep 2014 21:15
%%%-------------------------------------------------------------------
-module(stream_pgsql_fsm).
-author("tom").

-behaviour(gen_fsm).

%% API
-export([start_link/1]).

%% gen_fsm callbacks
-export([init/1,
  startup/3,
  writing/3,
  reading/3,
  state_name/3,
  handle_event/3,
  handle_sync_event/4,
  handle_info/3,
  terminate/3,
  code_change/4]).

-define(SERVER, ?MODULE).
%% -define(CHUNK_SIZE, 512).

-record(state, {name, mode, follow = true, pgconn, reading_chunk = 0, read_buffer = <<>>}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Name :: term()) -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link(Name) ->
  %% This needs to use poolboy
  {PGHost, PGUser, PGPassword, PGOptions} = case os:getenv("DATABASE_URL") of
                                              false ->
                                                {"localhost", "ttytube", "ttytube", [{database, "ttytube"}]};
                                              DatabaseUrl ->
                                                {uri,<<"postgres">>, UserPassword, Host, Port, DBName, _, _, _} = uri:from_string(DatabaseUrl),
                                                [User, Password] = string:tokens(binary_to_list(UserPassword), ":"),
%%       io:format("connecting to ~p on port ~p with username ~p and password ~p~n", [Host, Port, User, Password]),
                                                {binary_to_list(Host), User, Password, [{port, Port}, {database, tl(binary_to_list(DBName))}, {ssl, true}]}
                                            end,
  {ok, PGConn} = pgsql:connect(PGHost, PGUser, PGPassword, PGOptions),
  gen_fsm:start_link(?MODULE, [PGConn, Name], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, StateName :: atom(), StateData :: #state{}} |
  {ok, StateName :: atom(), StateData :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([PGConn, Name]) ->
  {ok, startup, #state{pgconn = PGConn, name = Name}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @end
%%--------------------------------------------------------------------
-spec(startup(Event :: term(), From :: term(), State :: term()) ->
  {next_state, NextStateName :: atom(), NextState :: #state{}} |
  {next_state, NextStateName :: atom(), NextState :: #state{},
    timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
startup({open, binary_read}, _From, #state{pgconn = PGConn, name = Name} = State) ->
  BinName = case is_binary(Name) of
    true  -> Name;
    false -> list_to_binary(Name)
  end,
  case pgsql:equery(PGConn, "select id from stream_pgsql_metadata where id = $1", [Name]) of
    {ok,[{column,<<"id">>,text,-1,-1,1}],[]} ->
      {stop, normal, {error, enoent}, State};
    {ok,[{column,<<"id">>,text,-1,-1,1}],[{BinName}]} ->
      case ets:lookup(stream_pgsql_in_progress, Name) of
        [] ->
          self() ! {'DOWN', make_ref(), process, nil, notevenstartedproc};
        [{Name, Pid}] ->
          erlang:monitor(process, Pid)
      end,
      {reply, ok, reading, State}
  end;

startup({open, binary_exclusive_write}, _From, #state{pgconn = PGConn, name = Name} = State) ->
  case pgsql:equery(PGConn, "insert into stream_pgsql_metadata (id) values ($1)", [Name]) of
    {error,{error,error,<<"23505">>, _, _}} ->
      {stop, normal, {error, eexist}, State};
    {ok, 1} ->
      ets:insert(stream_pgsql_in_progress, {Name, self()}),
      {reply, ok, writing, State}
  end;

startup(delete, _From, #state{pgconn = PGConn, name = Name} = State) ->
  case { pgsql:equery(PGConn, "delete from stream_pgsql_metadata where id = $1", [Name]), pgsql:equery(PGConn, "delete from stream_pgsql_data where id = $1;", [Name]) } of
    { {ok, 1}, _} ->
      {stop, normal, ok, State};
    { {ok, 0}, _} ->
      {stop, normal, {error, enoent}, State}
  end;

startup(list, _From, #state{pgconn = PGConn} = State) ->
  case pgsql:squery(PGConn, "select id from stream_pgsql_metadata") of
    {ok,[{column,<<"id">>,text,-1,-1,0}], R} ->
      {stop, normal, lists:map(fun({X}) -> X end, R), State}
  end.

writing({write, Bytes}, _From, #state{pgconn = PGConn, name = Name} = State) ->
  case pgsql:equery(PGConn, "insert into stream_pgsql_data (id, data) values ($1, $2)", [Name, Bytes]) of
    {ok, 1} -> {reply, ok, writing, State};
    _ -> {stop, normal, {error, eweird}, State}
  end;
writing(close, _From, State) ->
  {stop, normal, ok, State};
writing(_, _From, State) ->
  {stop, normal, {error, ebadf}, State}.


reading(close, _From, State) ->
  {stop, normal, ok, State};

reading({read, Number}, _From, #state{read_buffer = ReadBuffer} = State) when size(ReadBuffer) >= Number ->
  {reply, {ok, ReadBuffer}, reading, State#state{read_buffer = <<>>}};

reading({read, _Number} = Cmd, From, #state{pgconn = PGConn, name = Name, reading_chunk = PreviousChunk, follow = Follow, read_buffer = ReadBuffer} = State) ->
  case pgsql:equery(PGConn, "select chunk, data from stream_pgsql_data where id = $1 and chunk > $2 order by chunk limit 1", [Name, PreviousChunk]) of
    {ok, _, [{CurrentChunk, Data}]} when is_binary(Data)->
      reading(Cmd, From, State#state{reading_chunk = CurrentChunk, read_buffer = <<ReadBuffer/binary, Data/binary>>});
    {ok, _, []} ->
      case size(ReadBuffer) > 0 of
        true ->
          {reply, {ok, ReadBuffer}, reading, State#state{read_buffer = <<>>}};
        false ->
          case Follow of
            true ->
              {reply, {ok, <<>>}, reading, State};
            false ->
              {reply, eof, reading, State}
          end
      end;
    Error -> {stop, normal, {error, Error}, State}
  end;
reading(_, _From, State) ->
  {stop, normal, {error, ebadf}, State}.



%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @end
%%--------------------------------------------------------------------
-spec(state_name(Event :: term(), From :: {pid(), term()},
    State :: #state{}) ->
  {next_state, NextStateName :: atom(), NextState :: #state{}} |
  {next_state, NextStateName :: atom(), NextState :: #state{},
    timeout() | hibernate} |
  {reply, Reply, NextStateName :: atom(), NextState :: #state{}} |
  {reply, Reply, NextStateName :: atom(), NextState :: #state{},
    timeout() | hibernate} |
  {stop, Reason :: normal | term(), NewState :: #state{}} |
  {stop, Reason :: normal | term(), Reply :: term(),
    NewState :: #state{}}).
state_name(_Event, _From, State) ->
  Reply = ok,
  {reply, Reply, state_name, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_event(Event :: term(), StateName :: atom(),
    StateData :: #state{}) ->
  {next_state, NextStateName :: atom(), NewStateData :: #state{}} |
  {next_state, NextStateName :: atom(), NewStateData :: #state{},
    timeout() | hibernate} |
  {stop, Reason :: term(), NewStateData :: #state{}}).
handle_event(_Event, StateName, State) ->
  {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_sync_event(Event :: term(), From :: {pid(), Tag :: term()},
    StateName :: atom(), StateData :: term()) ->
  {reply, Reply :: term(), NextStateName :: atom(), NewStateData :: term()} |
  {reply, Reply :: term(), NextStateName :: atom(), NewStateData :: term(),
    timeout() | hibernate} |
  {next_state, NextStateName :: atom(), NewStateData :: term()} |
  {next_state, NextStateName :: atom(), NewStateData :: term(),
    timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewStateData :: term()} |
  {stop, Reason :: term(), NewStateData :: term()}).
handle_sync_event(_Event, _From, StateName, State) ->
  Reply = ok,
  {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: term(), StateName :: atom(),
    StateData :: term()) ->
  {next_state, NextStateName :: atom(), NewStateData :: term()} |
  {next_state, NextStateName :: atom(), NewStateData :: term(),
    timeout() | hibernate} |
  {stop, Reason :: normal | term(), NewStateData :: term()}).
handle_info({'DOWN', _Ref, process, _Pid, _NormalOrNoproc}, reading, #state{name = Name} = State) ->
  ets:delete(stream_pgsql_in_progress, Name),
  {next_state, reading, State#state{follow = false}}.
%%
%% handle_info(_Info, StateName, State) ->
%%   {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: normal | shutdown | {shutdown, term()}
| term(), StateName :: atom(), StateData :: term()) -> term()).
terminate(_Reason, _StateName, #state{pgconn = PGConn} = _State) ->
  pgsql:close(PGConn),
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, StateName :: atom(),
    StateData :: #state{}, Extra :: term()) ->
  {ok, NextStateName :: atom(), NewStateData :: #state{}}).
code_change(_OldVsn, StateName, State, _Extra) ->
  {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

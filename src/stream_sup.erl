%%%-------------------------------------------------------------------
%%% @author tom
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Sep 2014 20:09
%%%-------------------------------------------------------------------
-module(stream_sup).
-author("tom").

-behaviour(supervisor).

%% API
-export([
  start_link/1,
  start_child/1
]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(CouchConn :: term()) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(CouchConn) ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, [CouchConn]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
    MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
    [ChildSpec :: supervisor:child_spec()]
  }} |
  ignore |
  {error, Reason :: term()}).
init([PGConn]) ->
  RestartStrategy = simple_one_for_one,
  MaxRestarts = 0,
  MaxSecondsBetweenRestarts = 1,

  SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

  Restart = temporary,
  Shutdown = 2000,
  Type = worker,

  AChild = {stream_fsm, {stream_fsm, start_link, [PGConn]},
    Restart, Shutdown, Type, [stream_fsm]},

  {ok, {SupFlags, [AChild]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_child(Name) ->
  supervisor:start_child(?SERVER, [Name]).
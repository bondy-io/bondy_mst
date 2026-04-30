%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_sync_scheduler).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Default sync scheduler (`_design/_implementation_plan.md` §5).

Periodic `gen_server` that, on each tick, asks
`bondy_oplog:list_instances/0` for the running instances and
— for each — invokes the configured peer source and dispatches a sync
session to each peer.

## Configuration

Read from app env at boot:

| Key                   | Default | Meaning |
|---|---|---|
| `sync_scheduler`      | `true`  | Enable / disable the scheduler. |
| `sync_interval_ms`    | `500`   | Time between ticks. |
| `peer_source`         | `bondy_oplog_peer_source_static` | Default behaviour module. |
| `peer_source_opts`    | `#{}`   | Default opts passed to `peers_for/2`. |
| `sync_dispatch`       | `undefined` | Optional `fun((InstanceId, [PeerId]) -> any())`; defaults to spawning one async sync session per peer. |

The default dispatch spawns one
`bondy_oplog_sync_session:start/3` per peer per tick;
exceptions raised by a custom dispatch are caught and logged.
""").

-record(state, {
    enabled :: boolean(),
    interval_ms :: non_neg_integer(),
    peer_source :: module(),
    peer_source_opts :: map(),
    dispatch :: undefined | fun((instance_id(), [peer_id()]) -> any()),
    tick_ref :: undefined | reference()
}).

%% Lifecycle
-export([start_link/0]).
-export([start_link/1]).
-export([child_spec/1]).

%% Control
-export([trigger/0]).
-export([set_dispatch/1]).
-export([set_peer_source/2]).
-export([info/0]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

%% =============================================================================
%% LIFECYCLE
%% =============================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.

start_link() ->
    start_link(#{}).

-spec start_link(map()) -> {ok, pid()} | {error, term()}.

start_link(Opts) when is_map(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

-spec child_spec(map()) -> supervisor:child_spec().

child_spec(Opts) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Opts]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

%% =============================================================================
%% CONTROL
%% =============================================================================

?DOC("""
Forces a tick now. Useful for tests and operational triggers.
""").
-spec trigger() -> ok.

trigger() ->
    gen_server:cast(?MODULE, tick).

?DOC("""
Replaces the dispatch callback. Pass `undefined` to disable dispatch
(ticks still run; nothing is invoked). Useful for runtime
reconfiguration and tests.
""").
-spec set_dispatch(undefined | fun((instance_id(), [peer_id()]) -> any())) ->
    ok.

set_dispatch(Fun) when is_function(Fun, 2); Fun =:= undefined ->
    gen_server:call(?MODULE, {set_dispatch, Fun}).

?DOC("""
Replaces the peer source module and options at runtime.
""").
-spec set_peer_source(module(), map()) -> ok.

set_peer_source(Mod, Opts) when is_atom(Mod), is_map(Opts) ->
    gen_server:call(?MODULE, {set_peer_source, Mod, Opts}).

?DOC("""
Returns the scheduler's current configuration. Cheap.
""").
-spec info() -> map().

info() ->
    gen_server:call(?MODULE, info).

%% =============================================================================
%% gen_server CALLBACKS
%% =============================================================================

init(Opts) ->
    process_flag(trap_exit, true),
    Dispatch =
        case maps:find(dispatch, Opts) of
            {ok, V} ->
                V;
            error ->
                case application:get_env(bondy_mst, sync_dispatch) of
                    {ok, EnvFun} -> EnvFun;
                    undefined -> fun default_dispatch/2
                end
        end,
    State = #state{
        enabled = maps:get(
            enabled,
            Opts,
            application:get_env(
                bondy_mst,
                sync_scheduler,
                true
            )
        ),
        interval_ms = maps:get(
            interval_ms,
            Opts,
            application:get_env(
                bondy_mst,
                sync_interval_ms,
                500
            )
        ),
        peer_source = maps:get(
            peer_source,
            Opts,
            application:get_env(
                bondy_mst,
                peer_source,
                bondy_oplog_peer_source_static
            )
        ),
        peer_source_opts = maps:get(
            peer_source_opts,
            Opts,
            application:get_env(
                bondy_mst, peer_source_opts, #{}
            )
        ),
        dispatch = Dispatch
    },
    {ok, schedule_tick(State)}.

handle_call(info, _From, State) ->
    Reply = #{
        enabled => State#state.enabled,
        interval_ms => State#state.interval_ms,
        peer_source => State#state.peer_source,
        peer_source_opts => State#state.peer_source_opts,
        dispatch_set => State#state.dispatch =/= undefined
    },
    {reply, Reply, State};
handle_call({set_dispatch, Fun}, _From, State) ->
    {reply, ok, State#state{dispatch = Fun}};
handle_call({set_peer_source, Mod, Opts}, _From, State) ->
    {reply, ok, State#state{peer_source = Mod, peer_source_opts = Opts}};
handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast(tick, State) ->
    {noreply, run_tick(State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    {noreply, schedule_tick(run_tick(State))};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
run_tick(#state{enabled = false} = State) ->
    State;
run_tick(#state{} = State) ->
    Instances = safe_list_instances(),
    lists:foreach(
        fun(InstanceId) -> dispatch_for(InstanceId, State) end,
        Instances
    ),
    telemetry:execute(
        [bondy_oplog, scheduler, sync, tick],
        #{instances => length(Instances)},
        #{}
    ),
    State.

%% @private
dispatch_for(InstanceId, #state{} = State) ->
    Peers = (State#state.peer_source):peers_for(
        InstanceId, State#state.peer_source_opts
    ),
    case State#state.dispatch of
        undefined ->
            ok;
        Fun when is_function(Fun, 2) ->
            try
                Fun(InstanceId, Peers)
            catch
                K:V:S ->
                    ?LOG_WARNING(#{
                        description => "sync dispatch raised",
                        instance => InstanceId,
                        class => K,
                        reason => V,
                        stacktrace => S
                    }),
                    ok
            end
    end.

%% @private
%% `list_instances/0` calls `info/1` on each running worker — if a
%% worker is mid-restart that call may briefly fail. Soft-fail to an
%% empty list rather than crash the scheduler.
safe_list_instances() ->
    try
        bondy_oplog:list_instances()
    catch
        _:_ -> []
    end.

%% @private
schedule_tick(#state{enabled = false} = State) ->
    State#state{tick_ref = undefined};
schedule_tick(#state{interval_ms = 0} = State) ->
    State#state{tick_ref = undefined};
schedule_tick(#state{interval_ms = Ms} = State) ->
    Ref = erlang:send_after(Ms, self(), tick),
    State#state{tick_ref = Ref}.

%% @private
%% Default dispatch: spawn one async sync session per peer for the
%% given instance. Errors from the spawn are absorbed by the session
%% process and reported via peer_state / logs; the scheduler does not
%% wait for completion.
default_dispatch(InstanceId, Peers) ->
    lists:foreach(
        fun(Peer) ->
            _ = bondy_oplog_sync_session:start(
                InstanceId, Peer, #{}
            )
        end,
        Peers
    ).

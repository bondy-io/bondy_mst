%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_instance).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
The per-instance Merkle Search Tree owner
(`_design/10_new_design.md` §11.3).

Exactly one process per running instance; one MST per instance; one
storage backend handle per instance.

## Responsibilities

- Own the MST handle (which itself owns the storage backend).
- Generate fresh `{HLC, Origin, Seq}` event keys on local appends.
- Sign local events through the configured validator.
- Verify peer events through the configured validator and append them
  via the merge strategy.
- Expose the MST root hash, key-range reads, and prefix truncation
  hooks for compaction.
- Run compaction cycles: stability frontier → `interpret_cog` →
  snapshot → MST truncate → watermark advance.

## What this module is *not*

The library is **agnostic** to lifecycle policy. This module does not
do lazy loading, LRU eviction, cold-tier offload, or per-tenant
naming. Those are *consumer* concerns (`_design/10_new_design.md`
§1.2, §12). The instance is started eagerly via
`bondy_oplog:start_instance/1,2`.

Anti-entropy and GC scheduling live in dedicated modules; they *use*
this one.

## Concurrency model

All operations currently round-trip the gen_server. Per-instance HLC
and Seq counters live in `atomics` cells inside the state record so
a future lock-free local-append path can move out of the gen_server
without protocol changes.
""").

-record(state, {
    instance_id :: binary(),
    origin :: bondy_oplog_origin:t(),
    hlc :: bondy_oplog_hlc:t(),
    seq :: atomics:atomics_ref(),
    mst :: bondy_mst:t(),
    backend :: backend(),
    validator_module :: module(),
    validator_state :: term(),
    merge_strategy :: module(),
    crdt_module :: module() | undefined,
    snapshot_store :: module(),
    snapshot_state :: term(),
    watermark :: undefined | bondy_oplog_event:event_key(),
    %% Cached `{Watermark, Snapshot}` from the snapshot store so the
    %% registry can publish it without re-reading the store on every
    %% mutation. Refreshed on init, compact, and load_snapshot.
    cached_snapshot :: undefined | {bondy_oplog_event:event_key(), term()},
    max_working_set :: pos_integer() | infinity,
    %% Cached size of the live MST (avoids a fold per append). Updated
    %% on every state-mutating handle_call.
    live_size :: non_neg_integer(),
    last_event_key :: undefined | bondy_oplog_event:event_key(),
    %% Compaction-in-flight tracking. While set, refuse new compact /
    %% load_snapshot requests (idempotent re-arm on next tick).
    compaction ::
        undefined
        | #{
            pid := pid(),
            from := gen_server:from(),
            started_at := integer()
        }
}).

-type backend() :: map | ets | module().

-type opts() :: #{
    backend => backend(),
    backend_options => map() | list(),
    storage_path => binary(),
    path_strategy => module(),
    hash_algorithm => sha256 | sha512,
    origin => bondy_oplog_origin:t(),
    hlc_seed => non_neg_integer(),
    seq_seed => non_neg_integer(),
    validator => module(),
    validator_opts => map(),
    merge_strategy => module(),
    crdt_module => module(),
    snapshot_store => module(),
    snapshot_store_opts => map(),
    max_working_set => pos_integer() | infinity
}.

-export_type([opts/0]).
-export_type([backend/0]).

%% Lifecycle
-export([start_link/2]).
-export([child_spec/2]).
-export([stop/1]).

%% Public API (typically called via `bondy_oplog`)
-export([append/2]).
-export([append/3]).
-export([append_many/2]).
-export([append_remote/2]).
-export([get/2]).
-export([root_hash/1]).
-export([fold_range/5]).
-export([range/3]).
-export([truncate_prefix/2]).
-export([size/1]).
-export([first_key/1]).
-export([latest_key/1]).
-export([origin/1]).
-export([info/1]).

%% Page-level API (sync protocol)
-export([get_pages/2]).
-export([merge_pages/2]).
-export([missing_set/2]).
-export([integrate_peer_root/2]).

%% GC / compaction API
-export([current_watermark/1]).
-export([crdt_module/1]).
-export([snapshot/1]).
-export([compact/2]).

%% Bootstrap
-export([load_snapshot/3]).

%% Registry helpers
-export([whereis/1]).
-export([lookup_origin/1]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

%% =============================================================================
%% LIFECYCLE
%% =============================================================================

-spec start_link(instance_id(), opts()) ->
    {ok, pid()} | {error, term()}.

start_link(InstanceId, Opts) when
    is_binary(InstanceId), is_map(Opts)
->
    gen_server:start_link(?MODULE, {InstanceId, Opts}, []).

-spec child_spec(instance_id(), opts()) -> supervisor:child_spec().

child_spec(InstanceId, Opts) ->
    #{
        id => {?MODULE, InstanceId},
        start => {?MODULE, start_link, [InstanceId, Opts]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

-spec stop(instance_id() | pid()) -> ok.

stop(Target) ->
    gen_server:stop(target(Target)).

%% =============================================================================
%% API
%% =============================================================================

-spec append(instance_id() | pid(), bondy_oplog_event:op()) ->
    bondy_oplog_event:event_key().

append(Target, Op) ->
    append(Target, Op, undefined).

-spec append(
    instance_id() | pid(),
    bondy_oplog_event:op(),
    bondy_oplog_event:meta()
) -> bondy_oplog_event:event_key().

append(Target, Op, Meta) ->
    gen_server:call(target(Target), {append, Op, Meta}, infinity).

?DOC("""
Appends a batch of operations atomically (all-or-nothing within the
instance). Returns the assigned keys in input order.
""").
-spec append_many(
    instance_id() | pid(),
    [{bondy_oplog_event:op(), bondy_oplog_event:meta()}]
) -> [bondy_oplog_event:event_key()].

append_many(_Target, []) ->
    [];
append_many(Target, OpsAndMetas) when is_list(OpsAndMetas) ->
    gen_server:call(target(Target), {append_many, OpsAndMetas}, infinity).

?DOC("""
Inserts an event received from a peer. Idempotent. Validation runs in
the caller's process: an Origin matching the instance's local Origin
raises `error/1` without disturbing the instance gen_server. The
configured validator's `verify_event/2` runs on the gen_server side
before the MST insert.
""").
-spec append_remote(instance_id() | pid(), bondy_oplog_event:t()) ->
    ok | {error, term()}.

append_remote(Target, Event) ->
    Key = bondy_oplog_event:key(Event),
    PeerOrigin = bondy_oplog_event:key_origin(Key),
    case lookup_origin(Target) of
        {ok, PeerOrigin} ->
            error({remote_event_with_local_origin, PeerOrigin});
        _ ->
            gen_server:call(target(Target), {append_remote, Event}, infinity)
    end.

-spec get(instance_id() | pid(), bondy_oplog_event:event_key()) ->
    {ok, bondy_oplog_event:t()} | not_found.

get(Target, Key) when is_binary(Target) ->
    %% Lock-free read path: pull the published MST handle from the
    %% registry and read directly. Bypasses the gen_server.
    case bondy_oplog_registry:mst(Target) of
        undefined ->
            error({noproc, {?MODULE, Target}});
        MST ->
            case bondy_mst:get(MST, Key) of
                undefined -> not_found;
                Value -> {ok, event_from_value(Key, Value)}
            end
    end;
get(Target, Key) ->
    gen_server:call(target(Target), {get, Key}).

-spec root_hash(instance_id() | pid()) -> binary() | undefined.

root_hash(Target) when is_binary(Target) ->
    case bondy_oplog_registry:mst(Target) of
        undefined -> error({noproc, {?MODULE, Target}});
        MST -> bondy_mst:root(MST)
    end;
root_hash(Target) ->
    gen_server:call(target(Target), root_hash).

-spec fold_range(
    instance_id() | pid(),
    From :: bondy_oplog_event:event_key(),
    To :: bondy_oplog_event:event_key(),
    fun((bondy_oplog_event:t(), Acc) -> Acc),
    Acc
) -> Acc when Acc :: term().

fold_range(Target, From, To, Fun, Acc0) when
    is_binary(Target), is_function(Fun, 2)
->
    case bondy_oplog_registry:mst(Target) of
        undefined ->
            error({noproc, {?MODULE, Target}});
        MST ->
            bondy_mst:fold(
                MST,
                fun
                    ({K, V}, Acc) when K >= From, K =< To ->
                        Fun(event_from_value(K, V), Acc);
                    (_, Acc) ->
                        Acc
                end,
                Acc0
            )
    end;
fold_range(Target, From, To, Fun, Acc0) when is_function(Fun, 2) ->
    gen_server:call(target(Target), {fold_range, From, To, Fun, Acc0}).

-spec range(
    instance_id() | pid(),
    From :: bondy_oplog_event:event_key(),
    To :: bondy_oplog_event:event_key()
) -> [bondy_oplog_event:t()].

range(Target, From, To) ->
    lists:reverse(
        fold_range(Target, From, To, fun(E, Acc) -> [E | Acc] end, [])
    ).

-spec truncate_prefix(instance_id() | pid(), bondy_oplog_event:event_key()) ->
    non_neg_integer().

truncate_prefix(Target, Watermark) ->
    gen_server:call(target(Target), {truncate_prefix, Watermark}, infinity).

-spec size(instance_id() | pid()) -> non_neg_integer().

size(Target) when is_binary(Target) ->
    case bondy_oplog_registry:live_size(Target) of
        undefined -> error({noproc, {?MODULE, Target}});
        N -> N
    end;
size(Target) ->
    gen_server:call(target(Target), instance_size).

-spec first_key(instance_id() | pid()) ->
    {ok, bondy_oplog_event:event_key()} | empty.

first_key(Target) when is_binary(Target) ->
    case bondy_oplog_registry:mst(Target) of
        undefined ->
            error({noproc, {?MODULE, Target}});
        MST ->
            case bondy_mst:first(MST) of
                undefined -> empty;
                {K, _V} -> {ok, K}
            end
    end;
first_key(Target) ->
    gen_server:call(target(Target), first_key).

-spec latest_key(instance_id() | pid()) ->
    {ok, bondy_oplog_event:event_key()} | empty.

latest_key(Target) when is_binary(Target) ->
    case bondy_oplog_registry:mst(Target) of
        undefined ->
            error({noproc, {?MODULE, Target}});
        MST ->
            case bondy_mst:last(MST) of
                undefined -> empty;
                {K, _V} -> {ok, K}
            end
    end;
latest_key(Target) ->
    gen_server:call(target(Target), latest_key).

-spec origin(instance_id() | pid()) -> bondy_oplog_origin:t().

origin(Target) ->
    case lookup_origin(Target) of
        {ok, Origin} -> Origin;
        not_found -> error({noproc, Target})
    end.

?DOC("""
Returns a diagnostic summary of an instance's state. Cheap;
appropriate for status pages and operational tools.
""").
-spec info(instance_id() | pid()) -> map().

info(Target) ->
    gen_server:call(target(Target), info).

%% =============================================================================
%% PAGE-LEVEL API (sync protocol)
%% =============================================================================

?DOC("""
Returns the subset of `Hashes` that this instance has, as a map of
`hash => page`. Hashes the instance does not have are silently absent
from the returned map.

Used by the sync protocol on the *responder* side: a peer asks for a
set of pages, this instance returns whichever it has.
""").
-spec get_pages(instance_id() | pid(), [bondy_mst:hash()]) ->
    #{bondy_mst:hash() => bondy_mst_page:t()}.

get_pages(Target, Hashes) when is_binary(Target), is_list(Hashes) ->
    %% Lock-free: pages live in the underlying store, addressable by
    %% hash. Read directly via the registry-published MST handle.
    case bondy_oplog_registry:mst(Target) of
        undefined ->
            error({noproc, {?MODULE, Target}});
        MST ->
            Store = bondy_mst:store(MST),
            lists:foldl(
                fun(Hash, Acc) ->
                    case bondy_mst_store:get(Store, Hash) of
                        undefined -> Acc;
                        Page -> Acc#{Hash => Page}
                    end
                end,
                #{},
                Hashes
            )
    end;
get_pages(Target, Hashes) when is_list(Hashes) ->
    gen_server:call(target(Target), {get_pages, Hashes}).

?DOC("""
Inserts a batch of pages received from a peer. Each page is verified
by re-hashing on insert; a hash mismatch (peer using a different
hash algorithm or malformed page) raises an error.

Used by the sync protocol on the *initiator* side after pulling pages
from a peer.
""").
-spec merge_pages(
    instance_id() | pid(),
    #{bondy_mst:hash() => bondy_mst_page:t()} | [bondy_mst_page:t()]
) -> ok.

merge_pages(Target, Pages) when is_map(Pages) ->
    merge_pages(Target, maps:values(Pages));
merge_pages(Target, Pages) when is_list(Pages) ->
    gen_server:call(target(Target), {merge_pages, Pages}, infinity).

?DOC("""
Returns the set of page hashes reachable from `Root` that this
instance does not have locally. Used by the sync protocol's initiator
to compute what to request from the peer.
""").
-spec missing_set(instance_id() | pid(), bondy_mst:hash()) ->
    [bondy_mst:hash()].

missing_set(Target, Root) when is_binary(Target), is_binary(Root) ->
    %% Lock-free: missing_set walks the store from Root computing what
    %% pages we lack. Pure read; safe off the gen_server.
    case bondy_oplog_registry:mst(Target) of
        undefined ->
            error({noproc, {?MODULE, Target}});
        MST ->
            Set = bondy_mst:missing_set(MST, Root),
            case is_list(Set) of
                true -> Set;
                false -> sets:to_list(Set)
            end
    end;
missing_set(Target, Root) when is_binary(Root) ->
    gen_server:call(target(Target), {missing_set, Root}).

?DOC("""
Integrates a peer's tree (identified by `PeerRoot`) into the local
MST. Pre-condition: every page reachable from `PeerRoot` must already
be present in the local store (caller's responsibility — typically
ensured by `missing_set/2` returning `[]` after page loading).

After this call, all events that were in the peer's tree are visible
to local queries, and the local root is the merged root of both trees.
""").
-spec integrate_peer_root(instance_id() | pid(), bondy_mst:hash()) -> ok.

integrate_peer_root(Target, PeerRoot) when is_binary(PeerRoot) ->
    gen_server:call(
        target(Target),
        {integrate_peer_root, PeerRoot},
        infinity
    ).

%% =============================================================================
%% GC / COMPACTION API
%% =============================================================================

?DOC("""
Returns the current compaction watermark — the highest event key that
has been folded into the snapshot. Events with keys ≤ watermark are
no longer in the MST.
""").
-spec current_watermark(instance_id() | pid()) ->
    undefined | bondy_oplog_event:event_key().

current_watermark(Target) when is_binary(Target) ->
    case ets_member(Target) of
        true -> bondy_oplog_registry:watermark(Target);
        false -> error({noproc, {?MODULE, Target}})
    end;
current_watermark(Target) ->
    gen_server:call(target(Target), current_watermark).

-spec crdt_module(instance_id() | pid()) -> module() | undefined.

crdt_module(Target) when is_binary(Target) ->
    case ets_member(Target) of
        true -> bondy_oplog_registry:crdt_module(Target);
        false -> error({noproc, {?MODULE, Target}})
    end;
crdt_module(Target) ->
    gen_server:call(target(Target), crdt_module).

?DOC("""
Returns `{ok, Watermark, Snapshot}` for the latest persisted snapshot,
or `not_found` if no compaction has run yet.
""").
-spec snapshot(instance_id() | pid()) ->
    {ok, bondy_oplog_event:event_key(), term()} | not_found.

snapshot(Target) when is_binary(Target) ->
    case bondy_oplog_registry:lookup(Target) of
        not_found -> error({noproc, {?MODULE, Target}});
        {ok, #{snapshot := undefined}} -> not_found;
        {ok, #{snapshot := {W, S}}} -> {ok, W, S}
    end;
snapshot(Target) ->
    gen_server:call(target(Target), get_snapshot).

?DOC("""
Runs one compaction cycle inside the instance gen_server.

`PeerRoots` is the list of root hashes confirmed at peers (from
`bondy_oplog_peer_state`). The instance:

1. Computes the stability frontier — the largest event key K such
   that every event with key ≤ K is reachable from every peer's root.
2. Extracts events in `(currentWatermark, frontier]`.
3. Calls the configured CRDT module's `interpret_cog/2` on top of
   the previous snapshot's state.
4. Persists the new snapshot at `frontier`.
5. Truncates the MST up to and including `frontier`.
6. Updates the watermark.

Returns `{ok, no_change}` if no advance is possible (no peers,
empty intersection, frontier ≤ current watermark); otherwise
`{ok, {compacted, NewWatermark, EventCount}}`.
""").
-spec compact(instance_id() | pid(), [bondy_mst:hash()]) ->
    {ok, no_change}
    | {ok, {compacted, bondy_oplog_event:event_key(), non_neg_integer()}}
    | {error, term()}.

compact(Target, PeerRoots) when is_list(PeerRoots) ->
    gen_server:call(target(Target), {compact, PeerRoots}, infinity).

?DOC("""
Bootstraps an instance by installing a peer's snapshot at `Watermark`.
Used by `bondy_oplog_sync_session:bootstrap/3` when a fresh
or far-behind replica joins a long-running cluster.

NOTE: not transactional with respect to a VM crash between
`put_snapshot` and the MST truncate. On the next start, events ≤ the
persisted watermark are filtered out at sync/append time, so the
transient overlap is self-correcting.

Atomic:

1. Persists the snapshot via the configured snapshot store.
2. Truncates the local MST up to and including `Watermark` (events ≤
   watermark are now in the snapshot, redundant in the live tree).
3. Advances the local watermark.
4. Updates the HLC to dominate `Watermark` so subsequent local
   appends sort above it.

Refuses to install a snapshot whose watermark is `=<` the current
watermark — going backwards would break monotonicity. Returns
`{ok, NewWatermark}` on success, `{error, watermark_not_advancing}`
otherwise.
""").
-spec load_snapshot(
    instance_id() | pid(),
    bondy_oplog_event:event_key(),
    term()
) -> {ok, bondy_oplog_event:event_key()} | {error, term()}.

load_snapshot(Target, Watermark, Snapshot) ->
    gen_server:call(
        target(Target),
        {load_snapshot, Watermark, Snapshot},
        infinity
    ).

%% =============================================================================
%% REGISTRY
%% =============================================================================

?DOC("""
Returns the pid of an instance owner gen_server, or `undefined` if no
such instance is currently running.
""").
-spec whereis(instance_id()) -> pid() | undefined.

whereis(InstanceId) when is_binary(InstanceId) ->
    case bondy_oplog_registry:pid(InstanceId) of
        undefined ->
            undefined;
        Pid ->
            case is_process_alive(Pid) of
                true -> Pid;
                false -> undefined
            end
    end.

-spec lookup_origin(instance_id() | pid()) ->
    {ok, bondy_oplog_origin:t()} | not_found.

lookup_origin(Pid) when is_pid(Pid) ->
    {ok, gen_server:call(Pid, origin, 5000)};
lookup_origin(InstanceId) when is_binary(InstanceId) ->
    case bondy_oplog_registry:origin(InstanceId) of
        undefined -> not_found;
        Origin -> {ok, Origin}
    end.

%% =============================================================================
%% gen_server CALLBACKS
%% =============================================================================

init({InstanceId, Opts}) ->
    process_flag(trap_exit, true),
    Origin = maps:get(origin, Opts, bondy_oplog_origin:default()),
    case bondy_oplog_origin:validate(Origin) of
        ok -> ok;
        {error, R0} -> error({invalid_origin, R0})
    end,
    HLC = bondy_oplog_hlc:new(maps:get(hlc_seed, Opts, 0)),
    SeqRef = atomics:new(1, [{signed, false}]),
    ok = atomics:put(SeqRef, 1, maps:get(seq_seed, Opts, 0)),
    ValidatorMod = maps:get(
        validator, Opts, bondy_oplog_validator_trust
    ),
    {ok, ValidatorState} =
        ValidatorMod:init(InstanceId, maps:get(validator_opts, Opts, #{})),
    MergeMod = maps:get(
        merge_strategy,
        Opts,
        bondy_oplog_merge_strict_uniqueness
    ),
    Backend = maps:get(backend, Opts, ets),
    CrdtModForWarn = maps:get(crdt_module, Opts, undefined),
    case Backend =:= map andalso CrdtModForWarn =/= undefined of
        true ->
            ?LOG_WARNING(#{
                description =>
                    "instance configured with map_store backend and a "
                    "crdt_module: each lock-free read copies the entire "
                    "map from the registry to the caller. Suitable for "
                    "tests only; use ets or a stateful custom backend "
                    "in production",
                instance_id => InstanceId
            });
        false ->
            ok
    end,
    MST = open_mst(InstanceId, Backend, MergeMod, Opts),
    %% Stage 5: snapshot store + watermark recovery.
    SnapshotMod = maps:get(
        snapshot_store,
        Opts,
        bondy_oplog_snapshot_store_ets
    ),
    {ok, SnapshotState} = SnapshotMod:init(
        InstanceId, maps:get(snapshot_store_opts, Opts, #{})
    ),
    Watermark = SnapshotMod:current_watermark(SnapshotState),
    CachedSnapshot =
        case SnapshotMod:get_snapshot(SnapshotState) of
            {ok, W0, S0} -> {W0, S0};
            not_found -> undefined
        end,
    CrdtMod = maps:get(crdt_module, Opts, undefined),
    %% Seed HLC from the highest persisted event key, so a restart with
    %% a durable backend doesn't issue keys below the previous high
    %% water mark. Sources, in order of precedence:
    %%   1. The MST's max event key (live events past the watermark).
    %%   2. The snapshot's compaction watermark.
    %% Fresh instances (ETS backend, no snapshot) leave the HLC at 0.
    LiveSize = compute_live_size(MST),
    LastMSTKey =
        case bondy_mst:last(MST) of
            undefined -> undefined;
            {K0, _V0} -> K0
        end,
    case LastMSTKey of
        undefined when Watermark =/= undefined ->
            _ = bondy_oplog_hlc:update(
                HLC, bondy_oplog_event:key_hlc(Watermark)
            );
        undefined ->
            ok;
        K ->
            _ = bondy_oplog_hlc:update(HLC, bondy_oplog_event:key_hlc(K))
    end,
    %% Seed Seq similarly: if the MST has local-origin events, advance
    %% the Seq counter to dominate the highest seen.
    case max_local_seq(MST, Origin) of
        undefined -> ok;
        MaxSeq -> ok = atomics:put(SeqRef, 1, MaxSeq)
    end,
    State = #state{
        instance_id = InstanceId,
        origin = Origin,
        hlc = HLC,
        seq = SeqRef,
        mst = MST,
        backend = Backend,
        validator_module = ValidatorMod,
        validator_state = ValidatorState,
        merge_strategy = MergeMod,
        crdt_module = CrdtMod,
        snapshot_store = SnapshotMod,
        snapshot_state = SnapshotState,
        watermark = Watermark,
        cached_snapshot = CachedSnapshot,
        max_working_set = maps:get(max_working_set, Opts, infinity),
        live_size = LiveSize,
        last_event_key = LastMSTKey,
        compaction = undefined
    },
    ok = publish(State),
    {ok, State}.

handle_call(Req, From, State0) ->
    case do_handle_call(Req, From, State0) of
        {reply, _, State0} = Reply ->
            %% No state change — no publish.
            Reply;
        {reply, _, State1} = Reply ->
            ok = publish(State1),
            Reply;
        Other ->
            Other
    end.

%% @private
do_handle_call({append, Op, Meta}, _From, State0) ->
    case backpressure_admit(State0, 1) of
        ok ->
            {Key, State} = do_append_local(State0, Op, Meta),
            {reply, Key, State};
        {error, _} = Err ->
            {reply, Err, State0}
    end;
do_handle_call({append_many, Items}, _From, State0) ->
    %% Atomic admission: either all events fit under the working-set
    %% cap or none are inserted.
    case backpressure_admit(State0, length(Items)) of
        ok ->
            {Keys, State} = lists:foldl(
                fun({Op, Meta}, {Acc, S0}) ->
                    {Key, S1} = do_append_local(S0, Op, Meta),
                    {[Key | Acc], S1}
                end,
                {[], State0},
                Items
            ),
            {reply, lists:reverse(Keys), State};
        {error, _} = Err ->
            {reply, Err, State0}
    end;
do_handle_call({append_remote, Event}, _From, State0) ->
    Origin = bondy_oplog_event:key_origin(bondy_oplog_event:key(Event)),
    case bondy_oplog_origin_bans:is_banned(Origin) of
        true ->
            telemetry:execute(
                [bondy_oplog, instance, append_remote, banned],
                #{count => 1},
                #{instance_id => State0#state.instance_id, origin => Origin}
            ),
            {reply, {error, banned_origin}, State0};
        false ->
            %% Backpressure applies to remote events too — a
            %% misbehaving peer must not be able to drive our working
            %% set arbitrarily high. Idempotent re-receives slip past
            %% this check and are treated as no-ops downstream.
            case backpressure_admit(State0, 1) of
                {error, _} = BPErr ->
                    {reply, BPErr, State0};
                ok ->
                    case verify(State0, Event) of
                        ok ->
                            case do_append_remote(State0, Event) of
                                {ok, State} ->
                                    {reply, ok, State};
                                {error, _} = Error ->
                                    {reply, Error, State0}
                            end;
                        {error, _} = Error ->
                            {reply, Error, State0}
                    end
            end
    end;
do_handle_call({get, Key}, _From, #state{mst = MST} = State) ->
    Reply =
        case bondy_mst:get(MST, Key) of
            undefined -> not_found;
            Value -> {ok, event_from_value(Key, Value)}
        end,
    {reply, Reply, State};
do_handle_call(root_hash, _From, #state{mst = MST} = State) ->
    {reply, bondy_mst:root(MST), State};
do_handle_call(
    {fold_range, From, To, Fun, Acc0},
    _From,
    #state{mst = MST} = State
) ->
    %% NOTE: `bondy_mst:fold/4`'s declared `{first, _}` / `{stop, _}`
    %% options are not yet honoured by `do_fold/5`. We filter inside
    %% the user fun. Range-aware fold is a Stage 6 candidate.
    Result = bondy_mst:fold(
        MST,
        fun({K, V}, Acc) ->
            case K >= From andalso K =< To of
                true -> Fun(event_from_value(K, V), Acc);
                false -> Acc
            end
        end,
        Acc0
    ),
    {reply, Result, State};
do_handle_call({truncate_prefix, Watermark}, _From, #state{mst = MST0} = State) ->
    %% Stage 2 simple truncation: collect keys ≤ Watermark and delete
    %% one by one. Descending delete order — see comment on
    %% truncate_below_or_equal/2 for the underlying `bondy_mst:delete/2`
    %% bug. Structural prefix-truncate (touching only the leftmost
    %% path) is a future optimisation in `bondy_mst` itself.
    Keys = bondy_mst:fold(
        MST0,
        fun
            ({K, _V}, Acc) when K =< Watermark -> [K | Acc];
            (_, Acc) -> Acc
        end,
        []
    ),
    MST1 = lists:foldl(
        fun(K, M) -> bondy_mst:delete(M, K) end,
        MST0,
        Keys
    ),
    Removed = length(Keys),
    {reply, Removed, State#state{
        mst = MST1,
        live_size = max(0, State#state.live_size - Removed)
    }};
do_handle_call(instance_size, _From, State) ->
    %% Cached counter; matches a fold of the live MST as long as
    %% all mutators keep it accurate.
    {reply, State#state.live_size, State};
do_handle_call(origin, _From, State) ->
    {reply, State#state.origin, State};
do_handle_call(first_key, _From, #state{mst = MST} = State) ->
    Reply =
        case bondy_mst:first(MST) of
            undefined -> empty;
            {K, _V} -> {ok, K}
        end,
    {reply, Reply, State};
do_handle_call(latest_key, _From, #state{mst = MST} = State) ->
    Reply =
        case bondy_mst:last(MST) of
            undefined -> empty;
            {K, _V} -> {ok, K}
        end,
    {reply, Reply, State};
do_handle_call(info, _From, State) ->
    Info = #{
        instance_id => State#state.instance_id,
        origin => State#state.origin,
        backend => State#state.backend,
        validator => State#state.validator_module,
        merge_strategy => State#state.merge_strategy,
        last_event_key => State#state.last_event_key
    },
    {reply, Info, State};
do_handle_call({get_pages, Hashes}, _From, #state{mst = MST} = State) ->
    Store = bondy_mst:store(MST),
    Pages = lists:foldl(
        fun(Hash, Acc) ->
            case bondy_mst_store:get(Store, Hash) of
                undefined -> Acc;
                Page -> Acc#{Hash => Page}
            end
        end,
        #{},
        Hashes
    ),
    {reply, Pages, State};
do_handle_call({merge_pages, Pages}, _From, #state{mst = MST0} = State) ->
    %% Page collection during anti-entropy. We only insert pages into
    %% the underlying store — the local root is *not* changed here, so
    %% no event becomes visible to readers and live_size is unaffected.
    %% The actual merge (root advance) is deferred to integrate_peer_root,
    %% which runs once the sync session has loaded every required page.
    %% This matches the bondy_mst_crdt anti-entropy pattern: the local
    %% tree is only mutated when the merge is fully prepared.
    MST = lists:foldl(
        fun(Page, Acc0) ->
            {_Hash, Acc1} = bondy_mst:put_page(Acc0, Page),
            Acc1
        end,
        MST0,
        Pages
    ),
    {reply, ok, State#state{mst = MST}};
do_handle_call({missing_set, Root}, _From, #state{mst = MST} = State) ->
    Set = bondy_mst:missing_set(MST, Root),
    Reply =
        case is_list(Set) of
            true -> Set;
            false -> sets:to_list(Set)
        end,
    {reply, Reply, State};
do_handle_call(
    {integrate_peer_root, PeerRoot},
    _From,
    #state{mst = MST0} = State
) ->
    MST1 = bondy_mst:merge(MST0, MST0, PeerRoot),
    %% Watermark filter: if our compaction has advanced past some of
    %% the events in PeerRoot's tree, re-truncate to drop them.
    MST2 =
        case State#state.watermark of
            undefined -> MST1;
            W -> truncate_below_or_equal(MST1, W)
        end,
    %% HLC update: events received via merge may carry HLCs higher than
    %% our local clock. Advance the HLC to dominate the merged tree's
    %% max key so subsequent local appends sort after every received
    %% event — and stay above any future watermark.
    case bondy_mst:last(MST2) of
        undefined ->
            ok;
        {LastKey, _V} ->
            _ = bondy_oplog_hlc:update(
                State#state.hlc, bondy_oplog_event:key_hlc(LastKey)
            )
    end,
    {reply, ok, State#state{
        mst = MST2,
        live_size = compute_live_size(MST2)
    }};
do_handle_call(current_watermark, _From, State) ->
    {reply, State#state.watermark, State};
do_handle_call(crdt_module, _From, State) ->
    {reply, State#state.crdt_module, State};
do_handle_call(get_snapshot, _From, State) ->
    Reply = (State#state.snapshot_store):get_snapshot(
        State#state.snapshot_state
    ),
    {reply, Reply, State};
do_handle_call({compact, PeerRoots}, From, State) ->
    do_compact_async(State, PeerRoots, From);
do_handle_call({load_snapshot, NewWatermark, Snapshot}, _From, State) ->
    do_load_snapshot(State, NewWatermark, Snapshot);
do_handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast(
    {compaction_done, Pid, Result},
    #state{compaction = #{pid := Pid}} = State0
) ->
    State = commit_compaction(State0, Result),
    ok = publish(State),
    {noreply, State};
handle_cast({compaction_done, _StalePid, _Result}, State) ->
    %% Worker pid doesn't match — must be a stale message from a
    %% previously-aborted run. Ignore.
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(
    {'DOWN', _Ref, process, Pid, Reason},
    #state{compaction = #{pid := Pid, from := From}} = State
) ->
    %% Compaction worker exited. If exit was normal, the cast already
    %% drove commit; otherwise we need to surface the failure.
    case Reason of
        normal ->
            {noreply, State};
        _ ->
            ?LOG_WARNING(#{
                description => "compaction worker died abnormally",
                instance_id => State#state.instance_id,
                reason => Reason
            }),
            gen_server:reply(From, {error, {compaction_worker_died, Reason}}),
            {noreply, State#state{compaction = undefined}}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{
    instance_id = Name,
    mst = MST,
    snapshot_store = SnapMod,
    snapshot_state = SnapState
}) ->
    _ = bondy_oplog_registry:unregister(Name),
    _ = catch SnapMod:close(SnapState),
    _ = catch bondy_mst:delete(MST),
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
do_append_local(#state{mst = MST0} = State, Op, Meta) ->
    HLC = bondy_oplog_hlc:now(State#state.hlc),
    Seq = atomics:add_get(State#state.seq, 1, 1),
    Key = bondy_oplog_event:key(HLC, State#state.origin, Seq),
    Event0 = bondy_oplog_event:new(Key, Op, Meta),
    {SignedEvent, ValidatorState} =
        (State#state.validator_module):sign_event(
            Event0, State#state.validator_state
        ),
    Value = value_from_event(SignedEvent),
    MST1 = bondy_mst:put(MST0, Key, Value),
    telemetry:execute(
        [bondy_oplog, instance, append],
        #{count => 1},
        #{instance_id => State#state.instance_id}
    ),
    {Key, State#state{
        mst = MST1,
        validator_state = ValidatorState,
        last_event_key = Key,
        live_size = State#state.live_size + 1
    }}.

%% @private
%% Returns `{ok, NewState}` on accepted insert (or below-watermark filter,
%% or idempotent re-receive); `{error, equivocation_detected}` when the
%% incoming event collides with a different existing value at the same
%% key. The collision is recorded in the quarantine table; the MST is
%% left unchanged. This keeps the gen_server alive — the alternative
%% (letting the strict merger crash on `bondy_mst:put`) would crash-loop
%% the instance on poisoned input.
do_append_remote(#state{mst = MST0} = State, Event) ->
    Key = bondy_oplog_event:key(Event),
    _ = bondy_oplog_hlc:update(
        State#state.hlc, bondy_oplog_event:key_hlc(Key)
    ),
    case below_or_equal_watermark(Key, State#state.watermark) of
        true ->
            telemetry:execute(
                [bondy_oplog, instance, append_remote, filtered],
                #{count => 1},
                #{
                    instance_id => State#state.instance_id,
                    reason => below_watermark
                }
            ),
            {ok, State};
        false ->
            NewValue = value_from_event(Event),
            case bondy_mst:get(MST0, Key) of
                undefined ->
                    {ok, do_insert_remote(State, Key, NewValue, true)};
                NewValue ->
                    %% Idempotent re-receive (bit-identical).
                    {ok, do_insert_remote(State, Key, NewValue, false)};
                ExistingValue ->
                    record_equivocation(State, Key, ExistingValue, Event),
                    {error, equivocation_detected}
            end
    end.

%% @private
do_insert_remote(State, Key, Value, IsNew) ->
    MST1 = bondy_mst:put(State#state.mst, Key, Value),
    telemetry:execute(
        [bondy_oplog, instance, append_remote, ok],
        #{count => 1},
        #{instance_id => State#state.instance_id, new => IsNew}
    ),
    LastKey =
        case State#state.last_event_key of
            undefined -> Key;
            Prev when Key > Prev -> Key;
            Prev -> Prev
        end,
    SizeDelta =
        case IsNew of
            true -> 1;
            false -> 0
        end,
    State#state{
        mst = MST1,
        last_event_key = LastKey,
        live_size = State#state.live_size + SizeDelta
    }.

%% @private
record_equivocation(#state{} = State, Key, ExistingValue, IncomingEvent) ->
    ExistingEvent = event_from_value(Key, ExistingValue),
    Proof = (State#state.validator_module):detect_equivocation(
        ExistingEvent, IncomingEvent
    ),
    bondy_oplog_quarantine:record(
        State#state.instance_id,
        Key,
        ExistingEvent,
        IncomingEvent,
        Proof
    ),
    telemetry:execute(
        [bondy_oplog, instance, append_remote, equivocation],
        #{count => 1},
        #{
            instance_id => State#state.instance_id,
            origin => bondy_oplog_event:key_origin(Key)
        }
    ),
    ok.

%% @private
below_or_equal_watermark(_Key, undefined) ->
    false;
below_or_equal_watermark(Key, Watermark) ->
    Key =< Watermark.

%% @private
%% Returns the number of items currently in the MST. Linear in tree
%% size; used after merge/integrate where the size delta is unknown.
compute_live_size(MST) ->
    bondy_mst:fold(MST, fun(_, Acc) -> Acc + 1 end, 0).

%% @private
%% MST value shape: a 4-tuple of `{Op, Meta, PrevHash, Signature}`.
value_from_event(Event) ->
    {
        bondy_oplog_event:op(Event),
        bondy_oplog_event:meta(Event),
        bondy_oplog_event:prev_hash(Event),
        bondy_oplog_event:signature(Event)
    }.

%% @private
event_from_value(Key, {Op, Meta, PrevHash, Signature}) ->
    bondy_oplog_event:new(Key, Op, Meta, PrevHash, Signature).

%% @private
%% Returns the maximum `Seq` field among events whose origin matches
%% `LocalOrigin`. `undefined` if no such events exist. Used at init to
%% seed the per-origin Seq counter from persisted state.
max_local_seq(MST, LocalOrigin) ->
    bondy_mst:fold(
        MST,
        fun({K, _V}, Acc) ->
            case bondy_oplog_event:key_origin(K) of
                LocalOrigin ->
                    Seq = bondy_oplog_event:key_seq(K),
                    case Acc of
                        undefined -> Seq;
                        N when Seq > N -> Seq;
                        N -> N
                    end;
                _ ->
                    Acc
            end
        end,
        undefined
    ).

%% @private
%% Backpressure admission test. Returns `ok` if the instance can
%% absorb `Delta` more events under its `max_working_set` cap, or
%% `{error, working_set_full}` otherwise. `infinity` disables the
%% cap.
backpressure_admit(#state{max_working_set = infinity}, _Delta) ->
    ok;
backpressure_admit(#state{max_working_set = Cap, live_size = Size}, Delta) when
    Size + Delta =< Cap
->
    ok;
backpressure_admit(
    #state{
        instance_id = Id,
        max_working_set = Cap,
        live_size = Size
    },
    Delta
) ->
    telemetry:execute(
        [bondy_oplog, instance, backpressure],
        #{count => 1},
        #{
            instance_id => Id,
            requested => Delta,
            live_size => Size,
            cap => Cap
        }
    ),
    {error, working_set_full}.

%% @private
%% Drops every key in MST that is `=< Watermark`. Used both by explicit
%% truncation and post-merge re-truncation. Linear in the prefix size;
%% a structural prefix-truncate that touches only the leftmost path
%% would be the long-term optimisation.
%%
%% Deletes are issued in **descending** order (highest first within the
%% to-remove prefix). The underlying `bondy_mst:delete/2` has been
%% observed to leave a page in an invalid state when keys are deleted
%% in ascending order — manifesting as a `case_clause` in
%% `bondy_mst:first/2`. Descending order avoids the pathological path.
truncate_below_or_equal(MST, Watermark) ->
    %% Collect keys ≤ Watermark *in descending order* (the fold cons-es
    %% in ascending order; we keep them ascending and reverse only when
    %% we want descending — here we want descending, so we don't reverse).
    Keys = bondy_mst:fold(
        MST,
        fun
            ({K, _V}, Acc) when K =< Watermark -> [K | Acc];
            (_, Acc) -> Acc
        end,
        []
    ),
    lists:foldl(fun(K, M) -> bondy_mst:delete(M, K) end, MST, Keys).

%% @private
%% Spawns a worker that does the heavy compaction work (frontier
%% computation, event fold, `interpret_cog`, snapshot persist) off the
%% gen_server. The gen_server only runs the final atomic commit
%% (truncate + watermark advance + HLC bump) when the worker reports
%% back via {compaction_done, ...} cast. Local appends and reads
%% proceed concurrently throughout.
%%
%% Concurrency guard: only one compaction in flight per instance.
%% Repeated `compact` requests while one is running reply
%% `{ok, no_change}` immediately — compaction is idempotent and
%% scheduled-driven, so the next tick will retry.
do_compact_async(#state{compaction = InFlight} = State, _PeerRoots, _From) when
    InFlight =/= undefined
->
    {reply, {ok, no_change}, State};
do_compact_async(#state{crdt_module = undefined} = State, _PeerRoots, _From) ->
    {reply, {error, no_crdt_module}, State};
do_compact_async(#state{} = State, PeerRoots, From) ->
    Self = self(),
    %% Capture exactly the data the worker needs. The State at
    %% commit time may differ (new appends), but truncation only
    %% removes events ≤ Frontier, which by construction were already
    %% present when the worker ran.
    MST = State#state.mst,
    Watermark0 = State#state.watermark,
    SnapshotStore = State#state.snapshot_store,
    SnapshotState = State#state.snapshot_state,
    CachedSnapshot = State#state.cached_snapshot,
    CrdtMod = State#state.crdt_module,
    InstanceId = State#state.instance_id,
    {Pid, _Ref} = spawn_monitor(fun() ->
        Result = run_compaction_worker(
            InstanceId,
            MST,
            Watermark0,
            PeerRoots,
            SnapshotStore,
            SnapshotState,
            CachedSnapshot,
            CrdtMod
        ),
        gen_server:cast(Self, {compaction_done, self(), Result})
    end),
    Started = erlang:monotonic_time(),
    Compaction = #{pid => Pid, from => From, started_at => Started},
    {noreply, State#state{compaction = Compaction}}.

%% @private
run_compaction_worker(
    InstanceId,
    MST,
    Watermark0,
    PeerRoots,
    SnapshotStore,
    SnapshotState,
    CachedSnapshot,
    CrdtMod
) ->
    try
        case compute_frontier_for(MST, PeerRoots) of
            undefined ->
                {ok, no_change};
            Frontier when
                Watermark0 =/= undefined,
                Frontier =< Watermark0
            ->
                {ok, no_change};
            Frontier ->
                Events = events_in_open_range(MST, Watermark0, Frontier),
                BaseSnapshot =
                    case CachedSnapshot of
                        undefined ->
                            case SnapshotStore:get_snapshot(SnapshotState) of
                                {ok, _W, S} -> S;
                                not_found -> CrdtMod:init()
                            end;
                        {_, S0} ->
                            S0
                    end,
                NewSnapshot = CrdtMod:interpret_cog(Events, BaseSnapshot),
                ok = SnapshotStore:put_snapshot(
                    SnapshotState, Frontier, NewSnapshot
                ),
                {ok, {compacted, Frontier, NewSnapshot, length(Events)}}
        end
    catch
        Class:Reason:Stack ->
            ?LOG_ERROR(#{
                description => "compaction worker raised",
                instance_id => InstanceId,
                class => Class,
                reason => Reason,
                stacktrace => Stack
            }),
            {error, {compaction_failed, Class, Reason}}
    end.

%% @private
%% Same algorithm as compute_frontier/2 but takes the captured MST
%% directly so it can run in the worker process.
compute_frontier_for(_MST, []) ->
    undefined;
compute_frontier_for(MST, PeerRoots) ->
    PeerSets = [keys_set_at_root(MST, R) || R <- PeerRoots, is_binary(R)],
    case PeerSets of
        [] ->
            undefined;
        [_ | _] ->
            LocalKeys = lists:reverse(
                bondy_mst:fold(
                    MST,
                    fun({K, _V}, Acc) -> [K | Acc] end,
                    []
                )
            ),
            longest_common_prefix(LocalKeys, PeerSets, undefined)
    end.

%% @private
%% Commits the worker's result atomically inside the gen_server.
%% Truncation uses the *current* state.mst — events appended during
%% the worker's run sort above Frontier and are preserved.
commit_compaction(
    #state{compaction = #{from := From, started_at := Started}} = State,
    {ok, no_change}
) ->
    gen_server:reply(From, {ok, no_change}),
    Duration = erlang:monotonic_time() - Started,
    telemetry:execute(
        [bondy_oplog, compaction, ok],
        #{duration => Duration, event_count => 0},
        #{instance_id => State#state.instance_id, frontier => undefined}
    ),
    State#state{compaction = undefined};
commit_compaction(
    #state{compaction = #{from := From, started_at := Started}} = State,
    {ok, {compacted, Frontier, NewSnapshot, EventCount}}
) ->
    MST1 = truncate_below_or_equal(State#state.mst, Frontier),
    _ = bondy_oplog_hlc:update(
        State#state.hlc, bondy_oplog_event:key_hlc(Frontier)
    ),
    State1 = State#state{
        mst = MST1,
        watermark = Frontier,
        cached_snapshot = {Frontier, NewSnapshot},
        live_size = max(0, State#state.live_size - EventCount),
        compaction = undefined
    },
    Duration = erlang:monotonic_time() - Started,
    telemetry:execute(
        [bondy_oplog, compaction, ok],
        #{duration => Duration, event_count => EventCount},
        #{instance_id => State#state.instance_id, frontier => Frontier}
    ),
    gen_server:reply(From, {ok, {compacted, Frontier, EventCount}}),
    State1;
commit_compaction(
    #state{compaction = #{from := From}} = State,
    {error, _} = Error
) ->
    gen_server:reply(From, Error),
    State#state{compaction = undefined}.

%% @private
%% Bootstrap: install a peer-supplied snapshot at the given watermark.
%% See `load_snapshot/3` for the contract.
%%
%% Refuses to run while a compaction worker is in flight — the two
%% operations both mutate the watermark/snapshot, and serialising them
%% is the simplest correctness story.
do_load_snapshot(#state{compaction = InFlight} = State, _, _) when
    InFlight =/= undefined
->
    {reply, {error, compaction_in_progress}, State};
do_load_snapshot(State, NewWatermark, Snapshot) ->
    case State#state.watermark of
        undefined ->
            apply_loaded_snapshot(State, NewWatermark, Snapshot);
        Current when NewWatermark > Current ->
            apply_loaded_snapshot(State, NewWatermark, Snapshot);
        _ ->
            {reply, {error, watermark_not_advancing}, State}
    end.

%% @private
apply_loaded_snapshot(State, NewWatermark, Snapshot) ->
    ok = (State#state.snapshot_store):put_snapshot(
        State#state.snapshot_state, NewWatermark, Snapshot
    ),
    %% Drop any live events that the new snapshot already covers.
    MST1 = truncate_below_or_equal(State#state.mst, NewWatermark),
    LiveSize1 = compute_live_size(MST1),
    %% Advance HLC to keep future local appends above the watermark.
    _ = bondy_oplog_hlc:update(
        State#state.hlc, bondy_oplog_event:key_hlc(NewWatermark)
    ),
    State1 = State#state{
        mst = MST1,
        watermark = NewWatermark,
        cached_snapshot = {NewWatermark, Snapshot},
        live_size = LiveSize1
    },
    {reply, {ok, NewWatermark}, State1}.

%% @private
%% Returns events in the half-open range (Watermark0, Frontier], in
%% key order. If Watermark0 is `undefined`, the range starts at
%% min_key (inclusive of all events ≤ Frontier).
events_in_open_range(MST, undefined, Frontier) ->
    lists:reverse(
        bondy_mst:fold(
            MST,
            fun
                ({K, V}, Acc) when K =< Frontier ->
                    [event_from_value(K, V) | Acc];
                (_, Acc) ->
                    Acc
            end,
            []
        )
    );
events_in_open_range(MST, W0, Frontier) ->
    lists:reverse(
        bondy_mst:fold(
            MST,
            fun
                ({K, V}, Acc) when K > W0, K =< Frontier ->
                    [event_from_value(K, V) | Acc];
                (_, Acc) ->
                    Acc
            end,
            []
        )
    ).

%% @private
%% Stability frontier:
%%   "the largest event key K such that every event with key ≤ K
%%    is reachable from every peer's confirmed root"
%%
%% Algorithm:
%%   1. For each PeerRoot, compute the set of keys reachable.
%%   2. Intersect with the local key set in key order.
%%   3. Return the largest K such that all keys up to and including K
%%      are in every peer's set.
%%
%% See `compute_frontier_for/2` for the worker-process variant used
%% during async compaction.

%% @private
keys_set_at_root(MST, Root) ->
    bondy_mst:fold(
        MST,
        fun({K, _V}, Acc) -> sets:add_element(K, Acc) end,
        sets:new([{version, 2}]),
        [{root, Root}]
    ).

%% @private
longest_common_prefix([], _PeerSets, Acc) ->
    Acc;
longest_common_prefix([K | Rest], PeerSets, Acc) ->
    case lists:all(fun(S) -> sets:is_element(K, S) end, PeerSets) of
        true -> longest_common_prefix(Rest, PeerSets, K);
        false -> Acc
    end.

%% @private
verify(#state{validator_module = Mod, validator_state = VS}, Event) ->
    Mod:verify_event(Event, VS).

%% @private
%% Builds the underlying MST struct.
open_mst(InstanceId, Backend, MergeMod, Opts) ->
    StoreMod = backend_module(Backend),
    StoreOpts = backend_opts(Backend, InstanceId, Opts),
    HashAlgo = maps:get(hash_algorithm, Opts, sha256),
    bondy_mst:new(#{
        store => StoreMod,
        store_opts => StoreOpts,
        hash_algorithm => HashAlgo,
        merger => fun(K, V1, V2) -> MergeMod:merge(K, V1, V2) end
    }).

%% @private
backend_module(map) -> bondy_mst_map_store;
backend_module(ets) -> bondy_mst_ets_store;
backend_module(Mod) when is_atom(Mod) -> Mod.

%% @private
backend_opts(ets, InstanceId, Opts) ->
    Defaults = #{name => InstanceId},
    maps:merge(Defaults, maps:get(backend_options, Opts, #{}));
backend_opts(_, InstanceId, Opts) ->
    Base = maps:get(backend_options, Opts, #{}),
    case maps:find(storage_path, Opts) of
        {ok, BaseDir} ->
            Strategy = maps:get(
                path_strategy, Opts, bondy_oplog_path_sharded
            ),
            Path = Strategy:storage_path(InstanceId, BaseDir),
            Base#{storage_path => unicode:characters_to_binary(Path)};
        error ->
            Base
    end.

%% @private
%% Publishes the current state's read-relevant fields to the registry.
%% Called after every state-mutating handle_call so that lock-free
%% read paths see fresh data without round-tripping the gen_server.
publish(#state{} = State) ->
    bondy_oplog_registry:publish(#{
        instance_id => State#state.instance_id,
        pid => self(),
        origin => State#state.origin,
        mst => State#state.mst,
        watermark => State#state.watermark,
        snapshot => State#state.cached_snapshot,
        crdt_module => State#state.crdt_module,
        live_size => State#state.live_size
    }).

%% @private
ets_member(InstanceId) ->
    bondy_oplog_registry:pid(InstanceId) =/= undefined.

%% @private
target(Pid) when is_pid(Pid) ->
    Pid;
target(InstanceId) when is_binary(InstanceId) ->
    case ?MODULE:whereis(InstanceId) of
        undefined -> error({noproc, {?MODULE, InstanceId}});
        Pid -> Pid
    end;
target(Other) ->
    error({invalid_target, Other}).

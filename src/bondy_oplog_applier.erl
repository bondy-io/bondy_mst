%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_applier).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").
-include("bondy_oplog_wal.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Per-instance applier loop.

Sits between the per-instance WAL writer and the per-instance
`bondy_oplog_instance` gen_server. Responsibilities:

- On start, choose a resume position based on the live MST's
  high-watermark + the snapshot watermark (see `resume_position/2`)
  and open a non-following `bondy_oplog_wal_reader` there.
- Drain the reader in batches. For each event in the batch the
  applier re-verifies the stored signature (defence-in-depth against
  WAL tampering) and then dispatches the surviving events to the
  instance via a one-way `gen_server:cast` so the instance installs
  them in the MST and evicts the matching overlay rows. The drain
  loop does not block on the install — the cast lands in the
  instance mailbox in FIFO order and is processed concurrently with
  the next batch's read+verify.
- At commit boundaries (every `commit_every` events or `end_of_log`)
  the applier issues a synchronous `drain_install_queue` call to the
  instance before persisting `consumer.offset` and advancing the
  WAL's committed-segment marker. This call returns once every
  in-flight install cast has been processed, so retention never
  drops a segment whose events the instance has not yet installed.
- Acts as the verify gateway for peer-received events.
  `bondy_oplog_instance:append_remote/2` forwards each remote event
  here via `enqueue_remote/2`. The applier captures a read-only
  snapshot of the validator state at `init/1` and, on every
  `enqueue_remote` call, spawns a short-lived worker that re-verifies
  the signature, forwards verified events to the instance for
  origin-ban / backpressure / watermark filtering and the MST
  install, and replies to the caller. The applier's mailbox is
  freed immediately so WAL drain and concurrent remote events can
  interleave. This keeps the applier as the sole verify+dispatch
  origin for both local and remote events. Tree-level operations
  (`merge_pages`, `integrate_peer_root`, `truncate_prefix`, `compact`,
  `load_snapshot`) are not event-stream operations and remain in the
  instance; the public façade drains the applier before invoking
  them.
- Owns the validator snapshot used for re-verification. Operators
  can rotate the snapshot at runtime via the
  `{refresh_validator, Reason}` cast (entry point is
  `bondy_oplog_instance:refresh_validator/1`); the cast calls
  `Mod:refresh/1` on the current snapshot and, on `{ok, NewState}`,
  installs `NewState` in the applier state. Workers spawned *before*
  the cast was processed continue to verify against the snapshot
  they captured — there is no mid-flight swap.

## Resume position

For a *durable* MST backend the live MST holds the highest applied
event from the previous run; for a *volatile* (ETS) backend it is
empty after a subtree restart. In both cases the resume HLC is
`max(MST_last_key.hlc, snapshot_watermark.hlc)`, falling back to
`beginning` when both are unknown. `bondy_mst:put` is content-
addressable so the small overlap that `{hlc, _}` includes around the
resume frame is an idempotent no-op.

## Configuration

| Option              | Default | Meaning |
|---|---|---|
| `commit_every`      | `64`    | Apply this many events between `consumer.offset` flushes. |
| `poll_interval_ms`  | `5`     | Backstop sleep when `await_durable/3` returns sooner than expected. The hot path long-polls rather than sleeping; this only affects the rare error fallback. |
| `ae_targets`        | `[]`    | List of `{Namespace, Index, Shard}` tuples whose AE-freshness counters are bumped via `bondy_db_core_registry:bump_ae/4` after every successful commit. Empty list disables the wiring. |
| `publish_ns`        | `undefined` | Namespace under which post-apply events are published via `bondy_db_core:publish/4`. `undefined` disables publishing. Requires `publish_fun`. |
| `publish_fun`       | `undefined` | `fun((bondy_oplog_event:t()) -> {Key, Op} \| skip)` invoked per verified event to derive the `(Key, Op)` pair forwarded to subscribers. `skip` suppresses publish for that event. Required when `publish_ns` is set. |

## Substrate read-side wiring

The applier optionally drives the read-side substrate
(`MST_DB_DESIGN.md` §11/§12). Both hooks are opt-in and consumer-
configured; defaults are no-ops so existing instances are unaffected.

- **Freshness (`bump_ae`)** — after every successful commit
  (`commit_now/1` flushed `consumer.offset` and advanced the WAL
  committed-segment marker) the applier walks `ae_targets` and bumps
  each shard's AE atomic counter with a single shared
  `monotonic_time(millisecond)` so a batch of shards observes the same
  "now". Missing registry entries are tolerated and surfaced via a
  `not_found` counter in telemetry; they typically indicate a
  registration race during startup. The AE-side bump path (long-quiet
  shards) is a separate follow-on (`MST_DB_DESIGN.md §18` item 8).
- **Subscriptions (`publish`)** — after `apply_batch/2` produces a
  non-empty verified set the applier walks the set in order and calls
  `publish_fun` per event. The applier passes `(Namespace, Key, Hlc,
  Op)` to `bondy_db_core:publish/4`. Delivery is best-effort
  (dispatcher walks subscribers; no round-trip; pattern matching runs
  in the applier process). Events for which `publish_fun` returns
  `skip` are not published. The applier's own mailbox is never
  blocked on delivery.

  **Timing — at-apply, not at-commit.** Publishing fires inside
  `apply_batch/2` (per verified event, in HLC-monotonic order). This
  deviates from `MST_DB_DESIGN.md §18` item 7's "Same as (6)" hint —
  which would batch publish into `commit_now/1` to mirror bump_ae —
  and was chosen deliberately on the live system:

  - Latency: at-commit would batch up to `commit_every` events (default
    64) into one burst delivered at the commit barrier. At-apply
    publishes per event with no added queueing delay.
  - Commit-barrier cost: `commit_now/1` is already a synchronous
    barrier (it issues `drain_install_queue` against the instance
    gen_server). Folding an N-event publish pass into that barrier
    would extend it linearly in the batch size with ETS `select`s and
    `erlang:send/2`s.
  - Graceful-shutdown gap: at-commit would require an in-memory
    accumulator drained from `terminate/2`. The shutdown path already
    writes `consumer.offset` independently, so a partial drain failure
    would silently lose subscriber notifications without a way for
    crash recovery to recover them (the offset advanced).
  - Crash semantics: at-least-once delivery is the contract on either
    side (a crash between apply and commit re-applies events on
    restart, producing duplicates regardless of timing). Subscribers
    must be idempotent or self-dedup — this is the contract.
  - Read consistency: readers via `bondy_oplog:read/3` see the new
    value as soon as the overlay/MST holds it (before commit), so
    at-apply publishes already align with what concurrent readers
    observe. Substrate-side reads through `bondy_db_core:read/3` depend
    on a separate projection-write path (out of scope here).

  Subscribers that need commit-coherent batching can coalesce
  consumer-side; the substrate's `commit_every` parameter is not the
  right knob for subscriber delivery cadence.
""").

-record(state, {
    instance_id :: instance_id(),
    instance_pid :: pid(),
    wal_pid :: pid(),
    wal_dir :: file:filename_all(),
    iter :: bondy_oplog_wal_reader:t() | undefined,
    consumer_offset :: bondy_oplog_wal_state:consumer_offset(),
    %% Number of events applied since the last `commit/1`. Used to
    %% batch consumer.offset writes — flushed at `commit_every` or
    %% when the reader returns `end_of_log`.
    uncommitted :: non_neg_integer(),
    commit_every :: pos_integer(),
    %% Milliseconds between polling ticks when the reader returns
    %% `end_of_log`. Constant for now; the writer publishes an atomics
    %% durable position so a future revision could long-poll instead.
    poll_interval_ms :: pos_integer(),
    %% Validator module + snapshot of validator state for signature
    %% re-verification (S1) in the applier process. Fetched once from
    %% the instance at `init/1`. `verify_event/2` is read-only on
    %% state (the only state mutation happens in `sign_event/2` which
    %% the instance owns), so the snapshot remains valid for the
    %% lifetime of the applier.
    validator_module :: module(),
    validator_state :: term(),
    %% Per-instance fold projection (FOLD_STRATEGY_DESIGN §3 +
    %% §6/§7). Read once from the registry at `init/1`; `undefined`
    %% when no fold is configured for the instance, in which case the
    %% fold path is a strict no-op.
    %%
    %% Scope: single-cell-per-instance. The fold's event vocabulary is
    %% the `op` field of each WAL event (see `bondy_oplog_event:op/1`)
    %% by convention. Per-cell projections are deferred to MST_DB_DESIGN.
    %% Remote events bypass the WAL drain path and are NOT folded yet —
    %% F8 documents this as a known gap; F9's cross-PR QA will track
    %% resolution.
    fold_module :: bondy_oplog_fold:strategy() | undefined,
    fold_state :: term(),
    %% Substrate read-side wiring (MST_DB_DESIGN §11). Shards bumped
    %% via `bondy_db_core_registry:bump_ae/4` after each successful
    %% commit. Empty list disables the wiring.
    ae_targets = [] :: [shard_key()],
    %% Substrate subscription wiring (MST_DB_DESIGN §12). When both
    %% `publish_ns` and `publish_fun` are set, every verified event in
    %% an applied batch is forwarded to `bondy_db_core:publish/4` at
    %% apply time. See moduledoc "Substrate read-side wiring" for the
    %% rationale behind the at-apply timing.
    publish_ns :: atom() | undefined,
    publish_fun :: publish_fun() | undefined,
    %% Per-cell projection write wiring (`MST_DB_DESIGN.md` §6.3).
    %% When set, events whose op matches `{cell_apply, Bucket, Key, FoldEvent}`
    %% bypass the per-instance fold and instead do a read-modify-write
    %% against the projection adapter registered for the configured
    %% `(NS, Index, Shard)` triple in `bondy_db_core_registry`. The
    %% cell's fold module (taken from the registry entry, which can
    %% differ from the per-instance `fold_module`) drives the
    %% decode/apply/encode cycle. `undefined` disables the path —
    %% existing instances are unaffected.
    cell_apply_ctx :: cell_apply_ctx() | undefined,
    %% Demand-based flow control toward the instance gen_server. The
    %% applier increments slot 1 of `install_in_flight` before each
    %% `gen_server:cast({install_local_batch, …})`; the instance
    %% decrements it after handling the cast. When the value would
    %% reach `max_install_in_flight`, the applier defers reading the
    %% next WAL batch and waits for the instance to send a
    %% `drain_resume` cast. Bounds the instance's mailbox; without
    %% it, sustained write throughput overruns the install path and
    %% builds an unbounded backlog (observable as an 8 GB+ RES set
    %% under stress, and ultimately a `gen_server:call` timeout on
    %% `drain_install_queue` during commit).
    install_in_flight :: atomics:atomics_ref() | undefined,
    max_install_in_flight :: pos_integer() | undefined,
    %% Set when the applier deferred a drain because the cap was
    %% reached. The next `drain_resume` cast (or, defensively, the
    %% backstop poll timer) re-arms `self() ! drain`.
    drain_deferred = false :: boolean(),
    %% Root hash of the MST snapshot whose `cell_apply` events have
    %% already been folded into the projection. `do_replay_cell_events/1`
    %% diffs the live MST against this root via `bondy_mst:diff_to_list/2`
    %% and only re-applies the new entries — so the cost of a replay is
    %% O(events since last sync), not O(events in MST). `undefined`
    %% triggers a one-time full fold (cold start / restart, since the
    %% MST may hold peer-authored events whose `cell_apply` has never
    %% been replayed on this node). Advanced exclusively from
    %% `do_replay_cell_events/1` after the diff fold completes — *not*
    %% from `commit_now/1`, because a peer `integrate_peer_root` can
    %% interleave with the WAL drain and land remote pages under the
    %% post-barrier root, and those remote events have not been folded
    %% into the projection until the replay path runs. Advancing the
    %% watermark from `commit_now/1` regresses convergence (Jepsen
    %% OR-set: 27/226 lost adds).
    last_replayed_root = undefined :: undefined | bondy_mst:hash(),
    %% Bootstrap lifecycle handle (`bondy_oplog_bootstrap_lifecycle`).
    %% Cached once at `init/1` from the registry; the gate check in
    %% `drain_loop/1` is then a single `atomics:get/2`. `undefined`
    %% means the entry hasn't published one yet (race with the
    %% instance's `init/1`) and is treated as `live` for backward
    %% compatibility — the instance's publish is idempotent and will
    %% catch up by the next backstop tick. See
    %% `_design/catalogue_expansion_plan.md` §2.
    lifecycle :: bondy_oplog_bootstrap_lifecycle:handle() | undefined,
    %% Monitor reference of the parked idle-wait helper process (see
    %% `arm_idle_waiter/1`). `undefined` when the applier is actively
    %% draining or about to. The helper blocks on the WAL's
    %% `await_durable/3`; its monitor `DOWN` wakes the applier to
    %% re-drain. Event-driven replacement for the historical busy poll.
    idle_waiter = undefined :: undefined | reference()
}).

-type shard_key() :: {atom(), atom(), non_neg_integer()}.
-type publish_fun() :: fun(
    (bondy_oplog_event:t()) -> {Key :: term(), Op :: term()} | skip
).
-type cell_apply_ctx() :: #{
    shard_key := shard_key(),
    adapter := module(),
    handle := term(),
    fold_module := bondy_oplog_fold:strategy(),
    %% Cache adapter pair captured at init time so the applier can
    %% keep the per-shard read cache coherent after every projection
    %% write. Without this, `bondy_db:apply/4` followed by `read/3` on
    %% a different process returns stale state — the cache is
    %% populate-on-miss and never invalidated by writers otherwise.
    cache_adapter => module() | undefined,
    cache_handle => term(),
    %% Per-shard high-water HLC mark. Advanced via
    %% `bondy_oplog_high_water:advance/2` after every successful
    %% projection write in `apply_one_cell/11`. `undefined` when the
    %% shard's registry entry has no ref (legacy entries created
    %% before PR-D1 §3 — defensive only; new registrations always
    %% allocate).
    high_water_ref => bondy_oplog_high_water:ref() | undefined
}.

-type opts() :: #{
    instance_id := instance_id(),
    wal_dir := file:filename_all(),
    commit_every => pos_integer(),
    poll_interval_ms => pos_integer(),
    %% Substrate read-side wiring (MST_DB_DESIGN §18 item 6).
    ae_targets => [shard_key()],
    %% Substrate subscription wiring (MST_DB_DESIGN §18 item 7).
    publish_ns => atom(),
    publish_fun => publish_fun(),
    %% Per-cell projection write wiring (MST_DB_DESIGN §6.3).
    %% Setting this requires the shard to be already registered in
    %% `bondy_db_core_registry`. Resolved eagerly at init/1.
    cell_apply_target => shard_key()
}.

-export_type([opts/0]).

-export([start_link/1]).
-export([child_spec/1]).
-export([stop/1]).
-export([enqueue_remote/2]).
-export([refresh_validator/2]).
-export([projection/1]).
-export([notify_drain_resume/1]).
-export([replay_cell_events/1]).
-export([replay_cell_events_sync/1]).
-export([cell_apply_target/1]).
-export([install_catalogue_batch/2]).
-export([resolve_logical_event/4]).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

-define(DEFAULT_COMMIT_EVERY, 64).
%% The applier long-polls the WAL via `await_durable/3` on
%% `end_of_log` rather than sleeping between ticks, so this only
%% bounds the wake-up cadence when the WAL is idle. A small interval
%% keeps responsiveness if `await_durable/3` ever returns sooner than
%% expected or a future revision drops the long-poll path.
-define(DEFAULT_POLL_INTERVAL_MS, 5).
%% Soft inner timeout for the `await_durable/3` long-poll. Bounded so
%% supervisor shutdown messages and any future control-plane signals
%% are processed in a timely fashion.
-define(AWAIT_DURABLE_TIMEOUT_MS, 200).

%% =============================================================================
%% API
%% =============================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.

start_link(#{instance_id := _, wal_dir := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec child_spec(opts()) -> supervisor:child_spec().

child_spec(#{instance_id := InstanceId} = Opts) ->
    #{
        id => {?MODULE, InstanceId},
        start => {?MODULE, start_link, [Opts]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

-spec stop(pid()) -> ok.

stop(Pid) when is_pid(Pid) ->
    try gen_server:stop(Pid, normal, 5000) of
        ok -> ok
    catch
        exit:noproc -> ok;
        exit:{noproc, _} -> ok
    end.

-spec enqueue_remote(pid(), bondy_oplog_event:t()) ->
    ok | {error, term()}.

%% Verify gateway for peer-received events. The call returns once a
%% per-event worker has finished verifying the signature, forwarded
%% the event to the instance, and received its accept/reject reply —
%% so callers continue to see `equivocation_detected`, `banned_origin`,
%% and other accept/reject modes synchronously. While the worker runs,
%% the applier's own mailbox is free, so WAL drain and other remote
%% events interleave without head-of-line blocking.
enqueue_remote(ApplierPid, Event) when is_pid(ApplierPid) ->
    gen_server:call(ApplierPid, {enqueue_remote, Event}, infinity).

-spec refresh_validator(pid(), term()) -> ok.

%% Asks the applier to refresh its in-process validator snapshot by
%% calling `Mod:refresh/1` on the current snapshot. The cast is
%% fire-and-forget; the applier logs success/failure and emits
%% telemetry. Validators that do not export `refresh/1` are a no-op
%% (debug log).
%%
%% Operators normally call `bondy_oplog_instance:refresh_validator/1`,
%% which resolves the applier pid for them.
refresh_validator(ApplierPid, Reason) when is_pid(ApplierPid) ->
    gen_server:cast(ApplierPid, {refresh_validator, Reason}).

-spec notify_drain_resume(pid()) -> ok.

-doc """
Called by the instance after it processes an `install_local_batch`
cast and the in-flight counter drops below the cap. Lets the applier
resume reading the WAL if it had deferred its drain. Idempotent:
extra resumes during normal operation are absorbed by the
`drain_deferred` flag.
""".
notify_drain_resume(ApplierPid) when is_pid(ApplierPid) ->
    gen_server:cast(ApplierPid, drain_resume).

-spec replay_cell_events(pid()) -> ok.

-doc """
Re-fold the entire MST through the cell_apply path. Intended to be
called by the instance after a `merge_pages` / `integrate_peer_root`
cycle — without this, peer-received events sit in the local MST but
never reach the per-cell projection, and `bondy_db:read/3` returns
only events authored on the local node.

Idempotent for the CRDT folds that ship with the library (LWW
register, OR-set, map_of_fields, ttl_presence): replaying an
absorbed event either no-ops (same dot already in OR-set live or
tombstones; same `{set, V, H}` already applied) or yields the same
terminal state (later-HLC LWW). `strict_register` rejects duplicates
with `{error, ...}` from `apply_event/3` but `apply_one_cell` already
catches and logs.

A no-op when the instance was started without a `cell_apply_target`
— pure-substrate consumers are not affected.
""".
replay_cell_events(ApplierPid) when is_pid(ApplierPid) ->
    gen_server:cast(ApplierPid, replay_cell_events).

-spec replay_cell_events_sync(pid()) -> ok.

-doc """
Synchronous variant of `replay_cell_events/1`. Blocks the caller
until the diff fold has been applied to the projection, so a read
issued immediately after this returns observes the peer-merged events
the corresponding sync session installed. Otherwise identical to the
cast (idempotent, no-op when `cell_apply_target` is not configured).
""".
replay_cell_events_sync(ApplierPid) when is_pid(ApplierPid) ->
    gen_server:call(ApplierPid, replay_cell_events, infinity).

-spec projection(pid()) ->
    {ok, term()} | {error, no_fold_configured}.

-doc """
Returns the current fold projection for the applier's instance.

`{error, no_fold_configured}` when the instance was started without a
`fold_module` opt (the legacy event-storage path is in effect).

The reply observes the freshest fold state visible to the applier
*after* the call is processed — synchronous `gen_server:call/2`
contract. Events appended after the call returns are not reflected.
Callers that need read-your-writes semantics across a recent append
should call `bondy_oplog:await_apply/1` first.
""".
projection(ApplierPid) when is_pid(ApplierPid) ->
    gen_server:call(ApplierPid, get_projection, infinity).

-spec cell_apply_target(pid()) -> {ok, shard_key()} | undefined.

-doc """
Returns the applier's resolved `cell_apply_target` shard key, or
`undefined` if no projection target was configured. Used by the
catalogue-snapshot bootstrap path to discover where to read the
projection's cells from.
""".
cell_apply_target(ApplierPid) when is_pid(ApplierPid) ->
    gen_server:call(ApplierPid, cell_apply_target, infinity).

-spec resolve_logical_event(pid(), term(), term(), term()) ->
    {ok, term() | passthrough} | {error, term()}.

-doc """
Translate a logical event into a physical event by reading the
current projection state for `(Bucket, Key)` and dispatching to the
fold's `resolve_event/2` callback. Used by `bondy_db:apply/4` for
event shapes that need server-side resolution (e.g. AW-Map's
`{remove_aw_key, K}` → `{remove, K, ObservedDots}`).

Returns `{ok, passthrough}` when the logical event has no effect
against current state (e.g. remove of an absent / tombstoned key);
the caller skips the WAL append. Otherwise returns
`{ok, ResolvedEvent}` for substrate-side append.

`{error, no_cell_apply_target}` if the applier wasn't configured
with a `cell_apply_target`.
""".
resolve_logical_event(ApplierPid, Bucket, Key, Event) when
    is_pid(ApplierPid)
->
    gen_server:call(
        ApplierPid,
        {resolve_logical_event, Bucket, Key, Event},
        infinity
    ).

-type install_mode() :: replace | merge.

-spec install_catalogue_batch(
    pid(),
    [bondy_oplog_transport:cell()]
    | {install_mode(), [bondy_oplog_transport:cell()]}
) ->
    {ok, #{
        installed := non_neg_integer(),
        skipped := non_neg_integer(),
        merged := non_neg_integer(),
        replaced_no_merge := non_neg_integer()
    }}
    | {error, term()}.

-doc """
Installs a batch of catalogue-snapshot cells into the applier's
projection shard. Each cell is `{Bucket, Key, Frame}` where `Frame` is
a V2 cell frame as produced by the peer's projection adapter.

Two modes:

- **`replace`** (fresh bootstrap; `WasLive = false`): for each cell,
  if the existing local HLC is `>=` the incoming HLC the cell is
  skipped (Q11 per-cell HLC guard against bootstrap-vs-live
  interleave). Otherwise the frame is written through unchanged.
- **`merge`** (recovering bootstrap; `WasLive = true`): for each cell,
  if no local cell exists it is written through; if a local cell
  exists, the fold's `merge_states/2` is invoked on
  `(IncomingState, LocalState)` and the merged state is encoded into
  a fresh frame. Folds without `merge_states/2` (only `presence_basic`
  in the shipped catalogue) emit telemetry
  `[bondy_oplog, applier, catalogue_bootstrap, presence_basic_replaced]`
  and fall back to skip-if-older replacement.

Both modes invalidate the read cache and advance the per-shard
high-water HLC atomic after each successful write.

Returns `{ok, #{installed := N, skipped := M, merged := P,
replaced_no_merge := Q}}`.

`installed` counts straight writes, `merged` counts merge_states
writes, `replaced_no_merge` counts merge-mode cells where the fold
lacks `merge_states/2` and the path fell back to skip-if-older.

Returns `{error, no_cell_apply_target}` if the applier was not started
with a `cell_apply_target`.
""".
install_catalogue_batch(ApplierPid, Cells) when
    is_pid(ApplierPid), is_list(Cells)
->
    install_catalogue_batch(ApplierPid, {replace, Cells});
install_catalogue_batch(ApplierPid, {Mode, Cells}) when
    is_pid(ApplierPid),
    is_list(Cells),
    (Mode =:= replace orelse Mode =:= merge)
->
    gen_server:call(
        ApplierPid, {install_catalogue_batch, Mode, Cells}, infinity
    ).

%% =============================================================================
%% gen_server CALLBACKS
%% =============================================================================

init(#{instance_id := InstanceId, wal_dir := WalDir} = Opts) ->
    process_flag(trap_exit, true),
    %% Off-heap inbox — the applier consumes batches from the WAL on
    %% one side and posts `install_local_batch` casts back to the
    %% instance on the other; either side may bunch under load. Off-
    %% heap messages keep the applier's own heap small.
    process_flag(message_queue_data, off_heap),
    CommitEvery = maps:get(commit_every, Opts, ?DEFAULT_COMMIT_EVERY),
    PollMs = maps:get(poll_interval_ms, Opts, ?DEFAULT_POLL_INTERVAL_MS),
    case validate_substrate_opts(Opts) of
        ok ->
            do_init(InstanceId, WalDir, CommitEvery, PollMs, Opts);
        {error, _} = Err ->
            {stop, Err}
    end.

do_init(InstanceId, WalDir, CommitEvery, PollMs, Opts) ->
    AeTargets = maps:get(ae_targets, Opts, []),
    PublishNs = maps:get(publish_ns, Opts, undefined),
    PublishFun = maps:get(publish_fun, Opts, undefined),
    case resolve_cell_apply_ctx(Opts) of
        {ok, CellCtx} ->
            do_init_2(
                InstanceId,
                WalDir,
                CommitEvery,
                PollMs,
                Opts,
                AeTargets,
                PublishNs,
                PublishFun,
                CellCtx
            );
        {error, _} = Err ->
            {stop, Err}
    end.

do_init_2(
    InstanceId,
    WalDir,
    CommitEvery,
    PollMs,
    _Opts,
    AeTargets,
    PublishNs,
    PublishFun,
    CellCtx
) ->
    case resolve_siblings(InstanceId) of
        {ok, InstP, WalP, MST, Watermark} ->
            CO = read_consumer_offset(WalDir),
            StartPos = resume_position(MST, Watermark),
            case
                bondy_oplog_wal_reader:open(
                    WalP, StartPos, [{follow, false}]
                )
            of
                {ok, Iter} ->
                    {ValidatorMod, ValidatorState} =
                        bondy_oplog_instance:get_validator(InstP),
                    {FoldMod, FoldState0} = init_fold(InstanceId),
                    %% Snapshot the demand-based flow-control handle
                    %% published by the instance's `init/1`. `undefined`
                    %% means the entry hasn't caught up yet — the
                    %% applier treats that as "no cap" and falls back
                    %% to the previous unbounded behaviour until the
                    %% next drain pass picks the ref up.
                    InFlightRef =
                        bondy_oplog_registry:install_in_flight(InstanceId),
                    InFlightCap =
                        bondy_oplog_registry:max_install_in_flight(InstanceId),
                    Lifecycle =
                        bondy_oplog_registry:lifecycle(InstanceId),
                    State = #state{
                        instance_id = InstanceId,
                        instance_pid = InstP,
                        wal_pid = WalP,
                        wal_dir = WalDir,
                        iter = Iter,
                        consumer_offset = CO,
                        uncommitted = 0,
                        commit_every = CommitEvery,
                        poll_interval_ms = PollMs,
                        validator_module = ValidatorMod,
                        validator_state = ValidatorState,
                        fold_module = FoldMod,
                        fold_state = FoldState0,
                        ae_targets = AeTargets,
                        publish_ns = PublishNs,
                        publish_fun = PublishFun,
                        cell_apply_ctx = CellCtx,
                        install_in_flight = InFlightRef,
                        max_install_in_flight = InFlightCap,
                        lifecycle = Lifecycle
                    },
                    ok = bondy_oplog_registry:set_applier_pid(
                        InstanceId, self()
                    ),
                    self() ! drain,
                    %% Cold-replay catch-up: a durable MST can hold
                    %% peer-authored events from a previous run whose
                    %% `replay_cell_events` never ran (process died
                    %% between `integrate_peer_root/2` and the cast).
                    %% The WAL drain only handles events past
                    %% `resume_position/2`, so without this the
                    %% projection stays stale until the next sync tick.
                    case CellCtx of
                        undefined -> ok;
                        _ -> gen_server:cast(self(), replay_cell_events)
                    end,
                    {ok, State};
                {error, Reason} ->
                    {stop, {reader_open_failed, Reason}}
            end;
        {error, _} = Err ->
            {stop, Err}
    end.

%% @private
%% Resolve the optional `cell_apply_target` into a `cell_apply_ctx`
%% map of the projection adapter, handle, and fold module from the
%% shard's registry entry. `not_found` is a hard error so a typo'd
%% triple surfaces at startup instead of silently disabling the path.
resolve_cell_apply_ctx(Opts) ->
    case maps:get(cell_apply_target, Opts, undefined) of
        undefined ->
            {ok, undefined};
        {NS, Index, Shard} = Key ->
            case bondy_db_core_registry:lookup(NS, Index, Shard) of
                {ok, Entry} ->
                    {ok, #{
                        shard_key => Key,
                        adapter =>
                            bondy_db_core_registry:entry_projection_adapter(
                                Entry
                            ),
                        handle =>
                            bondy_db_core_registry:entry_projection_handle(
                                Entry
                            ),
                        fold_module =>
                            bondy_db_core_registry:entry_fold_module(Entry),
                        cache_adapter =>
                            bondy_db_core_registry:entry_cache_adapter(Entry),
                        cache_handle =>
                            bondy_db_core_registry:entry_cache_handle(Entry),
                        high_water_ref =>
                            bondy_db_core_registry:entry_high_water_ref(Entry)
                    }};
                not_found ->
                    {error, {cell_apply_target_not_registered, Key}}
            end
    end.

handle_call(
    {enqueue_remote, Event},
    From,
    #state{
        validator_module = Mod,
        validator_state = VS,
        instance_pid = InstP,
        instance_id = Id
    } = State
) ->
    %% Spawn-and-reply: free the applier mailbox immediately so the WAL
    %% drain (`handle_info(drain, _)`) and other `enqueue_remote` calls
    %% can interleave. The worker captures the read-only validator
    %% snapshot + the instance pid + the caller's `From` tag, performs
    %% the verify, forwards verified events to the instance for
    %% origin-ban / backpressure / watermark / install, and replies on
    %% behalf of the applier. The outer try/catch wraps the entire
    %% worker body — including `gen_server:reply/2` — so the caller
    %% can never hang on its `infinity` call: any exception (verify
    %% raised, forward raised, even reply raised) is logged and a
    %% best-effort fallback reply is attempted via `catch`.
    _ = spawn(fun() ->
        try
            Reply =
                case Mod:verify_event(Event, VS) of
                    ok ->
                        forward_remote(InstP, Event);
                    {error, Reason} = VerifyErr ->
                        ok = log_verify_failure(Id, Event, Reason),
                        VerifyErr
                end,
            gen_server:reply(From, Reply)
        catch
            C:R:S ->
                ?LOG_WARNING(#{
                    description =>
                        "bondy_oplog_applier verify worker raised before "
                        "delivering a reply; the remote event has been "
                        "rejected",
                    instance_id => Id,
                    class => C,
                    reason => R,
                    stacktrace => S
                }),
                %% Best-effort fallback. `gen_server:reply/2` is
                %% documented as never failing on a dead caller, but
                %% the wrapping `catch` swallows any pathological
                %% exception so the worker always exits cleanly.
                catch gen_server:reply(From, {error, {verify_crashed, R}})
        end
    end),
    {noreply, State};
handle_call(
    get_projection,
    _From,
    #state{fold_module = undefined} = State
) ->
    {reply, {error, no_fold_configured}, State};
handle_call(
    get_projection,
    _From,
    #state{fold_state = FS} = State
) ->
    {reply, {ok, FS}, State};
handle_call(
    cell_apply_target,
    _From,
    #state{cell_apply_ctx = undefined} = State
) ->
    {reply, undefined, State};
handle_call(
    cell_apply_target,
    _From,
    #state{cell_apply_ctx = #{shard_key := Key}} = State
) ->
    {reply, {ok, Key}, State};
handle_call(
    {install_catalogue_batch, _Mode, _Cells},
    _From,
    #state{cell_apply_ctx = undefined} = State
) ->
    {reply, {error, no_cell_apply_target}, State};
handle_call(
    {install_catalogue_batch, Mode, Cells},
    _From,
    #state{
        cell_apply_ctx = Ctx,
        instance_id = Id
    } = State
) ->
    Result = do_install_catalogue_batch(Id, Ctx, Mode, Cells),
    {reply, Result, State};
handle_call(replay_cell_events, _From, State) ->
    %% Synchronous variant of the `replay_cell_events` cast. Runs the
    %% same diff fold and replies `ok` once the projection has caught
    %% up. Callers that need read-your-peers-write semantics use this
    %% instead of the cast.
    {reply, ok, do_replay_cell_events(State)};
handle_call(
    {resolve_logical_event, _Bucket, _Key, _Event},
    _From,
    #state{cell_apply_ctx = undefined} = State
) ->
    {reply, {error, no_cell_apply_target}, State};
handle_call(
    {resolve_logical_event, Bucket, Key, Event},
    _From,
    #state{cell_apply_ctx = Ctx} = State
) ->
    #{
        adapter := Adapter,
        handle := Handle,
        fold_module := Fold
    } = Ctx,
    %% Read current cell state. With single-applier-per-cell, this
    %% read is serialised against the applier's event loop (the
    %% applier's `drain_loop/1` releases between batches; gen_server
    %% calls dispatch between handler returns). The instance defers
    %% WAL append until our reply, so resolve+append is atomic from
    %% the caller's perspective.
    State0 =
        case Adapter:get(Handle, Bucket, Key) of
            not_found ->
                bondy_oplog_fold:initial_value(Fold);
            {ok, Frame} ->
                {_PrevHlc, StateBytes, _ValueBytes} =
                    bondy_oplog_cell_frame:decode_full(Frame),
                bondy_oplog_fold:decode_state(Fold, StateBytes)
        end,
    Reply =
        case bondy_oplog_fold:resolve_event(Fold, State0, Event) of
            passthrough -> {ok, passthrough};
            Resolved -> {ok, Resolved}
        end,
    {reply, Reply, State};
handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast({refresh_validator, Reason}, State) ->
    {noreply, do_refresh_validator(Reason, State)};
handle_cast(replay_cell_events, State) ->
    {noreply, do_replay_cell_events(State)};
handle_cast(drain_resume, #state{drain_deferred = false} = State) ->
    %% Already draining (or about to); the next `self() ! drain` will
    %% pick up the freed slot anyway. Drop the redundant signal.
    {noreply, State};
handle_cast(drain_resume, #state{drain_deferred = true} = State) ->
    %% Capacity has freed up; resume the drain loop immediately.
    self() ! drain,
    {noreply, State#state{drain_deferred = false}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(drain, State0) ->
    %% A fresh drain supersedes any parked idle waiter — cancel it so
    %% waiter helpers don't accumulate across drains.
    State1 = cancel_idle_waiter(State0),
    case drain_loop(State1) of
        {ok, State2} ->
            %% Caught up. Park an async waiter on the WAL's durable
            %% position instead of re-sending `drain` immediately (a
            %% busy spin: the next-to-read byte is already durable in
            %% `per_write` mode, so an inline `await_durable/3` returns
            %% at once) or blocking the gen_server here (which would
            %% stall the `replay_cell_events` cast and other messages
            %% that cross-node sync depends on). The waiter fires the
            %% instant a new frame becomes durable — immediate apply
            %% latency, near-zero idle CPU, responsive mailbox.
            {noreply, arm_idle_waiter(State2)};
        {paused, State2} ->
            %% Hit the demand cap. Stay parked — the instance will
            %% send `drain_resume` once it processes a batch. The
            %% backstop timer is a defensive belt-and-braces in case
            %% the signal is ever lost (e.g. instance restart between
            %% increment and decrement); it costs ~one wake per
            %% second when fully gated and nothing when not.
            _ = erlang:send_after(1_000, self(), drain_backstop),
            {noreply, State2#state{drain_deferred = true}};
        {stop, Reason, State2} ->
            {stop, Reason, State2}
    end;
handle_info(
    {'DOWN', MRef, process, _Pid, _Reason},
    #state{idle_waiter = MRef} = State
) ->
    %% Our parked idle waiter finished: the WAL's durable position
    %% advanced past our read offset, the await timed out, or the WAL
    %% errored. In every case the right response is to re-drain (if
    %% nothing new is there we simply re-arm). Using the monitor `DOWN`
    %% as the wakeup keeps the helper a pure side-effect-free blocker
    %% (it sends no message of its own), so a crashed helper can never
    %% wedge the applier.
    self() ! drain,
    {noreply, State#state{idle_waiter = undefined}};
handle_info(drain_backstop, #state{drain_deferred = true} = State) ->
    %% Defensive re-arm in case `drain_resume` was missed. If the cap
    %% is still saturated, `drain_loop` returns `{paused, _}` again
    %% and another backstop is scheduled.
    self() ! drain,
    {noreply, State#state{drain_deferred = false}};
handle_info(drain_backstop, State) ->
    %% Backstop fired while we were already draining — ignore.
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{
    iter = Iter,
    consumer_offset = CO,
    wal_dir = Dir,
    uncommitted = N
}) ->
    case N > 0 of
        true -> _ = bondy_oplog_wal_state:write_consumer_offset(Dir, CO);
        false -> ok
    end,
    case Iter of
        undefined -> ok;
        _ -> bondy_oplog_wal_reader:close(Iter)
    end,
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
%% Looks up the three pids and the MST/watermark snapshot the applier
%% needs from the per-instance registry row. Returns a structured
%% error pointing at the first missing field so operators can diagnose
%% supervisor-start-order or registry-publish races.
resolve_siblings(InstanceId) ->
    InstancePid = bondy_oplog_registry:instance_pid(InstanceId),
    WalPid = bondy_oplog_registry:wal_pid(InstanceId),
    MST = bondy_oplog_registry:mst(InstanceId),
    Watermark = bondy_oplog_registry:watermark(InstanceId),
    case missing_sibling(InstancePid, WalPid, MST) of
        none ->
            {ok, InstancePid, WalPid, MST, Watermark};
        Field ->
            {missing_sibling, #{
                instance_id => InstanceId,
                missing => Field,
                instance_pid => InstancePid,
                wal_pid => WalPid,
                mst_present => MST =/= undefined
            }}
    end.

%% @private
missing_sibling(undefined, _, _) -> instance_pid;
missing_sibling(_, undefined, _) -> wal_pid;
missing_sibling(_, _, undefined) -> mst;
missing_sibling(_, _, _) -> none.

%% @private
%% Resolves the per-instance fold strategy (FOLD_STRATEGY_DESIGN
%% §6/§7) from the registry and seeds the initial projection state.
%% Returns `{undefined, undefined}` when no fold is configured —
%% callers check `fold_module` and skip the fold path.
init_fold(InstanceId) ->
    case bondy_oplog_registry:fold_module(InstanceId) of
        undefined ->
            {undefined, undefined};
        Strategy ->
            Initial = bondy_oplog_fold:initial_value(Strategy),
            {Strategy, Initial}
    end.

%% @private
%% Resume from `max(last_MST_key.hlc, watermark.hlc)`. The reader's
%% `{hlc, T}` start finds the first frame whose first event HLC is
%% `>= T`, so the frame that contained our resume HLC is re-read and
%% its events are re-applied — that is safe because `bondy_mst:put`
%% is content-addressable and verify+install are idempotent.
%%
%% Falls back to `beginning` when both inputs are absent (fresh
%% instance with empty MST and no compaction history) or when the
%% MST handle is missing.
resume_position(MST, Watermark) ->
    case resume_hlc(MST, Watermark) of
        undefined -> beginning;
        HLC -> {hlc, HLC}
    end.

%% @private
resume_hlc(MST, Watermark) ->
    MstHlc = mst_last_hlc(MST),
    WmHlc = watermark_hlc(Watermark),
    case {MstHlc, WmHlc} of
        {undefined, undefined} -> undefined;
        {undefined, H} -> H;
        {H, undefined} -> H;
        {A, B} when A >= B -> A;
        {_, B} -> B
    end.

%% @private
mst_last_hlc(undefined) ->
    undefined;
mst_last_hlc(MST) ->
    case bondy_mst:last(MST) of
        undefined -> undefined;
        {Key, _Value} -> bondy_oplog_event:key_hlc(Key)
    end.

%% @private
watermark_hlc(undefined) ->
    undefined;
watermark_hlc(Key) ->
    bondy_oplog_event:key_hlc(Key).

%% @private
%% Demand-based dispatch gate. Returns `true` while the in-flight
%% counter is below the cap (or the instance hasn't published a cap
%% yet, in which case the legacy unbounded behaviour applies).
install_dispatch_allowed(#state{
    install_in_flight = undefined
}) ->
    true;
install_dispatch_allowed(#state{
    max_install_in_flight = undefined
}) ->
    true;
install_dispatch_allowed(#state{
    install_in_flight = Ref,
    max_install_in_flight = Cap
}) ->
    atomics:get(Ref, 1) < Cap.

%% @private
%% Increment the in-flight counter just before dispatching a
%% `install_local_batch` cast to the instance. Idempotent for the
%% `undefined` fallback so callers don't have to branch.
reserve_install_slot(#state{install_in_flight = undefined}) ->
    ok;
reserve_install_slot(#state{install_in_flight = Ref}) ->
    _ = atomics:add(Ref, 1, 1),
    ok.

%% @private
%% Reads the on-disk `consumer.offset`. Used to seed the in-memory
%% commit accumulator so a brand-new applier doesn't overwrite a
%% live consumer.offset with a fresh `{0, 48}`. The resume position
%% itself comes from `resume_position/2` (MST + watermark), not from
%% this file, so a missing or stale offset is safe.
read_consumer_offset(WalDir) ->
    case bondy_oplog_wal_state:read_consumer_offset(WalDir) of
        {ok, CO} -> CO;
        {error, _} -> bondy_oplog_wal_state:new_consumer_offset()
    end.

%% @private
%% Drains the reader until it returns `end_of_log` or `{error, _}`.
%% On every batch it applies the events and bumps the in-memory
%% consumer offset; consumer.offset and `set_committed_segment` are
%% persisted at `commit_every` events or on `end_of_log`.
%%
%% Before each next-batch read the loop checks the demand-based
%% in-flight counter against `max_install_in_flight`. When the cap is
%% reached the loop returns `{paused, State}` and `handle_info(drain,
%% _)` marks `drain_deferred = true`; the loop is rearmed by the
%% instance's `drain_resume` cast.
drain_loop(#state{} = State0) ->
    case lifecycle_live(State0) of
        false ->
            {paused, State0};
        true ->
            case install_dispatch_allowed(State0) of
                false ->
                    {paused, State0};
                true ->
                    drain_loop_step(State0)
            end
    end.

%% @private
%% Bootstrap lifecycle gate. Returns `true` when the instance is `live`
%% (the applier may drain), `false` when the instance is still
%% `pre_bootstrap` (the applier must NOT touch the per-cell projection).
%% Treats a missing handle as `live` — the registry publish can race
%% with the applier's `init/1` after a one_for_all subtree restart, and
%% the WAL is the durable buffer either way; backwards-compatibility
%% for callers that haven't migrated to the lifecycle yet is the same
%% fail-open path.
lifecycle_live(#state{lifecycle = undefined}) ->
    true;
lifecycle_live(#state{lifecycle = H}) ->
    bondy_oplog_bootstrap_lifecycle:is_live(H).

drain_loop_step(#state{iter = Iter} = State0) ->
    case bondy_oplog_wal_reader:next(Iter) of
        {ok, Batch, _Hlcs, {NextSeg, NextOff}, NewIter} ->
            StateA = apply_batch(State0, Batch),
            {LastHlc, Count} = batch_summary(Batch),
            State1 = bump_offset(
                StateA#state{iter = NewIter},
                NextSeg,
                NextOff,
                LastHlc,
                Count
            ),
            State2 = maybe_commit(State1),
            drain_loop(State2);
        end_of_log ->
            {ok, commit_now(State0)};
        {error, Reason} ->
            ?LOG_ERROR(#{
                description =>
                    "bondy_oplog_applier reader returned an error; "
                    "stopping so the supervisor can restart the subtree "
                    "and recovery can reconcile the on-disk state",
                instance_id => State0#state.instance_id,
                reason => Reason
            }),
            {stop, {reader_error, Reason}, State0}
    end.

%% @private
%% Re-verify each event's stored signature (defence-in-depth against
%% WAL tampering) and dispatch the survivors to the instance via a
%% one-way `gen_server:cast`. The instance installs the events in
%% the MST and evicts the matching overlay rows in FIFO order; the
%% applier does not wait. Events that fail verification are dropped
%% from the batch: their telemetry is emitted here and their overlay
%% rows are evicted directly from the applier process so a reader
%% does not perpetually observe a row whose event the system has
%% rejected. Subsequent applier passes do not retry rejected events
%% (replay-from-beginning would just re-fire the same failure).
apply_batch(
    #state{instance_id = Id, instance_pid = InstancePid} = State, Batch
) ->
    %% Per-stage timing. Five stages emit `duration_us` + `count`
    %% under `[bondy_oplog, applier, batch_<stage>]` so the bench
    %% harness can compute µs/event-spent-in-this-stage and isolate
    %% which sub-path dominates the per-shard throughput floor. See
    %% `_design/latest/APPLIER_PIPELINE_RESIDUAL_PLAN.md` §3.1. The
    %% per-call overhead is ~500ns × 5 stages = ~2.5µs per batch,
    %% well under the <2% threshold for batches with ≥1 event of
    %% real work (pack-store puts are 100-1000µs each).
    BatchSize = length(Batch),
    VerifyT0 = erlang:monotonic_time(microsecond),
    {Verified, Rejected} = verify_batch(State, Batch, [], []),
    telemetry:execute(
        [bondy_oplog, applier, batch_verify],
        #{
            duration_us => erlang:monotonic_time(microsecond) - VerifyT0,
            count => BatchSize
        },
        #{instance_id => Id}
    ),
    RejectedCount = length(Rejected),
    case Rejected of
        [] ->
            ok;
        _ ->
            ok = evict_rejected_overlay(Id, Rejected),
            %% No `install_local_batch` cast will be issued for these
            %% events, but the overlay just shrank — hint the instance
            %% so any caller blocked in `await_apply/1,2` can be
            %% signalled instead of waiting for the next install batch.
            gen_server:cast(InstancePid, check_drain_waiters)
    end,
    VerifiedCount = length(Verified),
    State1 =
        case Verified of
            [] ->
                State;
            _ ->
                %% Order matters for `await_apply/1,2`'s contract:
                %% applier-side projection writes (fold, cell_apply,
                %% publish) run BEFORE the `install_local_batch` cast
                %% is dispatched to the instance. The instance's
                %% handler is the place that signals
                %% `drain_waiters` — by enqueuing the cast last we
                %% guarantee that, by the time a caller's
                %% `await_apply` sees the overlay empty, the
                %% projection adapter, fold state, and `publish_fun`
                %% have all observed the events. The earlier ordering
                %% (cast first, then process in the applier) was a
                %% concurrency micro-optimisation: it overlapped the
                %% applier's projection write with the instance's MST
                %% install. The pipeline still overlaps across
                %% batches (the applier's NEXT batch starts while the
                %% instance is processing this batch's cast), so the
                %% reorder only costs the within-batch overlap, which
                %% is dominated by the projection write anyway.
                {CellEvents, FoldEvents} = partition_by_op(Verified),

                FoldT0 = erlang:monotonic_time(microsecond),
                S1 = apply_fold_batch(State, FoldEvents),
                telemetry:execute(
                    [bondy_oplog, applier, batch_fold],
                    #{
                        duration_us => erlang:monotonic_time(microsecond) -
                            FoldT0,
                        count => length(FoldEvents)
                    },
                    #{instance_id => Id}
                ),

                CellT0 = erlang:monotonic_time(microsecond),
                S2 = apply_cell_batch(S1, CellEvents),
                telemetry:execute(
                    [bondy_oplog, applier, batch_cell_apply],
                    #{
                        duration_us => erlang:monotonic_time(microsecond) -
                            CellT0,
                        count => length(CellEvents)
                    },
                    #{instance_id => Id}
                ),

                PublishT0 = erlang:monotonic_time(microsecond),
                ok = publish_batch(S2, Verified),
                telemetry:execute(
                    [bondy_oplog, applier, batch_publish],
                    #{
                        duration_us => erlang:monotonic_time(microsecond) -
                            PublishT0,
                        count => VerifiedCount
                    },
                    #{instance_id => Id}
                ),

                %% Demand-based dispatch: bump the shared atomic
                %% BEFORE casting. The instance decrements after it
                %% handles the cast, and `drain_loop/1` checks this
                %% counter on its next iteration. The cap is checked
                %% by the loop, not here — `apply_batch/2` always
                %% dispatches the batch it just verified, because
                %% the verification already happened. The next
                %% iteration's check is what gates further reads.
                InstallT0 = erlang:monotonic_time(microsecond),
                ok = reserve_install_slot(State),
                gen_server:cast(
                    InstancePid,
                    {install_local_batch, Verified}
                ),
                telemetry:execute(
                    [bondy_oplog, applier, batch_install_cast],
                    #{
                        duration_us => erlang:monotonic_time(microsecond) -
                            InstallT0,
                        count => VerifiedCount
                    },
                    #{instance_id => Id}
                ),
                S2
        end,
    %% One telemetry event per applier batch, regardless of which
    %% sub-path the events take (fold, cell_apply, publish). This is
    %% the single source of truth for "events the applier has fully
    %% processed end-to-end" — the path-specific
    %% `[bondy_oplog, applier, published]` event only fires for the
    %% `publish_fun`/db_core mirror path.
    telemetry:execute(
        [bondy_oplog, applier, applied],
        #{count => VerifiedCount, rejected => RejectedCount},
        #{instance_id => Id}
    ),
    State1.

%% @private
%% Partitions a verified batch into `{CellApplyEvents, FoldEvents}`.
%% `CellApplyEvents` are events whose op matches
%% `{cell_apply, Bucket, Key, FoldEvent}`; these bypass the per-instance
%% fold and instead drive a projection read-modify-write through
%% `apply_cell_batch/2`. Everything else goes through the existing
%% per-instance fold path.
partition_by_op(Events) ->
    lists:partition(
        fun(E) ->
            case bondy_oplog_event:op(E) of
                {cell_apply, _, _, _} -> true;
                _ -> false
            end
        end,
        Events
    ).

%% @private
%% Folds the verified events into the per-instance projection state.
%% No-op when no fold module is configured. Wraps the fold in a
%% try/catch so a misbehaving fold module cannot wedge the applier —
%% an exception is logged and the state is preserved unchanged
%% (the applier continues to drain the WAL but the projection
%% deviates from the WAL; F9 will track recovery semantics).
apply_fold_batch(#state{fold_module = undefined} = State, _Verified) ->
    State;
apply_fold_batch(State, []) ->
    State;
apply_fold_batch(
    #state{
        fold_module = Mod,
        fold_state = FS0,
        instance_id = Id
    } = State,
    Verified
) ->
    try
        FS1 = lists:foldl(
            fun(Event, Acc) ->
                {NewState, _Delta} = bondy_oplog_fold:apply_event(
                    Mod,
                    Acc,
                    bondy_oplog_event:op(Event),
                    bondy_oplog_event:key(Event)
                ),
                NewState
            end,
            FS0,
            Verified
        ),
        State#state{fold_state = FS1}
    catch
        C:R:S ->
            ?LOG_ERROR(#{
                description =>
                    "bondy_oplog_applier fold raised; the projection "
                    "is now inconsistent with the WAL until the next "
                    "successful batch. Subtree continues to drain.",
                instance_id => Id,
                fold_module => Mod,
                class => C,
                reason => R,
                stacktrace => S
            }),
            State
    end.

%% @private
%% Per-cell projection write path (`MST_DB_DESIGN.md` §6.3). Each event
%% in `CellEvents` carries op `{cell_apply, Bucket, Key, FoldEvent}`. For
%% each, read the cell's current frame from the projection adapter,
%% decode to state via the fold's `decode_state/1`, fold the event in
%% via `apply_event/3`, encode back, and write the new frame via
%% `put_batch/2`. Bucket is a first-class call-time parameter on the
%% projection adapter; the applier passes it through verbatim.
apply_cell_batch(State, []) ->
    State;
apply_cell_batch(#state{cell_apply_ctx = undefined} = State, _Events) ->
    State;
apply_cell_batch(
    #state{
        cell_apply_ctx = Ctx,
        instance_id = Id
    } = State,
    Events
) ->
    #{adapter := Adapter, handle := Handle, fold_module := Fold} = Ctx,
    CacheAdapter = maps:get(cache_adapter, Ctx, undefined),
    CacheHandle = maps:get(cache_handle, Ctx, undefined),
    HighWaterRef = maps:get(high_water_ref, Ctx, undefined),

    %% PR-PS-15b: collect all per-event writes into a single
    %% `Adapter:put_batch/2` call.
    %%
    %% Correctness: when two events in the batch target the same
    %% `{Bucket, Key}`, the second must observe the first's write.
    %% We thread a `LocalWrites :: #{{Bucket, Key} => Frame}` shadow
    %% through the fold so the per-event read path checks the local
    %% map before falling back to `Adapter:get/3`. After the fold,
    %% we issue ONE `put_batch` with the deduped {Bucket, Key, Frame}
    %% list (last write wins per key — consistent with the previous
    %% sequential-per-key semantics).
    {LocalWrites, MaxHlc} = lists:foldl(
        fun(Event, {WAcc, HlcAcc}) ->
            case bondy_oplog_event:op(Event) of
                {cell_apply, Bucket, Key, FoldEvent} ->
                    Meta = bondy_oplog_event:key(Event),
                    case
                        compute_one_cell(
                            Id,
                            Adapter,
                            Handle,
                            Fold,
                            WAcc,
                            Bucket,
                            Key,
                            FoldEvent,
                            Meta
                        )
                    of
                        {ok, NewFrame, NewHlc} ->
                            WAcc1 = WAcc#{{Bucket, Key} => NewFrame},
                            {WAcc1, max_hlc(HlcAcc, NewHlc)};
                        skip ->
                            {WAcc, HlcAcc}
                    end;
                _ ->
                    {WAcc, HlcAcc}
            end
        end,
        {#{}, undefined},
        Events
    ),

    case map_size(LocalWrites) of
        0 ->
            ok;
        _ ->
            PutT0 = erlang:monotonic_time(microsecond),
            Entries = [{B, K, F} || {{B, K}, F} <- maps:to_list(LocalWrites)],
            PutResult = Adapter:put_batch(Handle, Entries),
            telemetry:execute(
                [bondy_oplog, applier, batch_cell_put],
                #{
                    duration_us => erlang:monotonic_time(microsecond) - PutT0,
                    count => length(Entries)
                },
                #{instance_id => Id}
            ),
            case PutResult of
                ok ->
                    %% Cache invalidate per unique key (dedup via
                    %% the LocalWrites map's key set, not the Inval
                    %% list which may have duplicates).
                    maps:foreach(
                        fun({B, K}, _F) ->
                            invalidate_cache(CacheAdapter, CacheHandle, B, K)
                        end,
                        LocalWrites
                    ),
                    case MaxHlc of
                        undefined -> ok;
                        _ -> advance_high_water(HighWaterRef, MaxHlc)
                    end;
                {error, Reason} ->
                    ?LOG_WARNING(#{
                        description =>
                            "bondy_oplog_applier projection batch write "
                            "failed; the cells will be re-applied on the "
                            "next replay of these events",
                        instance_id => Id,
                        count => map_size(LocalWrites),
                        reason => Reason
                    }),
                    ok
            end
    end,
    State.

%% @private
%% Per-event compute (read + apply + encode). Returns the new frame +
%% HLC to the batch caller, which collects and writes them all at once.
%%
%% Reads first consult `LocalWrites` so in-batch updates to the same
%% `{Bucket, Key}` see each other (the substrate has not been written
%% yet at this point). Then falls back to `Adapter:get/3`.
%%
%% Per-event telemetry boundaries `cell_read` + `cell_apply_event`
%% remain (each cell still pays the read + compute cost). The
%% PR-PS-15a `cell_put` and `cell_side_effects` events are GONE in
%% PR-PS-15b — the put + side-effects now happen once per batch and
%% are measured by `batch_cell_put` in `apply_cell_batch/2`.
compute_one_cell(
    Id,
    Adapter,
    Handle,
    Fold,
    LocalWrites,
    Bucket,
    Key,
    FoldEvent,
    Meta
) ->
    try
        ReadT0 = erlang:monotonic_time(microsecond),
        {OldState, OldValueOpt} =
            case maps:get({Bucket, Key}, LocalWrites, undefined) of
                undefined ->
                    case Adapter:get(Handle, Bucket, Key) of
                        not_found ->
                            {bondy_oplog_fold:initial_value(Fold), undefined};
                        {ok, OldFrame} ->
                            {_PrevHlc, OldStateBytes, OldValueBytes} =
                                bondy_oplog_cell_frame:decode_full(OldFrame),
                            {
                                bondy_oplog_fold:decode_state(
                                    Fold, OldStateBytes
                                ),
                                OldValueBytes
                            }
                    end;
                LocalFrame ->
                    {_PrevHlc, LStateBytes, LValueBytes} =
                        bondy_oplog_cell_frame:decode_full(LocalFrame),
                    {
                        bondy_oplog_fold:decode_state(Fold, LStateBytes),
                        LValueBytes
                    }
            end,
        telemetry:execute(
            [bondy_oplog, applier, cell_read],
            #{duration_us => erlang:monotonic_time(microsecond) - ReadT0},
            #{instance_id => Id}
        ),

        ApplyT0 = erlang:monotonic_time(microsecond),
        {NewState, Delta} =
            bondy_oplog_fold:apply_event(Fold, OldState, FoldEvent, Meta),
        Hlc = bondy_oplog_fold:hlc(Fold, NewState),
        NewStateBytes = bondy_oplog_fold:encode_state(Fold, NewState),
        NewValueBytes = compose_value_bytes(Fold, OldValueOpt, Delta),
        NewFrame = bondy_oplog_cell_frame:encode(
            Hlc,
            NewStateBytes,
            NewValueBytes,
            bondy_oplog_fold:value_equals_state(Fold)
        ),
        telemetry:execute(
            [bondy_oplog, applier, cell_apply_event],
            #{duration_us => erlang:monotonic_time(microsecond) - ApplyT0},
            #{instance_id => Id}
        ),
        {ok, NewFrame, Hlc}
    catch
        C:R:S ->
            ?LOG_ERROR(#{
                description =>
                    "bondy_oplog_applier cell_apply raised; the cell "
                    "has been skipped. Batch continues with remaining cells.",
                instance_id => Id,
                bucket => Bucket,
                cell_key => Key,
                fold_module => Fold,
                class => C,
                reason => R,
                stacktrace => S
            }),
            skip
    end.

%% @private
%% Tracks the maximum HLC seen across a batch so the per-shard
%% high-water mark can be advanced once at the end instead of once
%% per cell event.
max_hlc(undefined, Hlc) -> Hlc;
max_hlc(A, B) when A >= B -> A;
max_hlc(_, B) -> B.

%% @private
%% Re-fold the `cell_apply` events that landed in the MST since the
%% last replay through `apply_one_cell/11`. Called from the instance
%% after a sync session merges peer events. Without this, remote events
%% sit in the MST but never reach the projection — `bondy_db:read/3`
%% would only see events authored locally.
%%
%% The walk is incremental: `bondy_mst:diff_to_list/3` prunes subtrees
%% whose root hash is shared between the current MST and
%% `last_replayed_root`, so the cost is O(events since last sync) rather
%% than O(events in MST). A cold start (`last_replayed_root = undefined`)
%% does one full fold so any peer-authored events present in the MST at
%% boot time are observed; subsequent replays use the diff.
do_replay_cell_events(#state{cell_apply_ctx = undefined} = State) ->
    State;
do_replay_cell_events(
    #state{
        cell_apply_ctx = Ctx,
        instance_id = Id,
        last_replayed_root = LastRoot
    } = State
) ->
    case bondy_oplog_registry:mst(Id) of
        undefined ->
            State;
        MST ->
            CurrentRoot = bondy_mst:root(MST),
            case CurrentRoot of
                LastRoot ->
                    telemetry:execute(
                        [bondy_oplog, applier, replay_cell_events],
                        #{cells_applied => 0, pairs => 0},
                        #{
                            instance_id => Id,
                            outcome => no_change,
                            incremental => LastRoot =/= undefined
                        }
                    ),
                    State;
                _ ->
                    Pairs = diff_pairs(MST, LastRoot, Id),
                    Count = apply_cell_pairs(Ctx, Id, Pairs),
                    ?LOG_DEBUG(#{
                        description => "replay_cell_events done",
                        instance_id => Id,
                        cells_applied => Count,
                        incremental => LastRoot =/= undefined
                    }),
                    telemetry:execute(
                        [bondy_oplog, applier, replay_cell_events],
                        #{cells_applied => Count, pairs => length(Pairs)},
                        #{
                            instance_id => Id,
                            outcome => applied,
                            incremental => LastRoot =/= undefined
                        }
                    ),
                    State#state{last_replayed_root = CurrentRoot}
            end
    end.

%% @private
%% Returns the `[{Key, Value}]` list to re-apply. Falls back to a full
%% `to_list/1` if the diff raises — for example, if `LastRoot`'s pages
%% have been partially GC'd between two replays. The applier never
%% silently misses events: a failed diff costs one extra full fold.
diff_pairs(MST, undefined, _Id) ->
    bondy_mst:to_list(MST);
diff_pairs(MST, LastRoot, Id) ->
    try
        bondy_mst:diff_to_list(MST, LastRoot)
    catch
        C:R:S ->
            ?LOG_WARNING(#{
                description =>
                    "bondy_mst:diff_to_list raised; falling back to "
                    "full MST scan for this replay",
                instance_id => Id,
                last_root => LastRoot,
                class => C,
                reason => R,
                stacktrace => S
            }),
            bondy_mst:to_list(MST)
    end.

%% @private
%% Walks the `{Key, Value}` pairs from the MST (or its diff) and
%% dispatches every `cell_apply` op through the batched compute path.
%% Non-cell ops are skipped here — the per-instance fold owns them and
%% has already seen them via the WAL drain.
%%
%% PR-PS-15b: same collect-then-batch shape as `apply_cell_batch/2`.
%% Per-key shadow map preserves in-batch read-your-own-writes when
%% two pairs target the same `{Bucket, Key}`.
apply_cell_pairs(Ctx, Id, Pairs) ->
    #{adapter := Adapter, handle := Handle, fold_module := Fold} = Ctx,
    CacheAdapter = maps:get(cache_adapter, Ctx, undefined),
    CacheHandle = maps:get(cache_handle, Ctx, undefined),
    HighWaterRef = maps:get(high_water_ref, Ctx, undefined),
    try
        {LocalWrites, MaxHlc, N} = lists:foldl(
            fun
                (
                    {MstKey, {
                        {cell_apply, Bucket, CellKey, FoldEvent},
                        _Meta,
                        _Prev,
                        _Sig
                    }},
                    {WAcc, HlcAcc, NAcc}
                ) ->
                    case
                        compute_one_cell(
                            Id,
                            Adapter,
                            Handle,
                            Fold,
                            WAcc,
                            Bucket,
                            CellKey,
                            FoldEvent,
                            MstKey
                        )
                    of
                        {ok, NewFrame, NewHlc} ->
                            WAcc1 = WAcc#{{Bucket, CellKey} => NewFrame},
                            {WAcc1, max_hlc(HlcAcc, NewHlc), NAcc + 1};
                        skip ->
                            {WAcc, HlcAcc, NAcc}
                    end;
                (_, Acc) ->
                    Acc
            end,
            {#{}, undefined, 0},
            Pairs
        ),
        case map_size(LocalWrites) of
            0 ->
                ok;
            _ ->
                Entries = [
                    {B, K, F}
                 || {{B, K}, F} <- maps:to_list(LocalWrites)
                ],
                case Adapter:put_batch(Handle, Entries) of
                    ok ->
                        maps:foreach(
                            fun({B, K}, _F) ->
                                invalidate_cache(
                                    CacheAdapter, CacheHandle, B, K
                                )
                            end,
                            LocalWrites
                        ),
                        case MaxHlc of
                            undefined -> ok;
                            _ -> advance_high_water(HighWaterRef, MaxHlc)
                        end;
                    {error, Reason} ->
                        ?LOG_WARNING(#{
                            description =>
                                "bondy_oplog_applier replay batch write "
                                "failed; the cells will be re-applied on "
                                "the next sync tick",
                            instance_id => Id,
                            count => map_size(LocalWrites),
                            reason => Reason
                        })
                end
        end,
        N
    catch
        C:R:S ->
            ?LOG_WARNING(#{
                description =>
                    "bondy_oplog_applier replay_cell_events raised; "
                    "the projection may be temporarily stale on this "
                    "node — the next sync tick re-issues the replay.",
                instance_id => Id,
                class => C,
                reason => R,
                stacktrace => S
            }),
            0
    end.

%% @private
%% Encode the value bytes column for the V2 cell frame
%% (`bondy_oplog_cell_frame:encode/4`).
%%
%% For folds that declare `value_equals_state/0 -> true` the substrate
%% omits the value column and reuses the state bytes; we return
%% `undefined` here so the encoder sets `HasValueColumn = 0`.
%%
%% Otherwise we honour the op-based delta the fold emitted from
%% `apply_event/3`:
%%
%%  - `Delta =:= none` — the event did not change the value (dedup,
%%    no-op, monotone-rejected). The cell's value column keeps the
%%    prior bytes (or, on cold-start, the initial value's bytes).
%%  - `Delta` of any other shape — the substrate calls
%%    `apply_value_delta(Fold, OldValue, Delta)` to combine into the
%%    new value, then encodes it.
%%
%% `OldValueOpt` is `undefined` only when there was no prior cell
%% frame (cold-start on `not_found`); the substrate seeds OldValue
%% from `to_value(initial_value(Fold))` in that case.
compose_value_bytes(Fold, OldValueOpt, Delta) ->
    case bondy_oplog_fold:value_equals_state(Fold) of
        true ->
            undefined;
        false ->
            OldValue = decode_old_value(Fold, OldValueOpt),
            NewValue =
                case Delta of
                    none ->
                        OldValue;
                    _ ->
                        bondy_oplog_fold:apply_value_delta(
                            Fold, OldValue, Delta
                        )
                end,
            term_to_binary(NewValue)
    end.

decode_old_value(Fold, undefined) ->
    bondy_oplog_fold:to_value(Fold, bondy_oplog_fold:initial_value(Fold));
decode_old_value(_Fold, Bytes) when is_binary(Bytes) ->
    binary_to_term(Bytes).

%% @private
%% Evict the (Bucket, Key) entry from the per-shard read cache so the
%% next `bondy_db_core:read/4` re-reads from the projection. Without
%% this, `bondy_db:apply/4` followed by a `read/3` from a different
%% process returns stale state — the cache adapter is populate-on-miss
%% and has no other invalidation path.
%%
%% A `delete` is preferred over a `put` because (a) we cannot
%% reconstruct the cache value here (it is `{Value, Hlc}` where Value
%% is the *decoded* fold state, but the cache adapter stores it
%% post-overlay-merge — the applier has no overlay context) and
%% (b) the next reader's `slow_read_traced/3` will repopulate the
%% cache anyway.
invalidate_cache(undefined, _Handle, _Bucket, _Key) ->
    ok;
invalidate_cache(_Adapter, undefined, _Bucket, _Key) ->
    ok;
invalidate_cache(Adapter, Handle, Bucket, Key) ->
    %% `delete/3` is the cache_adapter callback. Swallow any errors —
    %% a failed cache eviction must not stop the drain.
    _ = catch Adapter:delete(Handle, Bucket, Key),
    ok.

%% @private
%% Advance the per-shard high-water HLC mark
%% (`bondy_oplog_high_water:advance/2`) after a successful projection
%% write. The ref may be `undefined` defensively (older
%% `bondy_db_core_registry` entries created before PR-D1 §3); new
%% registrations always allocate, so this branch is dead in practice
%% but keeps the applier resilient to a partial rollback.
advance_high_water(undefined, _Hlc) ->
    ok;
advance_high_water(Ref, Hlc) ->
    bondy_oplog_high_water:advance(Ref, Hlc).

%% @private
%% Installs a catalogue-snapshot batch of `[{Bucket, Key, Frame}]`
%% triples into the projection.
%%
%% `replace` mode: for fresh bootstrap. Skip-if-older guards a stale
%% bootstrap write from clobbering a newer locally-applied event (see
%% Q11, `_design/catalogue_expansion_plan.md` §4.12).
%%
%% `merge` mode: for recovering bootstrap. Calls the fold's
%% `merge_states/2` on the incoming + local state and writes the
%% merged frame. Folds without `merge_states/2` (only `presence_basic`
%% in the shipped catalogue) emit telemetry and fall back to
%% skip-if-older replacement.
do_install_catalogue_batch(Id, Ctx, Mode, Cells) ->
    #{
        adapter := Adapter,
        handle := Handle,
        cache_adapter := CacheAdapter,
        cache_handle := CacheHandle,
        high_water_ref := HighWaterRef,
        fold_module := Fold
    } = Ctx,
    Counts = lists:foldl(
        fun(Cell, Acc) ->
            install_one_cell(
                Id,
                Mode,
                Fold,
                Adapter,
                Handle,
                CacheAdapter,
                CacheHandle,
                HighWaterRef,
                Cell,
                Acc
            )
        end,
        #{
            installed => 0,
            skipped => 0,
            merged => 0,
            replaced_no_merge => 0
        },
        Cells
    ),
    {ok, Counts}.

%% @private
install_one_cell(
    Id,
    Mode,
    Fold,
    Adapter,
    Handle,
    CacheAdapter,
    CacheHandle,
    HighWaterRef,
    {Bucket, Key, Frame},
    Acc
) ->
    try bondy_oplog_cell_frame:decode_full(Frame) of
        {IncomingHlc, IncomingStateBytes, _IncomingValueBytes} ->
            Existing = read_existing_for_install(
                Mode, Adapter, Handle, Bucket, Key
            ),
            handle_cell(
                Id,
                Mode,
                Fold,
                Adapter,
                Handle,
                CacheAdapter,
                CacheHandle,
                HighWaterRef,
                Bucket,
                Key,
                Frame,
                IncomingHlc,
                IncomingStateBytes,
                Existing,
                Acc
            )
    catch
        C:R:St ->
            ?LOG_WARNING(#{
                description =>
                    "install_catalogue_batch: cell skipped due to "
                    "decode error",
                instance_id => Id,
                bucket => Bucket,
                cell_key => Key,
                class => C,
                reason => R,
                stacktrace => St
            }),
            bump(skipped, Acc)
    end.

%% @private
%% Returns one of:
%%   not_found
%% | {ok, ExistingHlc, ExistingStateBytes | undefined}
%%
%% In `replace` mode only the HLC is needed for the skip-if-older
%% check, so we use the adapter's optional `head/3` callback when
%% available and avoid pulling the full V2 frame off the journal.
%% In `merge` mode the local state bytes are needed by the fold's
%% `merge_states/2`, so we always pay for a full `get/3`.
read_existing_for_install(replace, Adapter, Handle, Bucket, Key) ->
    case adapter_head_hlc(Adapter, Handle, Bucket, Key) of
        not_found ->
            not_found;
        {ok, ExistingHlc} ->
            {ok, ExistingHlc, undefined}
    end;
read_existing_for_install(merge, Adapter, Handle, Bucket, Key) ->
    case Adapter:get(Handle, Bucket, Key) of
        not_found ->
            not_found;
        {ok, ExistingFrame} ->
            {ExistingHlc, ExistingStateBytes, _ExistingValueBytes} =
                bondy_oplog_cell_frame:decode_full(ExistingFrame),
            {ok, ExistingHlc, ExistingStateBytes}
    end.

%% @private
%% HLC-only read against the projection adapter. Uses the optional
%% `head/3` callback when the adapter exports it; otherwise falls
%% back to `get/3 + decode_full/1`.
adapter_head_hlc(Adapter, Handle, Bucket, Key) ->
    case erlang:function_exported(Adapter, head, 3) of
        true ->
            case Adapter:head(Handle, Bucket, Key) of
                not_found ->
                    not_found;
                {ok, HeadBytes} ->
                    {Hlc, _ValueBytes} =
                        bondy_oplog_cell_frame:decode_head(HeadBytes),
                    {ok, Hlc}
            end;
        false ->
            case Adapter:get(Handle, Bucket, Key) of
                not_found ->
                    not_found;
                {ok, Frame} ->
                    {Hlc, _StateBytes, _ValueBytes} =
                        bondy_oplog_cell_frame:decode_full(Frame),
                    {ok, Hlc}
            end
    end.

%% @private
handle_cell(
    _Id,
    _Mode,
    _Fold,
    Adapter,
    Handle,
    CacheAdapter,
    CacheHandle,
    HighWaterRef,
    Bucket,
    Key,
    Frame,
    IncomingHlc,
    _IncomingStateBytes,
    not_found,
    Acc
) ->
    %% No local cell — install verbatim under both modes.
    install_cell_unchecked(
        Adapter,
        Handle,
        CacheAdapter,
        CacheHandle,
        HighWaterRef,
        Bucket,
        Key,
        Frame,
        IncomingHlc
    ),
    bump(installed, Acc);
handle_cell(
    Id,
    replace,
    _Fold,
    Adapter,
    Handle,
    CacheAdapter,
    CacheHandle,
    HighWaterRef,
    Bucket,
    Key,
    Frame,
    IncomingHlc,
    _IncomingStateBytes,
    {ok, ExistingHlc, _ExistingStateBytes},
    Acc
) ->
    case IncomingHlc > ExistingHlc of
        true ->
            install_cell_unchecked(
                Adapter,
                Handle,
                CacheAdapter,
                CacheHandle,
                HighWaterRef,
                Bucket,
                Key,
                Frame,
                IncomingHlc
            ),
            bump(installed, Acc);
        false ->
            telemetry:execute(
                [bondy_oplog, applier, catalogue_bootstrap, cell_skipped],
                #{count => 1},
                #{
                    instance_id => Id,
                    bucket => Bucket,
                    cell_key => Key,
                    incoming_hlc => IncomingHlc,
                    existing_hlc => ExistingHlc
                }
            ),
            bump(skipped, Acc)
    end;
handle_cell(
    Id,
    merge,
    Fold,
    Adapter,
    Handle,
    CacheAdapter,
    CacheHandle,
    HighWaterRef,
    Bucket,
    Key,
    _Frame,
    IncomingHlc,
    IncomingStateBytes,
    {ok, ExistingHlc, ExistingStateBytes},
    Acc
) ->
    try
        IncomingState = bondy_oplog_fold:decode_state(Fold, IncomingStateBytes),
        ExistingState = bondy_oplog_fold:decode_state(Fold, ExistingStateBytes),
        MergedState = bondy_oplog_fold:merge_states(
            Fold, IncomingState, ExistingState
        ),
        MergedHlc = bondy_oplog_fold:hlc(Fold, MergedState),
        MergedStateBytes = bondy_oplog_fold:encode_state(Fold, MergedState),
        MergedValueBytes = compose_merged_value_bytes(
            Fold, MergedState, MergedStateBytes
        ),
        MergedFrame = bondy_oplog_cell_frame:encode(
            MergedHlc,
            MergedStateBytes,
            MergedValueBytes,
            bondy_oplog_fold:value_equals_state(Fold)
        ),
        install_cell_unchecked(
            Adapter,
            Handle,
            CacheAdapter,
            CacheHandle,
            HighWaterRef,
            Bucket,
            Key,
            MergedFrame,
            MergedHlc
        ),
        bump(merged, Acc)
    catch
        error:{merge_states_not_supported, _} ->
            telemetry:execute(
                [
                    bondy_oplog,
                    applier,
                    catalogue_bootstrap,
                    presence_basic_replaced
                ],
                #{count => 1},
                #{
                    instance_id => Id,
                    bucket => Bucket,
                    cell_key => Key,
                    fold_module => Fold
                }
            ),
            ?LOG_WARNING(#{
                description =>
                    "merge-mode catalogue bootstrap encountered a "
                    "fold without merge_states/2; falling back to "
                    "skip-if-older replacement",
                instance_id => Id,
                bucket => Bucket,
                cell_key => Key,
                fold_module => Fold
            }),
            handle_cell(
                Id,
                replace,
                Fold,
                Adapter,
                Handle,
                CacheAdapter,
                CacheHandle,
                HighWaterRef,
                Bucket,
                Key,
                _Frame,
                IncomingHlc,
                IncomingStateBytes,
                {ok, ExistingHlc, ExistingStateBytes},
                bump(replaced_no_merge, Acc)
            );
        C:R:St ->
            ?LOG_WARNING(#{
                description =>
                    "install_catalogue_batch: merge raised; cell skipped",
                instance_id => Id,
                bucket => Bucket,
                cell_key => Key,
                class => C,
                reason => R,
                stacktrace => St
            }),
            bump(skipped, Acc)
    end.

%% @private
%% Rebuild the value column for the merged state. `value_equals_state`
%% folds (G-Set) keep `undefined` (cell-frame elides the column);
%% others encode `to_value(MergedState)`.
compose_merged_value_bytes(Fold, MergedState, _MergedStateBytes) ->
    case bondy_oplog_fold:value_equals_state(Fold) of
        true ->
            undefined;
        false ->
            term_to_binary(bondy_oplog_fold:to_value(Fold, MergedState))
    end.

%% @private
bump(Key, Acc) ->
    maps:update_with(Key, fun(X) -> X + 1 end, Acc).

%% @private
install_cell_unchecked(
    Adapter,
    Handle,
    CacheAdapter,
    CacheHandle,
    HighWaterRef,
    Bucket,
    Key,
    Frame,
    Hlc
) ->
    case Adapter:put_batch(Handle, [{Bucket, Key, Frame}]) of
        ok ->
            invalidate_cache(CacheAdapter, CacheHandle, Bucket, Key),
            advance_high_water(HighWaterRef, Hlc),
            ok;
        {error, Reason} ->
            ?LOG_WARNING(#{
                description =>
                    "install_catalogue_batch: projection write failed",
                bucket => Bucket,
                cell_key => Key,
                reason => Reason
            }),
            ok
    end.

%% @private
%% Folds the batch in order, partitioning into verified events and
%% rejected ones. Verified order is preserved (the cast handler
%% relies on HLC-monotonic order within a batch).
verify_batch(_State, [], VAcc, RAcc) ->
    {lists:reverse(VAcc), lists:reverse(RAcc)};
verify_batch(#state{} = State, [Event | Rest], VAcc, RAcc) ->
    case verify_event(State, Event) of
        ok ->
            verify_batch(State, Rest, [Event | VAcc], RAcc);
        {error, Reason} ->
            ok = log_verify_failure(State#state.instance_id, Event, Reason),
            verify_batch(State, Rest, VAcc, [Event | RAcc])
    end.

%% @private
%% Removes overlay rows for events the applier refused to install.
%% Uses the registry to find the overlay tid and an `ets:select_delete/2`
%% with an HLC-conditional guard so a concurrent retry of the same key
%% with a higher HLC is preserved.
evict_rejected_overlay(InstanceId, Events) ->
    case bondy_oplog_registry:overlay_tab(InstanceId) of
        undefined ->
            ok;
        Tab ->
            lists:foreach(
                fun(Event) ->
                    Key = bondy_oplog_event:key(Event),
                    Hlc = bondy_oplog_event:key_hlc(Key),
                    _ =
                        try
                            ets:select_delete(Tab, [
                                {
                                    {Key, '_', '$1', '_'},
                                    [{'=<', '$1', Hlc}],
                                    [true]
                                }
                            ])
                        catch
                            error:badarg -> 0
                        end
                end,
                Events
            ),
            ok
    end.

%% @private
verify_event(#state{validator_module = Mod, validator_state = VS}, Event) ->
    Mod:verify_event(Event, VS).

%% @private
%% Refreshes the applier's snapshot of the validator state by calling
%% the optional `Mod:refresh/1` callback. The new snapshot is only
%% installed on `{ok, NewState}`; on any other return value (or on a
%% raise) the old snapshot is preserved so a misbehaving validator
%% cannot wedge the applier. In-flight `enqueue_remote` workers
%% captured the old snapshot before this cast was processed and
%% continue to use it — there is no mid-flight swap.
do_refresh_validator(
    Reason,
    #state{
        instance_id = Id,
        validator_module = Mod,
        validator_state = VS
    } = State
) ->
    case erlang:function_exported(Mod, refresh, 1) of
        false ->
            ?LOG_DEBUG(#{
                description =>
                    "bondy_oplog_applier ignored a refresh_validator "
                    "request because the validator module does not "
                    "export refresh/1",
                instance_id => Id,
                validator => Mod,
                refresh_reason => Reason
            }),
            telemetry:execute(
                [bondy_oplog, applier, validator_refresh],
                #{count => 1},
                #{
                    instance_id => Id,
                    validator => Mod,
                    outcome => unsupported,
                    refresh_reason => Reason
                }
            ),
            State;
        true ->
            try Mod:refresh(VS) of
                {ok, NewVS} ->
                    ?LOG_INFO(#{
                        description =>
                            "bondy_oplog_applier refreshed validator "
                            "snapshot",
                        instance_id => Id,
                        validator => Mod,
                        refresh_reason => Reason
                    }),
                    telemetry:execute(
                        [bondy_oplog, applier, validator_refresh],
                        #{count => 1},
                        #{
                            instance_id => Id,
                            validator => Mod,
                            outcome => ok,
                            refresh_reason => Reason
                        }
                    ),
                    State#state{validator_state = NewVS};
                {error, RefreshReason} ->
                    ?LOG_WARNING(#{
                        description =>
                            "bondy_oplog_applier validator refresh "
                            "returned an error; keeping the previous "
                            "snapshot",
                        instance_id => Id,
                        validator => Mod,
                        refresh_reason => Reason,
                        reason => RefreshReason
                    }),
                    telemetry:execute(
                        [bondy_oplog, applier, validator_refresh],
                        #{count => 1},
                        #{
                            instance_id => Id,
                            validator => Mod,
                            outcome => error,
                            refresh_reason => Reason,
                            error => RefreshReason
                        }
                    ),
                    State
            catch
                C:R:S ->
                    ?LOG_ERROR(#{
                        description =>
                            "bondy_oplog_applier validator refresh "
                            "raised; keeping the previous snapshot",
                        instance_id => Id,
                        validator => Mod,
                        refresh_reason => Reason,
                        class => C,
                        reason => R,
                        stacktrace => S
                    }),
                    telemetry:execute(
                        [bondy_oplog, applier, validator_refresh],
                        #{count => 1},
                        #{
                            instance_id => Id,
                            validator => Mod,
                            outcome => crashed,
                            refresh_reason => Reason,
                            class => C,
                            error => R
                        }
                    ),
                    State
            end
    end.

%% @private
%% Forwards a verified remote event to the instance for install. The
%% instance still owns origin-ban / backpressure / watermark filtering
%% and the equivocation check, so its reply is what the caller sees.
%% A `noproc` race during subtree restart is surfaced as
%% `{error, instance_unavailable}` so the caller (a sync session) can
%% retry instead of treating the event as accepted.
forward_remote(InstancePid, Event) ->
    try gen_server:call(InstancePid, {install_remote, Event}, infinity) of
        Reply -> Reply
    catch
        exit:{noproc, _} -> {error, instance_unavailable};
        exit:noproc -> {error, instance_unavailable};
        exit:{normal, _} -> {error, instance_unavailable};
        exit:{shutdown, _} -> {error, instance_unavailable}
    end.

%% @private
log_verify_failure(Id, Event, Reason) when is_binary(Id) ->
    Key = bondy_oplog_event:key(Event),
    ?LOG_WARNING(#{
        description =>
            "bondy_oplog_applier dropped an event whose stored "
            "signature does not verify; the event has been skipped "
            "to keep the subtree alive",
        instance_id => Id,
        key => Key,
        reason => Reason
    }),
    telemetry:execute(
        [bondy_oplog, applier, verify_failed],
        #{count => 1},
        #{instance_id => Id}
    ),
    ok.

%% @private
batch_summary(Batch) ->
    LastEvent = lists:last(Batch),
    LastHlc = bondy_oplog_event:key_hlc(
        bondy_oplog_event:key(LastEvent)
    ),
    {LastHlc, length(Batch)}.

%% @private
bump_offset(
    #state{consumer_offset = CO0, uncommitted = U} = State,
    Seg,
    Off,
    LastHlc,
    Count
) ->
    CO1 = bondy_oplog_wal_state:with_position(CO0, Seg, Off),
    CO2 = bondy_oplog_wal_state:with_hlc(CO1, LastHlc),
    Old = bondy_oplog_wal_state:commit_count(CO2),
    CO3 = bondy_oplog_wal_state:with_commit_count(CO2, Old + 1),
    State#state{consumer_offset = CO3, uncommitted = U + Count}.

%% @private
maybe_commit(#state{uncommitted = U, commit_every = N} = State) when
    U >= N
->
    commit_now(State);
maybe_commit(State) ->
    State.

%% @private
commit_now(#state{uncommitted = 0} = State) ->
    State;
commit_now(
    #state{
        instance_id = InstanceId,
        instance_pid = InstancePid,
        wal_dir = Dir,
        wal_pid = WalPid,
        consumer_offset = CO
    } = State
) ->
    %% Drain barrier: block until the instance has processed every
    %% `install_local_batch` cast we issued before this commit. The
    %% FIFO mailbox ordering of casts and the synchronous call
    %% together guarantee that, when the call returns, all events
    %% whose keys we're about to commit have been installed in the
    %% MST. Without this barrier, `notify_committed_segment` could
    %% drop a WAL segment whose events the instance has not yet
    %% applied — a hard durability hole on a co-crash.
    ok = drain_install_queue(InstancePid),
    %% NOTE: `last_replayed_root` is NOT advanced here even though
    %% `drain_install_queue/1` proves every local install has been
    %% applied to the MST. Reason: a peer sync's
    %% `integrate_peer_root/2` can interleave with the WAL drain and
    %% land remote pages in the MST under the same root that this
    %% barrier returns. Those remote events flow through the
    %% `replay_cell_events` cast — not through `apply_cell_batch/2` —
    %% so the projection has *not* seen them yet. Advancing the
    %% watermark to the live root here would mark them as already
    %% replayed, and `do_replay_cell_events/1` would short-circuit
    %% before folding them. Empirically (Jepsen OR-set,
    %% random-partition-halves): doing so produces 27/226 lost adds.
    %% Leaving the watermark anchored at its previous value keeps the
    %% next `do_replay_cell_events/1` honest — it sees a diff that
    %% includes both the locally-installed events and any
    %% interleaving peer events. Local events are re-folded
    %% idempotently (CRDT contract); the cost is one extra RMW per
    %% local event per sync tick, dominated by the sync round-trip
    %% itself.
    case bondy_oplog_wal_state:write_consumer_offset(Dir, CO) of
        ok ->
            Seg = bondy_oplog_wal_state:committed_segment(CO),
            ok = notify_committed_segment(InstanceId, WalPid, Seg),
            ok = bump_ae_targets(State),
            State#state{uncommitted = 0};
        {error, Reason} ->
            ?LOG_WARNING(#{
                description =>
                    "bondy_oplog_applier could not persist consumer.offset; "
                    "retrying on the next commit boundary",
                instance_id => InstanceId,
                reason => Reason
            }),
            %% Keep uncommitted > 0 so the next commit boundary retries.
            State
    end.

%% @private
%% Synchronous barrier — `gen_server:call` jumps the instance mailbox
%% to the back of the queue, so every prior cast (the `install_local_batch`
%% messages from this drain pass) has been fully handled by the time
%% this call returns. A `noproc` race during subtree shutdown is
%% treated as a "no events to wait on" and tolerated.
drain_install_queue(InstancePid) ->
    try gen_server:call(InstancePid, drain_install_queue, infinity) of
        ok -> ok
    catch
        exit:{noproc, _} -> ok;
        exit:noproc -> ok;
        exit:{normal, _} -> ok;
        exit:{shutdown, _} -> ok
    end.

%% @private
%% Tells the WAL writer to advance its committed-segment marker so the
%% retention sweep can drop fully-applied segments. A narrow `noproc`
%% catch covers the benign supervisor-shutdown race where the WAL has
%% already exited; any other error is logged so it doesn't get
%% swallowed silently.
notify_committed_segment(InstanceId, WalPid, Seg) ->
    try bondy_oplog_wal:set_committed_segment(WalPid, Seg) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_WARNING(#{
                description =>
                    "bondy_oplog_wal refused a set_committed_segment "
                    "request; retention sweep may lag until the next "
                    "commit boundary",
                instance_id => InstanceId,
                segment => Seg,
                reason => Reason
            }),
            ok
    catch
        exit:{noproc, _} -> ok;
        exit:noproc -> ok
    end.

%% @private
%% Park an async waiter on the WAL's durable position. Spawns a
%% monitored helper that blocks in `bondy_oplog_wal:await_durable/3`
%% until the durable position advances *strictly past* the reader's
%% current offset (i.e. a new frame becomes durable) or
%% `?AWAIT_DURABLE_TIMEOUT_MS` elapses, then exits. The helper's
%% monitor `DOWN` is the applier's wakeup signal (see
%% `handle_info({'DOWN', ...})`).
%%
%% Why a helper rather than calling `await_durable/3` inline:
%% `await_durable/3` is a blocking `gen_server:call`. Calling it from
%% the applier's own `handle_info(drain)` would make the applier
%% unresponsive to every other message — notably the
%% `replay_cell_events` cast that cross-node sync uses to fold synced
%% events into the projection — for the duration of the wait. The
%% helper isolates the block; the applier returns immediately and its
%% mailbox keeps flowing.
%%
%% Why `{Seg, Off + 1}` and not `{Seg, Off}`: the reader's current
%% position is the next-to-read byte, which is already durable whenever
%% we are caught up (always so in `per_write` mode, where head ≡
%% durable). Awaiting `{Seg, Off}` is satisfied instantly and the
%% helper would exit immediately, spinning. `{Seg, Off + 1}` waits for
%% genuinely new data. A segment rollover satisfies it too, since
%% `{Seg, Off + 1} =< {Seg + 1, _}`.
arm_idle_waiter(#state{idle_waiter = Ref} = State) when is_reference(Ref) ->
    %% Already parked — don't spawn a second helper.
    State;
arm_idle_waiter(#state{iter = Iter, wal_pid = WalPid} = State) ->
    {Seg, Off} = bondy_oplog_wal_reader:position(Iter),
    {_Pid, MRef} = spawn_monitor(fun() ->
        _ = bondy_oplog_wal:await_durable(
            WalPid, {Seg, Off + 1}, ?AWAIT_DURABLE_TIMEOUT_MS
        )
    end),
    State#state{idle_waiter = MRef}.

%% @private
%% Drop a parked idle waiter (if any). The orphaned helper is harmless:
%% it is blocked in `await_durable/3` and self-terminates within
%% `?AWAIT_DURABLE_TIMEOUT_MS`; `demonitor(_, [flush])` discards its
%% now-irrelevant `DOWN` so the next `handle_info({'DOWN', ...})` clause
%% won't match a stale reference.
cancel_idle_waiter(#state{idle_waiter = undefined} = State) ->
    State;
cancel_idle_waiter(#state{idle_waiter = MRef} = State) ->
    _ = erlang:demonitor(MRef, [flush]),
    State#state{idle_waiter = undefined}.

%% @private
%% Validate the substrate-wiring opts at init/1. Both hooks are opt-in;
%% the validation rejects partial configurations early so a typo in the
%% supervisor child-spec surfaces as a startup failure instead of a
%% silent no-op at the first publish call.
validate_substrate_opts(Opts) ->
    case validate_ae_targets(maps:get(ae_targets, Opts, [])) of
        ok ->
            case validate_publish_opts(Opts) of
                ok -> validate_cell_apply_target(Opts);
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

validate_cell_apply_target(Opts) ->
    case maps:get(cell_apply_target, Opts, undefined) of
        undefined ->
            ok;
        {NS, Index, Shard} when
            is_atom(NS),
            is_atom(Index),
            is_integer(Shard),
            Shard >= 0
        ->
            ok;
        Bad ->
            {error, {invalid_cell_apply_target, Bad}}
    end.

validate_ae_targets([]) ->
    ok;
validate_ae_targets([{NS, Index, Shard} | Rest]) when
    is_atom(NS),
    is_atom(Index),
    is_integer(Shard),
    Shard >= 0
->
    validate_ae_targets(Rest);
validate_ae_targets([Bad | _]) ->
    {error, {invalid_ae_target, Bad}};
validate_ae_targets(Bad) ->
    {error, {invalid_ae_targets, Bad}}.

validate_publish_opts(Opts) ->
    NS = maps:get(publish_ns, Opts, undefined),
    Fun = maps:get(publish_fun, Opts, undefined),
    case {NS, Fun} of
        {undefined, undefined} -> ok;
        {Atom, F} when is_atom(Atom), is_function(F, 1) -> ok;
        _ -> {error, {invalid_publish_opts, NS, Fun}}
    end.

%% @private
%% Walks `Verified` in HLC-monotonic order and publishes each event via
%% `bondy_db_core:publish/4`. A `publish_fun` returning `skip` suppresses
%% delivery for that event; a raise is logged and treated as `skip` so
%% a misbehaving derivation cannot wedge the applier. Best-effort
%% delivery; the dispatcher walks subscribers in this process.
publish_batch(#state{publish_ns = undefined}, _Verified) ->
    ok;
publish_batch(#state{publish_fun = undefined}, _Verified) ->
    ok;
publish_batch(
    #state{
        instance_id = Id,
        publish_ns = NS,
        publish_fun = Fun
    },
    Verified
) ->
    {Count, Skipped} = lists:foldl(
        fun(Event, {C, S}) ->
            case derive_publish(Fun, Event, Id) of
                skip ->
                    {C, S + 1};
                {Key, Op} ->
                    Hlc = bondy_oplog_event:key_hlc(
                        bondy_oplog_event:key(Event)
                    ),
                    ok = bondy_db_core:publish(NS, Key, Hlc, Op),
                    {C + 1, S}
            end
        end,
        {0, 0},
        Verified
    ),
    telemetry:execute(
        [bondy_oplog, applier, published],
        #{count => Count, skipped => Skipped},
        #{instance_id => Id, namespace => NS}
    ),
    ok.

derive_publish(Fun, Event, InstanceId) ->
    try Fun(Event) of
        skip ->
            skip;
        {K, Op} ->
            {K, Op};
        Bad ->
            log_publish_fun_bad_return(InstanceId, Event, Bad),
            skip
    catch
        C:R:S ->
            log_publish_fun_raised(InstanceId, Event, C, R, S),
            skip
    end.

log_publish_fun_bad_return(InstanceId, Event, Bad) ->
    ?LOG_WARNING(#{
        description =>
            "bondy_oplog_applier publish_fun returned an unexpected "
            "shape; event will not be published",
        instance_id => InstanceId,
        key => bondy_oplog_event:key(Event),
        return => Bad
    }).

log_publish_fun_raised(InstanceId, Event, C, R, S) ->
    ?LOG_WARNING(#{
        description =>
            "bondy_oplog_applier publish_fun raised; event will not "
            "be published",
        instance_id => InstanceId,
        key => bondy_oplog_event:key(Event),
        class => C,
        reason => R,
        stacktrace => S
    }).

%% @private
%% Bump the AE atomic counter for every shard in `ae_targets` with a
%% shared `monotonic_time(millisecond)` so the batch observes the same
%% "now". `not_found` is treated as benign (the registry entry may be
%% torn down concurrently during shutdown) and counted in telemetry.
%% Delegates the per-shard write to
%% `bondy_db_core_registry:bump_ae_targets/2` so the applier-side and
%% AE-side wirings share one primitive.
bump_ae_targets(#state{ae_targets = []}) ->
    ok;
bump_ae_targets(#state{instance_id = Id, ae_targets = Targets}) ->
    Now = erlang:monotonic_time(millisecond),
    {Bumped, NotFound} =
        bondy_db_core_registry:bump_ae_targets(Targets, Now),
    telemetry:execute(
        [bondy_oplog, applier, ae_bumped],
        #{count => Bumped, not_found => NotFound},
        #{instance_id => Id, now_ms => Now}
    ),
    ok.

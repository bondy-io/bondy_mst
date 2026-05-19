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
    cell_apply_ctx :: cell_apply_ctx() | undefined
}).

-type shard_key()   :: {atom(), atom(), non_neg_integer()}.
-type publish_fun() :: fun((bondy_oplog_event:t()) ->
    {Key :: term(), Op :: term()} | skip).
-type cell_apply_ctx() :: #{
    shard_key       := shard_key(),
    adapter         := module(),
    handle          := term(),
    fold_module     := bondy_oplog_fold:strategy()
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
            do_init_2(InstanceId, WalDir, CommitEvery, PollMs, Opts,
                AeTargets, PublishNs, PublishFun, CellCtx);
        {error, _} = Err ->
            {stop, Err}
    end.

do_init_2(InstanceId, WalDir, CommitEvery, PollMs, _Opts,
          AeTargets, PublishNs, PublishFun, CellCtx) ->
    case resolve_siblings(InstanceId) of
        {ok, InstP, WalP, MST, Watermark} ->
            CO = read_consumer_offset(WalDir),
            StartPos = resume_position(MST, Watermark),
            case bondy_oplog_wal_reader:open(
                WalP, StartPos, [{follow, false}]
            ) of
                {ok, Iter} ->
                    {ValidatorMod, ValidatorState} =
                        bondy_oplog_instance:get_validator(InstP),
                    {FoldMod, FoldState0} = init_fold(InstanceId),
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
                        cell_apply_ctx = CellCtx
                    },
                    ok = bondy_oplog_registry:set_applier_pid(
                        InstanceId, self()
                    ),
                    self() ! drain,
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
                        shard_key   => Key,
                        adapter     =>
                            bondy_db_core_registry:entry_projection_adapter(Entry),
                        handle      =>
                            bondy_db_core_registry:entry_projection_handle(Entry),
                        fold_module =>
                            bondy_db_core_registry:entry_fold_module(Entry)
                    }};
                not_found ->
                    {error, {cell_apply_target_not_registered, Key}}
            end
    end.

handle_call({enqueue_remote, Event}, From,
            #state{validator_module = Mod, validator_state = VS,
                   instance_pid = InstP, instance_id = Id} = State) ->
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
handle_call(get_projection, _From,
            #state{fold_module = undefined} = State) ->
    {reply, {error, no_fold_configured}, State};
handle_call(get_projection, _From,
            #state{fold_state = FS} = State) ->
    {reply, {ok, FS}, State};
handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast({refresh_validator, Reason}, State) ->
    {noreply, do_refresh_validator(Reason, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(drain, State0) ->
    case drain_loop(State0) of
        {ok, State1} ->
            ok = await_or_idle(State1),
            self() ! drain,
            {noreply, State1};
        {stop, Reason, State1} ->
            {stop, Reason, State1}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{iter = Iter, consumer_offset = CO,
                          wal_dir = Dir, uncommitted = N}) ->
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
drain_loop(#state{iter = Iter} = State0) ->
    case bondy_oplog_wal_reader:next(Iter) of
        {ok, Batch, _Hlcs, {NextSeg, NextOff}, NewIter} ->
            StateA = apply_batch(State0, Batch),
            {LastHlc, Count} = batch_summary(Batch),
            State1 = bump_offset(
                StateA#state{iter = NewIter},
                NextSeg, NextOff, LastHlc, Count
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
apply_batch(#state{instance_id = Id, instance_pid = InstancePid} = State, Batch) ->
    {Verified, Rejected} = verify_batch(State, Batch, [], []),
    RejectedCount = length(Rejected),
    case Rejected of
        [] -> ok;
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
                S1 = apply_fold_batch(State, FoldEvents),
                S2 = apply_cell_batch(S1, CellEvents),
                ok = publish_batch(S2, Verified),
                gen_server:cast(
                    InstancePid,
                    {install_local_batch, Verified}
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
apply_fold_batch(#state{fold_module = Mod,
                        fold_state = FS0,
                        instance_id = Id} = State, Verified) ->
    try
        FS1 = lists:foldl(
            fun(Event, Acc) ->
                bondy_oplog_fold:apply_event(
                    Mod, Acc, bondy_oplog_event:op(Event)
                )
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
%% via `apply_event/2`, encode back, and write the new frame via
%% `put_batch/2`. Bucket is a first-class call-time parameter on the
%% projection adapter; the applier passes it through verbatim.
apply_cell_batch(State, []) ->
    State;
apply_cell_batch(#state{cell_apply_ctx = undefined} = State, _Events) ->
    State;
apply_cell_batch(#state{cell_apply_ctx = Ctx,
                        instance_id = Id} = State, Events) ->
    #{adapter := Adapter, handle := Handle, fold_module := Fold} = Ctx,
    lists:foreach(
        fun(Event) ->
            case bondy_oplog_event:op(Event) of
                {cell_apply, Bucket, Key, FoldEvent} ->
                    apply_one_cell(Id, Adapter, Handle, Fold,
                                   Bucket, Key, FoldEvent);
                _ ->
                    ok
            end
        end,
        Events
    ),
    State.

%% @private
apply_one_cell(Id, Adapter, Handle, Fold, Bucket, Key, FoldEvent) ->
    try
        OldState =
            case Adapter:get(Handle, Bucket, Key) of
                not_found ->
                    bondy_oplog_fold:initial_value(Fold);
                {ok, OldFrame} ->
                    {_PrevHlc, OldBody} =
                        bondy_oplog_cell_frame:decode(OldFrame),
                    bondy_oplog_fold:decode_state(Fold, OldBody)
            end,
        NewState = bondy_oplog_fold:apply_event(Fold, OldState, FoldEvent),
        Hlc = bondy_oplog_fold:hlc(Fold, NewState),
        NewBody = bondy_oplog_fold:encode_state(Fold, NewState),
        NewFrame = bondy_oplog_cell_frame:encode(Hlc, NewBody),
        case Adapter:put_batch(Handle, [{Bucket, Key, NewFrame}]) of
            ok ->
                ok;
            {error, Reason} ->
                ?LOG_WARNING(#{
                    description =>
                        "bondy_oplog_applier projection write failed; "
                        "the cell will be re-applied on the next replay "
                        "of this event",
                    instance_id => Id,
                    bucket => Bucket,
                    cell_key => Key,
                    reason => Reason
                }),
                ok
        end
    catch
        C:R:S ->
            ?LOG_ERROR(#{
                description =>
                    "bondy_oplog_applier cell_apply raised; the cell "
                    "has been skipped. Subtree continues to drain.",
                instance_id => Id,
                bucket => Bucket,
                cell_key => Key,
                fold_module => Fold,
                class => C,
                reason => R,
                stacktrace => S
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
                    _ = try
                        ets:select_delete(Tab, [{
                            {Key, '_', '$1', '_'},
                            [{'=<', '$1', Hlc}],
                            [true]
                        }])
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
do_refresh_validator(Reason,
                     #state{instance_id = Id,
                            validator_module = Mod,
                            validator_state = VS} = State) ->
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
                #{instance_id => Id, validator => Mod,
                  outcome => unsupported, refresh_reason => Reason}
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
                        #{instance_id => Id, validator => Mod,
                          outcome => ok, refresh_reason => Reason}
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
                        #{instance_id => Id, validator => Mod,
                          outcome => error, refresh_reason => Reason,
                          error => RefreshReason}
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
                        #{instance_id => Id, validator => Mod,
                          outcome => crashed, refresh_reason => Reason,
                          class => C, error => R}
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
bump_offset(#state{consumer_offset = CO0, uncommitted = U} = State,
            Seg, Off, LastHlc, Count) ->
    CO1 = bondy_oplog_wal_state:with_position(CO0, Seg, Off),
    CO2 = bondy_oplog_wal_state:with_hlc(CO1, LastHlc),
    Old = bondy_oplog_wal_state:commit_count(CO2),
    CO3 = bondy_oplog_wal_state:with_commit_count(CO2, Old + 1),
    State#state{consumer_offset = CO3, uncommitted = U + Count}.

%% @private
maybe_commit(#state{uncommitted = U, commit_every = N} = State)
        when U >= N ->
    commit_now(State);
maybe_commit(State) ->
    State.

%% @private
commit_now(#state{uncommitted = 0} = State) ->
    State;
commit_now(#state{
    instance_id = InstanceId,
    instance_pid = InstancePid,
    wal_dir = Dir,
    wal_pid = WalPid,
    consumer_offset = CO
} = State) ->
    %% Drain barrier: block until the instance has processed every
    %% `install_local_batch` cast we issued before this commit. The
    %% FIFO mailbox ordering of casts and the synchronous call
    %% together guarantee that, when the call returns, all events
    %% whose keys we're about to commit have been installed in the
    %% MST. Without this barrier, `notify_committed_segment` could
    %% drop a WAL segment whose events the instance has not yet
    %% applied — a hard durability hole on a co-crash.
    ok = drain_install_queue(InstancePid),
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
%% Block (briefly) until the WAL's durable position advances past the
%% reader's current offset, or the inner timeout fires. The applier's
%% main loop schedules an immediate re-drain after this returns; the
%% `poll_interval_ms` is only used as a backstop if `await_durable/3`
%% replies sooner than the inner timeout (which shouldn't normally
%% happen but is bounded here defensively).
await_or_idle(#state{iter = Iter, wal_pid = WalPid,
                     poll_interval_ms = PollMs}) ->
    {Seg, Off} = bondy_oplog_wal_reader:position(Iter),
    case bondy_oplog_wal:await_durable(WalPid, {Seg, Off},
                                       ?AWAIT_DURABLE_TIMEOUT_MS) of
        ok -> ok;
        {error, timeout} -> ok;
        {error, _} ->
            %% Defensive: any other error from `await_durable/3` is a
            %% WAL-level failure that the next `next/1` will surface
            %% on its own. Fall back to a short sleep so we don't burn
            %% CPU re-asking immediately.
            timer:sleep(PollMs)
    end,
    ok.

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
        {NS, Index, Shard}
                when is_atom(NS), is_atom(Index),
                     is_integer(Shard), Shard >= 0 ->
            ok;
        Bad ->
            {error, {invalid_cell_apply_target, Bad}}
    end.

validate_ae_targets([]) ->
    ok;
validate_ae_targets([{NS, Index, Shard} | Rest])
        when is_atom(NS), is_atom(Index),
             is_integer(Shard), Shard >= 0 ->
    validate_ae_targets(Rest);
validate_ae_targets([Bad | _]) ->
    {error, {invalid_ae_target, Bad}};
validate_ae_targets(Bad) ->
    {error, {invalid_ae_targets, Bad}}.

validate_publish_opts(Opts) ->
    NS  = maps:get(publish_ns, Opts, undefined),
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
publish_batch(#state{instance_id = Id, publish_ns = NS,
                     publish_fun = Fun}, Verified) ->
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
        skip            -> skip;
        {K, Op}         -> {K, Op};
        Bad             ->
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

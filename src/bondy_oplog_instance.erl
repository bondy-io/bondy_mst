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
- Install verified peer events into the MST. Signature verification
  for both locally appended events (WAL drain path) and peer-received
  events (`append_remote/2`) runs in the per-instance applier
  process; this gen_server is the install point but not the verify
  point.
- Expose the MST root hash, key-range reads, and prefix truncation
  hooks for compaction.
- Run compaction cycles: stability frontier → `interpret_cog` →
  snapshot → MST truncate → watermark advance.
- Own the page-level anti-entropy primitives (`merge_pages`,
  `integrate_peer_root`) and snapshot-load operations. These are not
  event-stream operations; the public façade drains the applier
  before invoking them so installs already in flight are visible.

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
    %% Per-namespace fold strategy (FOLD_STRATEGY_DESIGN §6/§7). The
    %% applier consumes WAL events and folds them into per-cell
    %% projection state via this module's callbacks. `undefined`
    %% means no fold is configured for the instance and the applier
    %% takes the legacy event-storage path. Stored as a resolved
    %% module name (shorthand atoms are validated and recorded
    %% verbatim — `mod_of/1` resolves at call time, so a config
    %% migration that changes a shorthand → module mapping is
    %% transparent on restart).
    fold_module :: module() | atom() | undefined,
    %% Opaque, fold-module-specific options. Passed to the fold
    %% module at applier-side initialisation (the behaviour callback
    %% `initial_value/0` is parameterless, so opts are consumed by
    %% the consumer wrapping the fold — see F8). Empty map by
    %% default.
    fold_opts :: map(),
    crdt_module :: module() | undefined,
    compaction_checkpoint :: module(),
    compaction_checkpoint_state :: term(),
    watermark :: undefined | bondy_oplog_event:event_key(),
    %% Cached `{Watermark, Checkpoint}` from the compaction checkpoint
    %% store so the registry can publish it without re-reading the
    %% store on every mutation. Refreshed on init, compact, and
    %% load_snapshot.
    cached_checkpoint :: undefined | {bondy_oplog_event:event_key(), term()},
    max_working_set :: pos_integer() | infinity,
    %% Cached size of the live MST (avoids a fold per append). Updated
    %% on every state-mutating handle_call.
    live_size :: non_neg_integer(),
    last_event_key :: undefined | bondy_oplog_event:event_key(),
    %% Cached per-instance WAL writer pid. Refreshed lazily from the
    %% registry on the first append after a `'DOWN'` from the previous
    %% writer (one_for_all restarts swap in a new pid).
    wal_pid :: undefined | pid(),
    %% Monitor reference for the cached `wal_pid`; cleared when the
    %% monitored process dies.
    wal_pid_monitor :: undefined | reference(),
    %% Per-instance overlay (`ordered_set`, public). Receives every
    %% successfully WAL-appended local event so callers reading back
    %% the key see the entry before the applier promotes it to the
    %% MST. Rows are `{Key, Value, Hlc, Origin}`; entries are evicted
    %% atomically with the MST insert via HLC-conditional
    %% `ets:select_delete/2`. Created in `init/1`, deleted in
    %% `terminate/2`; no heir.
    overlay :: undefined | ets:tid(),
    %% Overlay backpressure caps. `max_overlay_events` defaults to
    %% 10_000; `max_overlay_bytes` to 5 MB; `throttle_strategy`
    %% defaults to `drop` and is the only supported value
    %% (`block` reserved).
    max_overlay_events :: pos_integer(),
    max_overlay_bytes :: pos_integer(),
    overlay_throttle :: drop,
    %% Atomic mirrors of `ets:info(overlay, size)` and
    %% `ets:info(overlay, memory)`. Held as `atomics:atomics_ref()` so
    %% the lock-free `append_fast/2,3` path (caller-side) can update
    %% them without going through this gen_server. Slot 1: event
    %% count. Slot 2: byte estimate. Updated on insert
    %% (`stage_to_overlay/2`) and evict (`evict_overlay_batch/2`).
    %% With `decentralized_counters: true` on the overlay table, the
    %% equivalent `ets:info/2` calls aggregate across all schedulers
    %% and grow expensive under concurrent appenders — these atomic
    %% mirrors keep the admit check purely lock-free and constant-time.
    overlay_counters :: atomics:atomics_ref(),
    %% Highest local-origin `seq` already installed into the MST.
    %% Used by `install_local_batch/2` to skip the
    %% O(log N) `bondy_mst:get/2` safety probe for events whose seq is
    %% strictly greater than this value — for the local origin the seq
    %% atomic monotonically increases, so any event with a higher seq
    %% cannot already be in the tree. Resume-overlap events (seq ≤ max)
    %% still take the safe path that probes the tree.
    max_local_installed_seq :: non_neg_integer(),
    %% Callers blocked in `await_apply/1,2` while the overlay is
    %% non-empty. Each install path that may shrink the overlay
    %% (`install_local_batch` cast, `install_remote` call, and the
    %% applier's `check_drain_waiters` rejection hint) calls
    %% `maybe_signal_drain_waiters/1` which `gen_server:reply`-s every
    %% queued From the moment the overlay reaches 0.
    drain_waiters = [] :: [gen_server:from()],
    %% Demand-based applier→instance flow control. The applier
    %% increments slot 1 of `install_in_flight` before dispatching an
    %% `install_local_batch` cast; this handler decrements it after
    %% the cast is processed. When the post-decrement value is
    %% `max_install_in_flight - 1` (i.e. just freed a slot from a
    %% saturated counter), the instance sends a `drain_resume` cast
    %% to the applier so it can read the next WAL batch. Bounds the
    %% instance's mailbox at `cap × batch_size` events.
    install_in_flight :: atomics:atomics_ref() | undefined,
    max_install_in_flight :: pos_integer(),
    %% A4 — instance-side install coalescing. The
    %% `install_local_batch` cast handler drains up to this many *queued*
    %% install casts (including the one being handled) and merges their
    %% events into a single `bondy_mst:put_batch/2` + one publish + one
    %% overlay-evict. When the applier outruns the instance the mailbox
    %% accumulates casts, so this amortises the O(log n) spine rebuild
    %% over many casts' worth of events — the dominant per-event durable
    %% cost (A0b). `1` reproduces the pre-A4 one-put_batch-per-cast
    %% behaviour. Bounded by `max_install_in_flight` in practice (the
    %% applier cannot have more than that many casts in flight).
    install_coalesce_max :: pos_integer(),
    %% Bootstrap lifecycle (`bondy_oplog_bootstrap_lifecycle`). Opened
    %% at `init/1` and published via the registry so the applier can
    %% gate its WAL drain on the durable two-state machine
    %% (`pre_bootstrap | live`). See
    %% `_design/catalogue_expansion_plan.md` §2.
    lifecycle :: bondy_oplog_bootstrap_lifecycle:handle()
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
    %% Legacy MST page-merge collision resolver. Deprecated in
    %% favour of `fold_module` (FOLD_STRATEGY_DESIGN §6/§7) — still
    %% honoured for backward compatibility. Defaults to
    %% `bondy_oplog_merge_strict_uniqueness`, which crashes on
    %% divergent values for the same event key. Configuring this
    %% emits a one-shot deprecation warning at instance start.
    merge_strategy => module(),
    %% Per-table CRDT, named by a `fold_module` label for backward
    %% compatibility. Resolves to its native `bondy_oplog_crdt` twin via
    %% `bondy_oplog_cell_kernel:default_crdt_for_fold/1` (PR-Z): a
    %% shorthand atom (`lww_register`, `g_counter`, `pn_counter`, `g_set`,
    %% `max_register`, `min_register`, `index_entry`), the fully-qualified
    %% `bondy_oplog_fold_*` form, or a native `bondy_oplog_crdt_*` module
    %% directly. A label with no twin is rejected (validated at instance
    %% start; a misconfigured value crashes init). Prefer `crdt_module`
    %% for new tables.
    fold_module => atom(),
    %% Opaque options passed through; shape is consumer-specific,
    %% defaults to `#{}`.
    fold_opts => map(),
    crdt_module => module(),
    compaction_checkpoint => module(),
    compaction_checkpoint_opts => map(),
    max_working_set => pos_integer() | infinity,
    %% Overlay backpressure caps. Either threshold triggers
    %% `{error, backpressure}` from `append/2,3` and `append_many/2`.
    max_overlay_events => pos_integer(),
    max_overlay_bytes => pos_integer(),
    %% Throttle strategy on overlay-cap breach. `drop` returns
    %% `{error, backpressure}` immediately; `block` is reserved for a
    %% follow-on PR and currently behaves like `drop`.
    overlay_throttle => drop,
    %% A4 — max number of queued `install_local_batch` casts the
    %% instance coalesces into one MST `put_batch` (default 16; `1`
    %% disables coalescing). See the `install_coalesce_max` state field.
    install_coalesce_max => pos_integer(),
    %% Per-instance applier tuning. See `bondy_oplog_applier:opts/0`.
    %% Recognised keys:
    %%   commit_every     :: pos_integer()   (default 64)
    %%   poll_interval_ms :: pos_integer()   (default 5)
    applier => #{
        commit_every => pos_integer(),
        poll_interval_ms => pos_integer()
    },
    %% Bootstrap lifecycle seed. `true` declares the instance as a
    %% genesis peer in a fresh cluster (no peer to bootstrap from); the
    %% lifecycle starts `live` and, if `storage_path` is set, the
    %% durable `lifecycle.live` flag file is written so the next
    %% restart sees `live` without needing the seed opt again. `false`
    %% (default for persistent instances) keeps the lifecycle in
    %% `pre_bootstrap` until `bondy_oplog_sync_session:bootstrap/3`
    %% completes against a live peer. Ephemeral instances (no
    %% `storage_path`) default to `live` regardless of `seed` —
    %% there is no persistent state to bootstrap from. See
    %% `_design/catalogue_expansion_plan.md` §2.
    seed => boolean()
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
-export([append_fast/3]).
-export([append_many/2]).
-export([append_many_fast/2]).
-export([append_remote/2]).
-export([await_apply/1]).
-export([await_apply/2]).
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

%% Applier handshake — applier reads `{validator_module, validator_state}`
%% once at its `init/1` so it can re-verify signatures (S1) in its own
%% process before dispatching events to the instance.
-export([get_validator/1]).

%% Operator-facing trigger that asks the applier to refresh its
%% validator snapshot by calling the optional
%% `bondy_oplog_validator:refresh/1` callback.
-export([refresh_validator/1, refresh_validator/2]).
-export([reap_origins/2]).

%% Page-level API (sync protocol)
-export([get_pages/2]).
-export([merge_pages/2]).
-export([missing_set/2]).
-export([integrate_peer_root/2]).

%% GC / compaction API
-export([current_watermark/1]).
-export([crdt_module/1]).
-export([compaction_checkpoint/1]).
-export([compact/2]).

%% Bootstrap
-export([load_snapshot/3]).
-export([mark_live/1]).
-export([lifecycle_state/1]).
-export([install_catalogue_batch/2]).
-export([finalize_catalogue_bootstrap/3]).

%% Registry helpers
-export([whereis/1]).
-export([lookup_origin/1]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

-ifdef(TEST).
%% Exposed for the stability-frontier equivalence test.
-export([compute_frontier_for/2]).
-endif.

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
Lock-free single-event append for instances whose validator is
stateless (advertises `bondy_oplog_validator:is_stateless/0 -> true`).
Builds the event in the caller's process, calls the WAL gen_server
directly, and stages the overlay row inline — skipping the instance
gen_server hop entirely.

Returns the assigned `event_key()` on success, `{error, backpressure}`
or `{error, working_set_full}` if backpressure caps would be
breached, and `{error, wal_unavailable}` if the WAL is mid-restart.

Callers should not use this directly; route through
`bondy_oplog:append/2,3`, which checks fast-path eligibility from
the registry and falls back to the gen_server when ineligible.
""").
-spec append_fast(
    instance_id(),
    bondy_oplog_event:op(),
    bondy_oplog_event:meta()
) -> bondy_oplog_event:event_key() | {error, term()}.

append_fast(InstanceId, Op, Meta) when is_binary(InstanceId) ->
    case bondy_oplog_registry:fast_path(InstanceId) of
        undefined ->
            %% Fast path was disabled or torn down — fall back.
            append(InstanceId, Op, Meta);
        FastPath ->
            do_append_fast(InstanceId, FastPath, Op, Meta)
    end.

%% @private
do_append_fast(InstanceId, FastPath, Op, Meta) ->
    #{
        hlc := HLC,
        seq := SeqRef,
        overlay_counters := Ctrs,
        origin := Origin,
        validator_module := ValidatorMod,
        validator_state := ValidatorState,
        max_overlay_events := MaxEvents,
        max_overlay_bytes := MaxBytes,
        max_working_set := MaxWorkingSet
    } = FastPath,
    case fast_admit(InstanceId, Ctrs, MaxEvents, MaxBytes, MaxWorkingSet, 1) of
        ok ->
            %% Build the event in the caller's process. The HLC + seq
            %% atomics give us a unique, monotonic key without holding
            %% the instance gen_server.
            Hlc = bondy_oplog_hlc:now(HLC),
            Seq = atomics:add_get(SeqRef, 1, 1),
            Key = bondy_oplog_event:key(Hlc, Origin, Seq),
            Event0 = bondy_oplog_event:new(Key, Op, Meta),
            %% Stateless validator: discard the returned state — by
            %% contract it equals the cached one.
            {Event, _} = ValidatorMod:sign_event(Event0, ValidatorState),
            %% Resolve the overlay tid up-front. It can briefly be
            %% `undefined` after a one_for_all restart before the new
            %% instance's init/1 republishes it; in that window we fall
            %% back to the gen_server path which builds its own event.
            case bondy_oplog_registry:overlay_tab(InstanceId) of
                undefined ->
                    append(InstanceId, Op, Meta);
                Tab ->
                    %% Stage the overlay row BEFORE the WAL append. See
                    %% the matching comment in `do_append_local/3` —
                    %% the applier reads from the WAL the instant it
                    %% becomes durable, and an in-flight overlay insert
                    %% races with `evict_overlay_batch/2`.
                    true = ets:insert(Tab, [overlay_row(Event, local)]),
                    overlay_counters_add(Ctrs, [Event]),
                    case fast_wal_append_batch(InstanceId, [Event]) of
                        ok ->
                            telemetry:execute(
                                [bondy_oplog, instance, append],
                                #{count => 1},
                                #{instance_id => InstanceId}
                            ),
                            Key;
                        {error, _} = Err ->
                            %% WAL rejected the batch — drop the
                            %% staged row so no phantom write is
                            %% observable.
                            ets:delete(Tab, Key),
                            overlay_counters_sub(Ctrs, 1),
                            Err
                    end
            end;
        {error, _} = Err ->
            Err
    end.

?DOC("""
Lock-free batch append. Same eligibility as `append_fast/3`: the
instance's validator must advertise `is_stateless/0 -> true`. The
caller mints every event's `{HLC, Origin, Seq}` key, signs each
event in-process, ships the whole batch through the WAL as one
atomic frame, inserts every overlay row in a single `ets:insert/2`,
and bumps the overlay-counters atomics once.

The WAL's `append_batch/2` is all-or-nothing: either every event
becomes durable or the entire batch is rejected. The fast path
inherits that semantic — on `{error, _}` no overlay row is written
and the caller can retry. HLCs and Seqs that were minted for a
rejected batch are not recycled; gaps in those sequences are
benign (the keys are global identifiers, not array indices).

Returns the assigned `event_key()` list in input order, or
`{error, backpressure | working_set_full | wal_unavailable | _}`.

Routed through automatically by `bondy_oplog:append_many/2` when
the registry exposes a fast-path bundle; the consumer-facing API
stays unchanged.
""").
-spec append_many_fast(
    instance_id(),
    [{bondy_oplog_event:op(), bondy_oplog_event:meta()}]
) -> [bondy_oplog_event:event_key()] | {error, term()}.

append_many_fast(_InstanceId, []) ->
    [];
append_many_fast(InstanceId, Items) when
    is_binary(InstanceId), is_list(Items)
->
    case bondy_oplog_registry:fast_path(InstanceId) of
        undefined ->
            %% Stateful validator or fast-path torn down — defer to
            %% the gen_server path which threads validator state
            %% through the batch.
            append_many(InstanceId, Items);
        FastPath ->
            do_append_many_fast(InstanceId, FastPath, Items)
    end.

%% @private
do_append_many_fast(InstanceId, FastPath, Items) ->
    #{
        hlc := HLC,
        seq := SeqRef,
        overlay_counters := Ctrs,
        origin := Origin,
        validator_module := ValidatorMod,
        validator_state := ValidatorState,
        max_overlay_events := MaxEvents,
        max_overlay_bytes := MaxBytes,
        max_working_set := MaxWorkingSet
    } = FastPath,
    Delta = length(Items),
    case
        fast_admit(InstanceId, Ctrs, MaxEvents, MaxBytes, MaxWorkingSet, Delta)
    of
        ok ->
            {Events, Keys} = build_events_fast(
                HLC, SeqRef, Origin, ValidatorMod, ValidatorState, Items
            ),
            %% Resolve the overlay tid up-front; the rare `undefined`
            %% window after a one_for_all restart routes through the
            %% gen_server which mints its own keys.
            case bondy_oplog_registry:overlay_tab(InstanceId) of
                undefined ->
                    append_many(InstanceId, Items);
                Tab ->
                    %% Stage overlay rows BEFORE the WAL append (see
                    %% the matching comment on `do_append_local/3`).
                    Rows = [overlay_row(E, local) || E <- Events],
                    true = ets:insert(Tab, Rows),
                    overlay_counters_add(Ctrs, Events),
                    case fast_wal_append_batch(InstanceId, Events) of
                        ok ->
                            telemetry:execute(
                                [bondy_oplog, instance, append],
                                #{count => Delta},
                                #{instance_id => InstanceId}
                            ),
                            Keys;
                        {error, _} = Err ->
                            %% Roll back the staged rows.
                            lists:foreach(
                                fun(E) ->
                                    ets:delete(
                                        Tab,
                                        bondy_oplog_event:key(E)
                                    )
                                end,
                                Events
                            ),
                            overlay_counters_sub(Ctrs, length(Events)),
                            Err
                    end
            end;
        {error, _} = Err ->
            Err
    end.

%% @private
%% Mints `{Event, Key}` pairs for every item.
%%
%% - One HLC tick per item: the WAL's `do_append_batch/2` rejects
%%   a batch whose HLCs are not strictly increasing (so receivers
%%   can rely on per-batch monotonicity for cheap merge-by-HLC).
%%   Each `bondy_oplog_hlc:now/1` is a lock-free CAS that already
%%   guarantees strict monotonicity at the per-replica level.
%% - **One** `atomics:add_get/3` to reserve a contiguous seq range,
%%   then assign each event `Start+i`. N - 1 fewer atomic
%%   read-modify-writes on the shared seq atomics per batch.
%%
%% The validator state is immutable (stateless validators by
%% contract), so we pass the same `ValState` for every call and
%% discard the returned state.
build_events_fast(HLC, SeqRef, Origin, Mod, ValState, Items) ->
    N = length(Items),
    EndSeq = atomics:add_get(SeqRef, 1, N),
    StartSeq = EndSeq - N + 1,
    {EventsRev, KeysRev, _} = lists:foldl(
        fun({Op, Meta}, {EvAcc, KAcc, Seq}) ->
            Hlc = bondy_oplog_hlc:now(HLC),
            Key = bondy_oplog_event:key(Hlc, Origin, Seq),
            Event0 = bondy_oplog_event:new(Key, Op, Meta),
            {Event, _} = Mod:sign_event(Event0, ValState),
            {[Event | EvAcc], [Key | KAcc], Seq + 1}
        end,
        {[], [], StartSeq},
        Items
    ),
    {lists:reverse(EventsRev), lists:reverse(KeysRev)}.

%% @private
%% Lock-free analogue of `admit/2` for the fast path. The atomics
%% counters can be transiently over-read because reads and writes are
%% not serialised across slot 1/slot 2 — backpressure is best-effort.
fast_admit(InstanceId, Ctrs, MaxEvents, MaxBytes, MaxWorkingSet, Delta) ->
    Size = atomics:get(Ctrs, 1),
    Bytes = atomics:get(Ctrs, 2),
    case Size + Delta > MaxEvents of
        true ->
            emit_overlay_backpressure(
                InstanceId, events, Size, MaxEvents, Delta
            ),
            {error, backpressure};
        false ->
            case Bytes >= MaxBytes of
                true ->
                    emit_overlay_backpressure(
                        InstanceId, bytes, Bytes, MaxBytes, Delta
                    ),
                    {error, backpressure};
                false ->
                    fast_working_set_admit(
                        InstanceId, Size, MaxWorkingSet, Delta
                    )
            end
    end.

%% @private
fast_working_set_admit(_InstanceId, _Size, infinity, _Delta) ->
    ok;
fast_working_set_admit(InstanceId, OverlaySize, Cap, Delta) ->
    LiveSize =
        case bondy_oplog_registry:live_size(InstanceId) of
            undefined -> 0;
            N -> N
        end,
    Total = LiveSize + OverlaySize,
    case Total + Delta =< Cap of
        true ->
            ok;
        false ->
            telemetry:execute(
                [bondy_oplog, instance, backpressure],
                #{count => 1},
                #{
                    instance_id => InstanceId,
                    requested => Delta,
                    live_size => LiveSize,
                    overlay_size => OverlaySize,
                    cap => Cap
                }
            ),
            {error, working_set_full}
    end.

%% @private
%% Resolves the per-instance WAL pid and calls `append_batch/2`. The
%% pid can be `undefined` for a window during init or after a
%% one_for_all subtree restart; in either case return
%% `{error, wal_unavailable}` so the caller can fall back to the
%% instance gen_server path (which has `ensure_wal_pid/1` retry).
fast_wal_append_batch(InstanceId, Events) ->
    case bondy_oplog_registry:wal_pid(InstanceId) of
        undefined ->
            {error, wal_unavailable};
        WalPid ->
            try bondy_oplog_wal:append_batch(WalPid, Events) of
                {ok, _Entries} -> ok;
                {error, _} = Err -> Err
            catch
                exit:{noproc, _} -> {error, wal_unavailable};
                exit:noproc -> {error, wal_unavailable};
                exit:{normal, _} -> {error, wal_unavailable};
                exit:{shutdown, _} -> {error, wal_unavailable}
            end
    end.

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
configured validator's `verify_event/2` runs in the per-instance
applier process, which then forwards the verified event to the
instance for origin-ban / backpressure / watermark filtering and the
MST install. The applier is therefore the sole verify+dispatch origin
for both locally appended and peer-received events.

**Pass an `instance_id()` (binary)** for hot-path callers. The binary
form resolves origin and applier pid via lock-free registry reads
before issuing the verify call. The `pid()` form is supported for
test/internal convenience only: it pays two extra `gen_server:call`
round trips (origin lookup, then instance-id reverse lookup) before
the verify call begins.
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
            case applier_pid_for(Target) of
                {ok, ApplierPid} ->
                    bondy_oplog_applier:enqueue_remote(ApplierPid, Event);
                {error, _} = Err ->
                    Err
            end
    end.

%% @private
%% Resolves the applier pid for a `Target` (instance id or instance
%% pid). The applier is published in the registry by its own
%% `init/1`; during a subtree mid-restart it can briefly be absent,
%% in which case callers see `{error, applier_unavailable}` and can
%% retry.
applier_pid_for(InstanceId) when is_binary(InstanceId) ->
    case bondy_oplog_registry:applier_pid(InstanceId) of
        undefined -> {error, applier_unavailable};
        Pid when is_pid(Pid) -> {ok, Pid}
    end;
applier_pid_for(Pid) when is_pid(Pid) ->
    case lookup_instance_id(Pid) of
        undefined -> {error, applier_unavailable};
        Id -> applier_pid_for(Id)
    end.

?DOC("""
Blocks until the per-instance applier has promoted every overlay row
into the MST, or until `Timeout` ms have elapsed.

After the write path returns from `append/2`, the event is durable
in the WAL and visible in the overlay but not yet in the MST. The
per-instance applier drains the WAL and dispatches `install_local_batch`
casts to the instance; each cast installs the events in the MST and
evicts the matching overlay rows.

Operations that read the MST directly (`root_hash/1`, `compact/2`,
`sync/2`) see the post-applier state only — callers that need
read-after-write consistency on the MST itself should call this
function as a synchronisation point.

Returns `ok` once the overlay is empty, or `{error, timeout}` on
expiry. A missing overlay (subtree mid-restart) returns `ok`.
""").
-spec await_apply(instance_id() | pid()) -> ok | {error, timeout}.

await_apply(Target) ->
    await_apply(Target, 5000).

-spec await_apply(instance_id() | pid(), timeout()) -> ok | {error, timeout}.

await_apply(Target, Timeout) when
    is_binary(Target) orelse is_pid(Target)
->
    %% Event-driven barrier: resolve the instance pid and issue a
    %% `await_overlay_drained` gen_server:call. If the overlay is
    %% non-empty the instance queues the caller in `drain_waiters`
    %% and replies the moment its install handlers shrink the overlay
    %% to 0. Replaces the prior 5 ms-poll loop, which floored every
    %% wait at the timer resolution regardless of how fast the
    %% applier actually drained.
    case resolve_instance_pid(Target) of
        undefined ->
            %% No registered instance — nothing to drain. Mirrors the
            %% old polling behaviour for pid lookups that miss.
            ok;
        Pid ->
            call_await_overlay_drained(Pid, Timeout)
    end.

%% @private
resolve_instance_pid(Pid) when is_pid(Pid) ->
    Pid;
resolve_instance_pid(InstanceId) when is_binary(InstanceId) ->
    bondy_oplog_registry:instance_pid(InstanceId).

%% @private
call_await_overlay_drained(Pid, Timeout) ->
    try gen_server:call(Pid, await_overlay_drained, Timeout) of
        ok -> ok
    catch
        exit:{timeout, _} -> {error, timeout};
        %% Subtree restart: treat as drained — the new instance starts
        %% with an empty overlay so the prior overlay contents (if any)
        %% are no longer observable.
        exit:{noproc, _} -> ok;
        exit:noproc -> ok;
        exit:{normal, _} -> ok;
        exit:{shutdown, _} -> ok
    end.

%% @private
%% Best-effort reverse lookup from gen_server pid to instance_id. Used
%% by `applier_pid_for/1` when the caller targets the instance by pid.
%% Returns `undefined` when the pid is not registered.
lookup_instance_id(Pid) when is_pid(Pid) ->
    try gen_server:call(Pid, instance_id, 1000) of
        Id when is_binary(Id) -> Id;
        _ -> undefined
    catch
        _:_ -> undefined
    end.

-spec get(instance_id() | pid(), bondy_oplog_event:event_key()) ->
    {ok, bondy_oplog_event:t()} | not_found.

get(Target, Key) when is_binary(Target) ->
    %% Overlay-first, then MST. One registry lookup pulls both
    %% handles — the old two-`lookup_element` pattern serialised on
    %% the same per-key slot lock and dominated the cost of cold
    %% reads. The overlay holds events that landed in the WAL but
    %% have not yet been promoted by the applier; reading the overlay
    %% before the MST handle closes the race where the applier
    %% publishes a new handle and then evicts the overlay row (MST
    %% publish strictly precedes overlay evict).
    case bondy_oplog_registry:read_overlay_and_mst(Target) of
        undefined ->
            error({noproc, {?MODULE, Target}});
        {Tab, MST} ->
            case overlay_lookup_tab(Tab, Key) of
                {ok, _} = Hit ->
                    Hit;
                not_found ->
                    case bondy_mst:get(MST, Key) of
                        undefined -> not_found;
                        Value -> {ok, event_from_value(Key, Value)}
                    end
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

fold_range(Target, From, To, Fun, Acc0) when is_function(Fun, 2) ->
    %% Routed through the gen_server so the MST snapshot and the
    %% overlay scan are captured in the same callback — `publish/1`
    %% (registry write) and `evict_overlay_batch/2` are sibling steps
    %% of `install_local_batch`, but they are visible to a lock-free
    %% reader at two independent moments. Under whole-suite load that
    %% race was dropping events from `fold_range/5`. Sync hop cost is
    %% acceptable for the rare admin / test use of range scans.
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

size(Target) ->
    %% Total events visible to the instance = `live_size` (events
    %% promoted to the MST) + overlay row count. The two are
    %% maintained in lockstep by `install_local_batch` (live_size +=
    %% N, overlay -= N), but those updates are *not* observable to a
    %% lock-free reader as a single atom. Routing through the
    %% gen_server is the simplest way to read them in the same
    %% callback — no `install_local_batch` cast can run while we are
    %% the handler — so we always return a consistent snapshot.
    %% Sync hop cost is acceptable for the stats / admin use of
    %% `size/1`; hot-path callers stay on `get/2` and `append/2,3`.
    gen_server:call(target(Target), instance_size).

-spec first_key(instance_id() | pid()) ->
    {ok, bondy_oplog_event:event_key()} | empty.

first_key(Target) ->
    %% Same MST-snapshot / overlay-scan atomicity story as
    %% `fold_range/5` — route through the gen_server so the two
    %% sources are read in a single handler.
    gen_server:call(target(Target), first_key).

-spec latest_key(instance_id() | pid()) ->
    {ok, bondy_oplog_event:event_key()} | empty.

latest_key(Target) ->
    %% Same atomicity story as `first_key/1`.
    gen_server:call(target(Target), latest_key).

?DOC("""
Returns the configured Origin for `Target`.

**Pass an `instance_id()` (binary)** for hot-path callers — this form
reads the value directly from the registry without messaging. The
`pid()` form is a test/internal convenience that issues a synchronous
`gen_server:call` to the instance.
""").
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

?DOC("""
Returns the validator module and a snapshot of the validator state
for the per-instance applier. The applier uses this to re-verify
event signatures (S1) in its own process before dispatching events
to the instance for install. `verify_event/2` is read-only on the
validator state, so the snapshot remains valid for the lifetime of
the applier.
""").
-spec get_validator(pid()) -> {module(), term()}.

get_validator(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, get_validator, infinity).

?DOC("""
Asks the per-instance applier to refresh its validator snapshot by
calling `bondy_oplog_validator:refresh/1` on the current snapshot.

Returns `ok` once the refresh request has been *delivered* to the
applier (fire-and-forget cast). The actual outcome — snapshot
swapped, validator returned `{error, _}`, validator raised, or
`refresh/1` not exported — is logged by the applier and surfaced
via the `[bondy_oplog, applier, validator_refresh]` telemetry event.

Returns `{error, applier_unavailable}` if the subtree is mid-restart
and the applier hasn't published its pid yet — operators / tests
should retry.

Equivalent to `refresh_validator(Target, validator_refresh)`.
""").
-spec refresh_validator(instance_id() | pid()) ->
    ok | {error, applier_unavailable}.

refresh_validator(Target) ->
    refresh_validator(Target, validator_refresh).

?DOC("""
As `refresh_validator/1` but tags the refresh request with an
operator-supplied `Reason` term. The reason is logged by the applier
and emitted on the `[bondy_oplog, applier, validator_refresh]`
telemetry event so operators can correlate the refresh with whatever
upstream change triggered it (config push, key rotation, etc.).
""").
-spec refresh_validator(instance_id() | pid(), term()) ->
    ok | {error, applier_unavailable}.

refresh_validator(Target, Reason) ->
    case applier_pid_for(Target) of
        {ok, ApplierPid} ->
            bondy_oplog_applier:refresh_validator(ApplierPid, Reason);
        {error, _} = Err ->
            Err
    end.

?DOC("""
Reap the per-cell causal-context entries of permanently-retired origins
from this shard's projection (the dead-origin GC; PR-H, #24). A tier_2
CRDT carries one version-vector entry per origin that ever wrote a cell;
a decommissioned node leaves those entries behind forever — the one cost
that grows with cluster *churn*. This drops only the value-preserving
(causal-history-only) entries of the supplied `RetiredOrigins`, so the
projection's value is unchanged.

The library cannot know which origins are retired (membership is delegated
to the consumer, as with `bondy_oplog_peer_source` and
`bondy_oplog_origin_bans`); the operator supplies `RetiredOrigins` and
owns the obligation that they are permanently gone and causally stable
cluster-wide. The local value-preserving gate means even a premature call
cannot lose live data — it just reaps fewer entries and reports them. The
pass is idempotent. A no-op (`supported => false`) for a legacy fold /
tier_0 shard.

A reap rewrites the projection checkpoint, not the MST, so it is undone by
a subsequent **live re-bootstrap** (which re-folds the full MST) and skips
**fully-compacted** cells — re-run it after a re-bootstrap. Both are
bounded-by-churn, not convergence bugs; see
`bondy_oplog_applier:reap_origins_sync/2` for the full durability note.

Returns `{ok, Report}` (see `bondy_oplog_applier:reap_report/0`) or
`{error, applier_unavailable}` during a subtree restart.
""").
-spec reap_origins(instance_id() | pid(), [term()]) ->
    {ok, bondy_oplog_applier:reap_report()} | {error, term()}.

reap_origins(Target, RetiredOrigins) when is_list(RetiredOrigins) ->
    case applier_pid_for(Target) of
        {ok, ApplierPid} ->
            bondy_oplog_applier:reap_origins_sync(ApplierPid, RetiredOrigins);
        {error, _} = Err ->
            Err
    end.

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
Returns `{ok, Watermark, Checkpoint}` for the latest persisted
compaction checkpoint, or `not_found` if no compaction has run yet.
""").
-spec compaction_checkpoint(instance_id() | pid()) ->
    {ok, bondy_oplog_event:event_key(), term()} | not_found.

compaction_checkpoint(Target) when is_binary(Target) ->
    case bondy_oplog_registry:lookup(Target) of
        not_found -> error({noproc, {?MODULE, Target}});
        {ok, #{snapshot := undefined}} -> not_found;
        {ok, #{snapshot := {W, S}}} -> {ok, W, S}
    end;
compaction_checkpoint(Target) ->
    gen_server:call(target(Target), get_compaction_checkpoint).

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
`put_checkpoint` and the MST truncate. On the next start, events ≤ the
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

?DOC("""
Flips the instance bootstrap lifecycle to `live` (durably). Called by
`bondy_oplog_sync_session:bootstrap/3` after `load_snapshot/3` has
installed the peer snapshot and the watermark has been advanced.

Idempotent. Order matters: `mark_live/1` MUST be the **last** step in
the bootstrap completion sequence — the durable flag file is the
crash-recovery marker that "everything before me succeeded". See
`_design/catalogue_expansion_plan.md` §2.4.
""").
-spec mark_live(instance_id() | pid()) -> ok.

mark_live(InstanceId) when is_binary(InstanceId) ->
    case bondy_oplog_registry:lifecycle(InstanceId) of
        undefined ->
            %% Instance not yet published; nothing to flip. This is a
            %% race between `mark_live/1` and the instance's `init/1`.
            %% Callers driving bootstrap are expected to talk to a
            %% running instance — log and return ok so the call is a
            %% no-op rather than crash the caller.
            ?LOG_WARNING(#{
                description =>
                    "mark_live/1 called for an instance that has no "
                    "registry entry; treating as no-op",
                instance_id => InstanceId
            }),
            ok;
        Handle ->
            ok = bondy_oplog_bootstrap_lifecycle:mark_live(Handle),
            ok = nudge_applier(InstanceId)
    end;
mark_live(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, mark_live, infinity).

%% @private
%% Wake the applier immediately after a lifecycle transition so we
%% don't have to wait for the next 1s backstop tick. The applier
%% absorbs unsolicited `drain_resume` casts; the worst case if the
%% applier isn't running yet is the cast lands in a queue that gets
%% dropped on supervisor restart.
nudge_applier(InstanceId) ->
    case bondy_oplog_registry:applier_pid(InstanceId) of
        undefined ->
            ok;
        Pid when is_pid(Pid) ->
            ok = bondy_oplog_applier:notify_drain_resume(Pid),
            ok
    end.

?DOC("""
Returns the current bootstrap lifecycle state of an instance.
`pre_bootstrap` while the instance is waiting for a successful
`bondy_oplog_sync_session:bootstrap/3`; `live` once it can serve
fold-driven reads. `undefined` when the instance is not registered.
""").
-spec lifecycle_state(instance_id() | pid()) ->
    bondy_oplog_bootstrap_lifecycle:state() | undefined.

lifecycle_state(InstanceId) when is_binary(InstanceId) ->
    case bondy_oplog_registry:lifecycle(InstanceId) of
        undefined -> undefined;
        Handle -> bondy_oplog_bootstrap_lifecycle:state(Handle)
    end;
lifecycle_state(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, lifecycle_state, infinity).

?DOC("""
Installs a batch of catalogue-snapshot cells into the instance's
projection. Each cell is a `{Bucket, Key, Frame}` triple where `Frame`
is the V2 cell frame produced by the peer's projection adapter.

`Mode` is `replace` (fresh bootstrap) or `merge` (recovering
bootstrap). See `bondy_oplog_applier:install_catalogue_batch/2` for
the per-mode semantics. The arity-2 form is equivalent to
`install_catalogue_batch(Inst, {replace, Cells})`.

Returns `{ok, #{installed := _, skipped := _, merged := _,
replaced_no_merge := _}}`.

Called by `bondy_oplog_sync_session:bootstrap_catalogue/3` between
`get_catalogue_snapshot_init` and `finalize_catalogue_bootstrap/3`.
""").
-spec install_catalogue_batch(
    instance_id() | pid(),
    [bondy_oplog_transport:cell()]
    | {replace | merge, [bondy_oplog_transport:cell()]}
) ->
    {ok, #{
        installed := non_neg_integer(),
        skipped := non_neg_integer(),
        merged := non_neg_integer(),
        replaced_no_merge := non_neg_integer()
    }}
    | {error, term()}.

install_catalogue_batch(InstanceId, Cells) when
    is_binary(InstanceId), is_list(Cells)
->
    install_catalogue_batch(InstanceId, {replace, Cells});
install_catalogue_batch(InstanceId, {replace, Cells}) when
    is_binary(InstanceId)
->
    %% `replace` is the only mode — PR-G removed the CvRDT `merge` mode.
    %% Guard fails fast on a stray `{merge, _}` here rather than letting it
    %% reach the applier (which would function_clause).
    case bondy_oplog_registry:applier_pid(InstanceId) of
        undefined ->
            {error, instance_not_running};
        ApplierPid ->
            bondy_oplog_applier:install_catalogue_batch(
                ApplierPid, {replace, Cells}
            )
    end;
install_catalogue_batch(Pid, ModeAndCells) when is_pid(Pid) ->
    case bondy_oplog_registry:instance_id_by_sup_pid(Pid) of
        {ok, InstanceId} ->
            install_catalogue_batch(InstanceId, ModeAndCells);
        not_found ->
            {error, instance_not_found}
    end.

?DOC("""
Finalises a catalogue-snapshot bootstrap session.

For a fresh-bootstrap caller (`WasLive = false`) this marks the
instance `live` durably. For a recovering caller (`WasLive = true`)
the instance is already live and no lifecycle change is needed.

`Watermark` is the peer's high-water HLC at session start, captured
into the per-shard high-water atomic so future replicas reading from
this peer see at least that watermark. (In v1 this is informational —
high-water is advanced cell-by-cell during `install_catalogue_batch/2`
already; this call is the last-write barrier.)
""").
-spec finalize_catalogue_bootstrap(
    instance_id() | pid(),
    Watermark :: non_neg_integer(),
    WasLive :: boolean()
) -> ok.

finalize_catalogue_bootstrap(InstanceId, Watermark, WasLive) when
    is_binary(InstanceId),
    is_integer(Watermark),
    Watermark >= 0,
    is_boolean(WasLive)
->
    ok = maybe_advance_high_water(InstanceId, Watermark),
    case WasLive of
        true ->
            ok;
        false ->
            ok = mark_live(InstanceId)
    end;
finalize_catalogue_bootstrap(Pid, Watermark, WasLive) when is_pid(Pid) ->
    case bondy_oplog_registry:instance_id_by_sup_pid(Pid) of
        {ok, InstanceId} ->
            finalize_catalogue_bootstrap(InstanceId, Watermark, WasLive);
        not_found ->
            ok
    end.

%% @private — advance the per-shard high-water atomic for the
%% applier's `cell_apply_target`. No-op if the instance is not
%% catalogue-mode.
maybe_advance_high_water(_InstanceId, 0) ->
    ok;
maybe_advance_high_water(InstanceId, Watermark) ->
    case bondy_oplog_registry:applier_pid(InstanceId) of
        undefined ->
            ok;
        ApplierPid ->
            case bondy_oplog_applier:cell_apply_target(ApplierPid) of
                undefined ->
                    ok;
                {ok, {NS, Index, Shard}} ->
                    case bondy_db_core_registry:lookup(NS, Index, Shard) of
                        not_found ->
                            ok;
                        {ok, Entry} ->
                            Ref = bondy_db_core_registry:entry_high_water_ref(
                                Entry
                            ),
                            ok = bondy_oplog_high_water:advance(Ref, Watermark)
                    end
            end
    end.

%% =============================================================================
%% REGISTRY
%% =============================================================================

?DOC("""
Returns the pid of an instance owner gen_server, or `undefined` if no
such instance is currently running.
""").
-spec whereis(instance_id()) -> pid() | undefined.

whereis(InstanceId) when is_binary(InstanceId) ->
    case bondy_oplog_registry:instance_pid(InstanceId) of
        undefined ->
            undefined;
        Pid ->
            case is_process_alive(Pid) of
                true -> Pid;
                false -> undefined
            end
    end.

%% Binary-id callers go through the lock-free registry path; pid
%% callers pay a `gen_server:call` because the registry is keyed by
%% instance_id and a pid→id reverse lookup would cost an ETS
%% `select`. Pid callers are tests/internals only — see
%% `append_remote/2` and `origin/1` docstrings.
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
    %% Off-heap inbox: incoming messages land in their own heap
    %% fragments instead of the process heap, so minor GC does not
    %% scan them and the process heap stays small even when many
    %% callers pile up appends. Without this flag the writer cliff
    %% at >=16 concurrent appenders shows up as throughput regression
    %% — the gen_server's heap fragmenting under mailbox depth
    %% triggers frequent full-sweep GCs that stall every caller.
    process_flag(message_queue_data, off_heap),
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
    case maps:is_key(merge_strategy, Opts) of
        true ->
            ?LOG_WARNING(#{
                description =>
                    "`merge_strategy` opt is deprecated; configure "
                    "`fold_module` per FOLD_STRATEGY_DESIGN §6/§7 "
                    "instead. The legacy value is still honoured for "
                    "MST page-merge collisions",
                instance_id => InstanceId,
                merge_strategy => MergeMod
            });
        false ->
            ok
    end,
    {FoldMod, FoldOpts} = resolve_fold_config(InstanceId, Opts),
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
    %% Stage 5: compaction checkpoint + watermark recovery.
    %% Default backend resolution: prefer the file backend when the
    %% instance has any durable storage configured (`storage_path` or
    %% an explicit `compaction_checkpoint_opts.path`); otherwise fall
    %% back to ETS for ephemeral instances. A caller passing
    %% `compaction_checkpoint` explicitly always wins.
    CkptOpts0 = maps:get(compaction_checkpoint_opts, Opts, #{}),
    {CkptMod, CkptOpts} = resolve_checkpoint_backend(
        InstanceId, Opts, CkptOpts0
    ),
    {ok, CkptState} = CkptMod:init(InstanceId, CkptOpts),
    %% Single read covers both: the checkpoint envelope carries the
    %% watermark, so calling current_watermark/1 first is redundant
    %% and (on a durable backend) a wasted disk read.
    {Watermark, CachedCheckpoint} =
        case CkptMod:get_checkpoint(CkptState) of
            {ok, W0, S0} ->
                {W0, {W0, S0}};
            not_found ->
                {undefined, undefined};
            {error, CkptErr} ->
                error({compaction_checkpoint_corrupted, InstanceId, CkptErr})
        end,
    CrdtMod = maps:get(crdt_module, Opts, undefined),
    %% Seed HLC from the highest persisted event key, so a restart with
    %% a durable backend doesn't issue keys below the previous high
    %% water mark. Sources, in order of precedence:
    %%   1. The MST's max event key (live events past the watermark).
    %%   2. The compaction checkpoint's watermark.
    %% Fresh instances (ETS backend, no checkpoint) leave the HLC at 0.
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
    MaxLocalInstalledSeq =
        case max_local_seq(MST, Origin) of
            undefined ->
                0;
            MaxSeq ->
                ok = atomics:put(SeqRef, 1, MaxSeq),
                MaxSeq
        end,
    %% Per-instance overlay (`ordered_set`, public, owned by this
    %% gen_server). Rows are `{Key, Value, Hlc, Origin}`.
    %% `ordered_set` so range reads (`fold_range/5`, `first_key/1`,
    %% `latest_key/1`) can streaming-merge it with the MST in key
    %% order. `public` so the applier-driven eviction can run via
    %% `ets:select_delete/2` from any process. No heir — the table
    %% dies with this process; a one_for_all subtree restart creates
    %% a fresh one.
    Overlay = ets:new(bondy_oplog_overlay, [
        ordered_set,
        public,
        {keypos, ?OVERLAY_KEY_POS},
        {read_concurrency, true},
        {write_concurrency, true},
        {decentralized_counters, true}
    ]),
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
        fold_module = FoldMod,
        fold_opts = FoldOpts,
        crdt_module = CrdtMod,
        compaction_checkpoint = CkptMod,
        compaction_checkpoint_state = CkptState,
        watermark = Watermark,
        cached_checkpoint = CachedCheckpoint,
        max_working_set = maps:get(max_working_set, Opts, infinity),
        live_size = LiveSize,
        last_event_key = LastMSTKey,
        wal_pid = undefined,
        wal_pid_monitor = undefined,
        overlay = Overlay,
        max_overlay_events = maps:get(max_overlay_events, Opts, 10_000),
        max_overlay_bytes = maps:get(max_overlay_bytes, Opts, 5 * 1024 * 1024),
        overlay_throttle = maps:get(overlay_throttle, Opts, drop),
        overlay_counters = atomics:new(2, [{signed, false}]),
        max_local_installed_seq = MaxLocalInstalledSeq,
        install_in_flight = atomics:new(1, [{signed, false}]),
        max_install_in_flight = maps:get(max_install_in_flight, Opts, 16),
        install_coalesce_max = validate_coalesce_max(
            maps:get(install_coalesce_max, Opts, 16)
        ),
        lifecycle = bondy_oplog_bootstrap_lifecycle:open(InstanceId, Opts)
    },
    ok = publish(State),
    %% Publish the overlay tid via a dedicated setter so a stale tid
    %% from a previous instance (left behind in a registry row that
    %% outlived a one_for_all restart) is overwritten. Symmetric with
    %% `set_wal_pid/2` / `set_applier_pid/2`.
    ok = bondy_oplog_registry:set_overlay_tab(InstanceId, Overlay),
    %% Publish the substrate read-side AE targets (`MST_DB_DESIGN.md`
    %% §18 items 6 & 8). Top-level instance opt; immutable for the
    %% instance's lifetime. Validation is deferred to startup: a
    %% malformed list crashes init before any peer can interact.
    AeTargets = validate_ae_targets(maps:get(ae_targets, Opts, [])),
    ok = bondy_oplog_registry:set_ae_targets(InstanceId, AeTargets),
    %% Publish the lock-free `append_fast` bundle iff the validator
    %% advertises `is_stateless/0 -> true`. The bundle lets callers
    %% build an event, hit the WAL gen_server directly, and stage
    %% to the overlay without routing through this gen_server.
    ok = bondy_oplog_registry:set_fast_path(
        InstanceId, build_fast_path(State)
    ),
    %% Publish the flow-control handle. The applier reads this once
    %% at its own `init/1` (or lazily on first drain) and gates its
    %% `install_local_batch` dispatch on the cap. The instance owns
    %% the atomic — applier just shares the ref.
    ok = bondy_oplog_registry:set_install_in_flight(
        InstanceId,
        State#state.install_in_flight,
        State#state.max_install_in_flight
    ),
    %% Publish the bootstrap lifecycle handle. The applier reads this
    %% in its own `init/1` and gates the WAL drain on it. The handle is
    %% set once and never replaced — the atomic mirror inside it is
    %% flipped in place when the instance transitions to `live`.
    ok = bondy_oplog_registry:set_lifecycle(
        InstanceId, State#state.lifecycle
    ),
    {ok, State}.

%% @private
%% Returns a fast-path bundle map when the configured validator is
%% stateless (the `is_stateless/0` optional callback returns `true`),
%% or `undefined` otherwise. The bundle is consumed by
%% `bondy_oplog_instance:append_fast/2,3`.
build_fast_path(#state{validator_module = ValidatorMod} = State) ->
    case validator_is_stateless(ValidatorMod) of
        true ->
            #{
                hlc => State#state.hlc,
                seq => State#state.seq,
                overlay_counters => State#state.overlay_counters,
                origin => State#state.origin,
                validator_module => ValidatorMod,
                validator_state => State#state.validator_state,
                max_overlay_events => State#state.max_overlay_events,
                max_overlay_bytes => State#state.max_overlay_bytes,
                max_working_set => State#state.max_working_set,
                overlay_throttle => State#state.overlay_throttle
            };
        false ->
            undefined
    end.

%% @private
validator_is_stateless(Mod) ->
    erlang:function_exported(Mod, is_stateless, 0) andalso
        Mod:is_stateless().

%% @private
%% Validate `ae_targets :: [{atom(), atom(), non_neg_integer()}]` at
%% startup so a typo in the supervisor child-spec surfaces as an init
%% crash rather than a silent freshness regression at the first AE
%% round. Returns the (unchanged) list on success and raises on the
%% first malformed entry; `init/1` is wrapped by the supervisor so the
%% error reaches the caller cleanly.
validate_ae_targets(Targets) when is_list(Targets) ->
    lists:foreach(fun assert_ae_target/1, Targets),
    Targets;
validate_ae_targets(Other) ->
    error({invalid_ae_targets, Other}).

assert_ae_target({NS, Index, Shard}) when
    is_atom(NS),
    is_atom(Index),
    is_integer(Shard),
    Shard >= 0
->
    ok;
assert_ae_target(Bad) ->
    error({invalid_ae_target, Bad}).

handle_call(Req, From, State0) ->
    Result = do_handle_call(Req, From, State0),
    ok = maybe_publish(State0, Result),
    Result.

%% @private
%% Publishes the registry row when the handle_call clause changed any
%% field exposed to lock-free readers. We compare only the
%% *published* subset (see `published_fingerprint/1`) because state
%% fields that no reader sees — e.g. the in-process overlay counters
%% maintained by `stage_to_overlay/2` and `evict_overlay_batch/2` —
%% would otherwise force a registry write on every append. Under
%% mixed read/write load that turned a 4 k/s writer into a
%% bottleneck on the registry row's per-key lock bucket and dragged
%% writer throughput by ~7×.
maybe_publish(State0, {reply, _, State1}) ->
    maybe_publish_diff(State0, State1);
maybe_publish(State0, {noreply, State1}) ->
    maybe_publish_diff(State0, State1);
maybe_publish(_State0, _Result) ->
    ok.

maybe_publish_diff(State0, State1) ->
    case published_fingerprint(State0) =:= published_fingerprint(State1) of
        true -> ok;
        false -> publish(State1)
    end.

%% @private
%% Tuple of the fields that `publish/1` writes to the registry. Used
%% by `maybe_publish/2` to skip the ETS write when nothing visible to
%% lock-free readers has changed. `instance_id` and `origin` are
%% immutable post-init so they are not included.
published_fingerprint(#state{} = S) ->
    {
        S#state.mst,
        S#state.watermark,
        S#state.cached_checkpoint,
        S#state.crdt_module,
        S#state.fold_module,
        S#state.fold_opts,
        S#state.live_size
    }.

%% @private
do_handle_call({append, Op, Meta}, _From, State0) ->
    %% Pressure check → WAL append (fsync) → overlay insert → reply.
    %% The reply happens inline as soon as the WAL is durable and
    %% the overlay row exists; the applier drains the WAL and casts
    %% `install_local_batch` back to this gen_server, which promotes
    %% the event to the MST and evicts the overlay row.
    case admit(State0, 1) of
        ok ->
            case ensure_wal_pid(State0) of
                {ok, WalPid, State1} ->
                    case do_append_local(State1, WalPid, [{Op, Meta}]) of
                        {ok, [Key], State2} ->
                            {reply, Key, State2};
                        {error, wal_unavailable} ->
                            {reply, {error, wal_unavailable},
                                invalidate_wal_pid(State1)};
                        {error, _} = Err ->
                            {reply, Err, State1}
                    end;
                {error, _} = Err ->
                    {reply, Err, State0}
            end;
        {error, _} = Err ->
            {reply, Err, State0}
    end;
do_handle_call({append_many, Items}, _From, State0) ->
    case admit(State0, length(Items)) of
        ok ->
            case ensure_wal_pid(State0) of
                {ok, WalPid, State1} ->
                    case do_append_local(State1, WalPid, Items) of
                        {ok, Keys, State2} ->
                            {reply, Keys, State2};
                        {error, wal_unavailable} ->
                            {reply, {error, wal_unavailable},
                                invalidate_wal_pid(State1)};
                        {error, _} = Err ->
                            {reply, Err, State1}
                    end;
                {error, _} = Err ->
                    {reply, Err, State0}
            end;
        {error, _} = Err ->
            {reply, Err, State0}
    end;
do_handle_call(get_validator, _From, State) ->
    {reply, {State#state.validator_module, State#state.validator_state}, State};
do_handle_call(drain_install_queue, _From, State) ->
    %% Synchronisation barrier for the applier's commit boundary.
    %% Calls jump past casts in the mailbox order, so by the time
    %% this call is processed, every prior `install_local_batch`
    %% cast has been handled. The reply itself carries no payload.
    {reply, ok, State};
do_handle_call(await_overlay_drained, From, State) ->
    %% Event-driven `await_apply/1,2`. Reply inline when the overlay
    %% is already empty; otherwise queue the caller and let
    %% `maybe_signal_drain_waiters/1` reply when an install handler
    %% next observes overlay_size == 0. The call has already jumped
    %% past pending `install_local_batch` casts (gen_server mailbox
    %% order), so the visible overlay size here reflects every event
    %% the applier has already dispatched to this instance.
    case overlay_size_tab(State#state.overlay) of
        0 ->
            {reply, ok, State};
        _ ->
            Waiters = State#state.drain_waiters,
            {noreply, State#state{drain_waiters = [From | Waiters]}}
    end;
do_handle_call({install_remote, Event}, _From, State0) ->
    %% Sole install path for peer-received events. Signature
    %% verification ran in the applier process (see
    %% `bondy_oplog_applier:enqueue_remote/2`) before this call, so
    %% we trust the event and run the remaining accept/reject checks:
    %% origin-ban, backpressure, watermark filter, and the
    %% `bondy_mst:get` three-way (undefined/match/equivocation).
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
                    case do_append_remote(State0, Event) of
                        {ok, State} ->
                            {reply, ok, State};
                        {error, _} = Error ->
                            {reply, Error, State0}
                    end
            end
    end;
do_handle_call({get, Key}, _From, #state{mst = MST, overlay = Overlay} = State) ->
    %% pid-targeted path: same overlay-first → MST order as the
    %% lock-free `get/2`.
    Reply =
        case overlay_lookup_tab(Overlay, Key) of
            {ok, _} = Hit ->
                Hit;
            not_found ->
                case bondy_mst:get(MST, Key) of
                    undefined -> not_found;
                    Value -> {ok, event_from_value(Key, Value)}
                end
        end,
    {reply, Reply, State};
do_handle_call(root_hash, _From, #state{mst = MST} = State) ->
    {reply, bondy_mst:root(MST), State};
do_handle_call(
    {fold_range, From, To, Fun, Acc0},
    _From,
    #state{mst = MST, overlay = Overlay} = State
) ->
    %% Streaming merge with strict ascending key order; overlay wins
    %% on conflict. The overlay queue is materialised once
    %% (`ets:select` on `ordered_set` yields key-ordered rows) and
    %% drained as the MST fold walks.
    OverlayQueue = overlay_range_tab(Overlay, From, To),
    Result = fold_range_merged(MST, From, To, OverlayQueue, Fun, Acc0),
    {reply, Result, State};
do_handle_call({truncate_prefix, Watermark}, _From, #state{mst = MST0} = State) ->
    %% Operator-driven prefix removal: structurally drop every key
    %% `=< Watermark` via `bondy_mst:truncate/2` (an O(log N) left-spine
    %% rewrite), counting the removed events first for `live_size`
    %% bookkeeping.
    %%
    %% Also advances `state.watermark` so the receive-side filter in
    %% `do_append_remote/2` rejects peer events with HLC ≤ Watermark.
    %% Without this, peers that have not yet seen the truncate would
    %% keep re-shipping the events we just dropped, defeating the
    %% purpose of the call. No snapshot is written at the new
    %% watermark — operator-driven truncate is documented as lossy for
    %% bootstrap consumers (see `bondy_oplog:truncate_prefix/2`). The
    %% watermark advance is monotone: a Watermark lower than the
    %% current `state.watermark` is ignored so compaction-set values
    %% are never regressed.
    Removed = count_in_open_range(MST0, undefined, Watermark),
    MST1 = bondy_mst:truncate(MST0, Watermark),
    NewWatermark = advance_watermark(State#state.watermark, Watermark),
    _ = bondy_oplog_hlc:update(
        State#state.hlc, bondy_oplog_event:key_hlc(Watermark)
    ),
    {reply, Removed, State#state{
        mst = MST1,
        live_size = max(0, State#state.live_size - Removed),
        watermark = NewWatermark
    }};
do_handle_call(instance_size, _From, State) ->
    %% `live_size` + overlay row count, read in the same handler so
    %% no `install_local_batch` cast can interleave between the two —
    %% see `size/1`. Slot 1 of `overlay_counters` is shared with
    %% `append_fast/2,3`, so it can transiently grow during this read
    %% (a concurrent caller appending) but cannot shrink (only this
    %% gen_server's `evict_overlay_batch/2` decrements it). The
    %% returned value is therefore monotone over the read window.
    Total =
        State#state.live_size +
            atomics:get(State#state.overlay_counters, 1),
    {reply, Total, State};
do_handle_call(origin, _From, State) ->
    {reply, State#state.origin, State};
do_handle_call(first_key, _From, #state{mst = MST, overlay = Overlay} = State) ->
    Reply = merge_first_key_tab(Overlay, MST),
    {reply, Reply, State};
do_handle_call(latest_key, _From, #state{mst = MST, overlay = Overlay} = State) ->
    Reply = merge_latest_key_tab(Overlay, MST),
    {reply, Reply, State};
do_handle_call(instance_id, _From, State) ->
    {reply, State#state.instance_id, State};
do_handle_call(info, _From, State) ->
    Info = #{
        instance_id => State#state.instance_id,
        origin => State#state.origin,
        backend => State#state.backend,
        validator => State#state.validator_module,
        merge_strategy => State#state.merge_strategy,
        fold_module => State#state.fold_module,
        fold_opts => State#state.fold_opts,
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
    %% Re-seed `max_local_installed_seq` from the merged tree. A sync
    %% can echo our own local events back to us (peer pulled them
    %% from us earlier, then we pull our originated pages back in via
    %% `pull_until_complete` → `integrate_peer_root`). When the
    %% applier later dispatches those same WAL entries to
    %% `install_local_batch`, the `is_fast_install` predicate uses
    %% this watermark to decide between the fast install (blindly
    %% bumps `live_size`) and the slow safe install (checks the MST
    %% first). Without this refresh, the fast path double-bumps
    %% `live_size` for the echoed event — visible to `size/1` as an
    %% off-by-N overcount.
    MaxLocalSeq =
        case max_local_seq(MST2, State#state.origin) of
            undefined -> State#state.max_local_installed_seq;
            S -> erlang:max(S, State#state.max_local_installed_seq)
        end,
    %% Sync produced new events in the local MST; ask the applier to
    %% re-fold the cell_apply projection so peer-authored events become
    %% visible to `bondy_db:read/3`. No-op when the instance was
    %% started without a `cell_apply_target` (the applier's
    %% `cell_apply_ctx` is `undefined` and the cast falls through).
    %% Cast — best-effort; the next sync tick re-arms the request if
    %% the applier was busy.
    case bondy_oplog_registry:applier_pid(State#state.instance_id) of
        undefined ->
            ok;
        ApplierPid when is_pid(ApplierPid) ->
            bondy_oplog_applier:replay_cell_events(ApplierPid)
    end,
    {reply, ok, State#state{
        mst = MST2,
        live_size = compute_live_size(MST2),
        max_local_installed_seq = MaxLocalSeq
    }};
do_handle_call(current_watermark, _From, State) ->
    {reply, State#state.watermark, State};
do_handle_call(crdt_module, _From, State) ->
    {reply, State#state.crdt_module, State};
do_handle_call(get_compaction_checkpoint, _From, State) ->
    Reply = (State#state.compaction_checkpoint):get_checkpoint(
        State#state.compaction_checkpoint_state
    ),
    {reply, Reply, State};
do_handle_call({compact, PeerRoots}, _From, State) ->
    do_compact_sync(State, PeerRoots);
do_handle_call({load_snapshot, NewWatermark, Snapshot}, _From, State) ->
    do_load_snapshot(State, NewWatermark, Snapshot);
do_handle_call(
    mark_live,
    _From,
    #state{lifecycle = LC, instance_id = Id} = State
) ->
    ok = bondy_oplog_bootstrap_lifecycle:mark_live(LC),
    ok = nudge_applier(Id),
    {reply, ok, State};
do_handle_call(lifecycle_state, _From, #state{lifecycle = LC} = State) ->
    {reply, bondy_oplog_bootstrap_lifecycle:state(LC), State};
do_handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast({install_local_batch, Events}, State0) ->
    %% Sole dispatch path for local-event MST installs. The applier
    %% verifies signatures in its own process and casts the surviving
    %% events here. We fold `install_event` over them in WAL order,
    %% publish once at the end (one ETS write per batch), then
    %% HLC-conditionally evict the matching overlay rows. MST publish
    %% strictly precedes overlay evict so a reader missing the
    %% overlay row finds the entry in the MST instead.
    %%
    %% A4 — instance-side install coalescing. When the applier outruns
    %% the instance, several `install_local_batch` casts queue in the
    %% mailbox while we are mid-`put_batch`. We drain the queued ones
    %% (up to `install_coalesce_max`) and merge every cast's events into
    %% a SINGLE `put_batch` + publish + overlay-evict, amortising the
    %% O(log n) spine rebuild — the dominant per-event durable cost
    %% (A0b) — over many casts' worth of events.
    %%
    %% The drain matches only `install_local_batch` casts and preserves
    %% their FIFO (= WAL = HLC) order. It may skip past queued peer
    %% `install_remote` / `drain_install_queue` / `await_overlay_drained`
    %% *calls*; this is convergence-safe: local and peer events have
    %% disjoint MST keys (different origin) and disjoint per-origin
    %% watermarks, so a reordered local-ahead-of-peer install yields the
    %% same final MST (merge is commutative/idempotent); overlay evict is
    %% per-event HLC-conditional (order-independent); and the local
    %% applier blocks on `drain_install_queue`, so no install cast is
    %% ever queued behind that barrier (the barrier cannot be skipped).
    {EventsRev, NCasts} = drain_install_casts(
        [Events], 1, State0#state.install_coalesce_max
    ),
    AllEvents = lists:append(lists:reverse(EventsRev)),
    State1 = install_local_batch(State0, AllEvents),
    ok = publish(State1),
    State2 = evict_overlay_batch(State1, AllEvents),
    State3 = maybe_signal_drain_waiters(State2),
    ok = release_install_slots(State3, NCasts),
    {noreply, State3};
handle_cast(check_drain_waiters, State) ->
    %% Sent by the applier after `evict_rejected_overlay/2` evicts
    %% events for which no `install_local_batch` cast will be issued
    %% (the verify step rejected the whole batch). Without this hint a
    %% caller blocked in `await_overlay_drained` would wait until the
    %% next install batch shrank the overlay, even though the overlay
    %% is already empty.
    {noreply, maybe_signal_drain_waiters(State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(
    {'DOWN', Ref, process, _Pid, _Reason},
    #state{wal_pid_monitor = Ref} = State
) ->
    %% Cached WAL pid has gone down (one_for_all restart); drop the
    %% cache so the next append re-resolves the new pid via the
    %% registry.
    {noreply, State#state{wal_pid = undefined, wal_pid_monitor = undefined}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{
    mst = MST,
    compaction_checkpoint = CkptMod,
    compaction_checkpoint_state = CkptState,
    overlay = Overlay
}) ->
    %% Leave the registry row in place so that on a one_for_all subtree
    %% restart the dyn_sup mapping (`sup_pid`) survives. The row's
    %% `instance_pid` field will be stale until the new instance
    %% gen_server's init runs and republishes; lock-free read paths
    %% use `is_process_alive/1` to detect that case.
    _ = catch CkptMod:close(CkptState),
    _ = catch bondy_mst:destroy(MST),
    %% Drop the overlay — it dies with the instance, no heir, no
    %% survival across subtree restart. The applier reads the tid
    %% from the registry, and the registry row's `overlay_tab`
    %% becomes stale here until the next `init/1` republishes a
    %% fresh one. Applier-side reads tolerate `undefined`.
    _ = catch ets:delete(Overlay),
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
%% Builds a signed event for each `{Op, Meta}` item, hands the resulting
%% list to the per-instance WAL as a single atomic batch frame, then
%% inserts the events into the per-instance overlay so the caller's
%% next read sees them. The MST is **not** mutated here — the per-
%% instance applier drains the WAL, re-verifies each event's
%% signature, and casts `install_local_batch` back to this gen_server
%% which performs the actual MST install and overlay eviction. The
%% overlay row closes the read-your-writes gap until the applier
%% catches up; the row is evicted via HLC-conditional
%% `ets:select_delete/2` once the install lands.
do_append_local(#state{} = State0, WalPid, Items) ->
    {Events, Keys, State1} = build_events(State0, Items),
    %% Stage overlay rows BEFORE the WAL append. The applier reads
    %% from the WAL the instant `append_batch/2` durably commits;
    %% if we staged after, the applier could send
    %% `install_local_batch` before the overlay row exists — its
    %% `evict_overlay_batch/2` would then run as a no-op, and a
    %% later overlay insert would leave an orphan row whose count
    %% inflates `size/1`. Staging first guarantees the row is
    %% visible the moment the WAL entry is.
    State2 = stage_to_overlay(State1, Events),
    try bondy_oplog_wal:append_batch(WalPid, Events) of
        {ok, _Entries} ->
            telemetry:execute(
                [bondy_oplog, instance, append],
                #{count => length(Events)},
                #{instance_id => State2#state.instance_id}
            ),
            {ok, Keys, State2};
        {error, _} = E ->
            %% WAL rejected the batch — roll back the overlay rows
            %% so they cannot be served as a phantom write.
            ok = unstage_overlay(State2, Events),
            E
    catch
        exit:{noproc, _} ->
            ok = unstage_overlay(State2, Events),
            {error, wal_unavailable};
        exit:noproc ->
            ok = unstage_overlay(State2, Events),
            {error, wal_unavailable};
        exit:{normal, _} ->
            ok = unstage_overlay(State2, Events),
            {error, wal_unavailable};
        exit:{shutdown, _} ->
            ok = unstage_overlay(State2, Events),
            {error, wal_unavailable}
    end.

%% @private
%% Undo the effect of `stage_to_overlay/2` for a batch whose WAL
%% append did not succeed. Deletes every overlay row by key and
%% decrements the shared counters by the same amount they were
%% bumped — keeping the counters and the table in lockstep.
unstage_overlay(#state{overlay = undefined}, _Events) ->
    ok;
unstage_overlay(#state{overlay = Tab, overlay_counters = Ctrs}, Events) ->
    lists:foreach(
        fun(E) -> ets:delete(Tab, bondy_oplog_event:key(E)) end,
        Events
    ),
    overlay_counters_sub(Ctrs, length(Events)),
    ok.

%% @private
%% Inserts every event in the batch into the per-instance overlay as
%% one atomic `ets:insert/2` call and bumps the in-state size +
%% byte-estimate counters used by `overlay_admit/2`. Origin is `local`
%% for events that went through the WAL; a future eager-push receiver
%% will insert with `eager_pushed` so the applier's eviction protocol
%% can distinguish the two (§10.3 of the applier design).
stage_to_overlay(
    #state{overlay = Overlay, overlay_counters = Ctrs} = State, Events
) ->
    Rows = [overlay_row(E, local) || E <- Events],
    true = ets:insert(Overlay, Rows),
    overlay_counters_add(Ctrs, Events),
    State.

%% @private
%% Adds the count + byte delta of `Events` into the shared
%% `overlay_counters` atomics. Called from this gen_server and from
%% the `append_fast/2,3` caller-side path.
overlay_counters_add(Ctrs, Events) ->
    {DeltaCount, DeltaBytes} = overlay_delta(Events),
    ok = atomics:add(Ctrs, 1, DeltaCount),
    ok = atomics:add(Ctrs, 2, DeltaBytes),
    ok.

%% @private
overlay_counters_get(Ctrs) ->
    {atomics:get(Ctrs, 1), atomics:get(Ctrs, 2)}.

%% @private
%% Counts events and sums their approximate on-heap size via
%% `erlang:external_size/1` — faster than `term_to_binary` because it
%% does not allocate. The result is used purely for backpressure
%% accounting; exactness is not required.
overlay_delta(Events) ->
    lists:foldl(
        fun(E, {Count, Bytes}) ->
            {Count + 1, Bytes + erlang:external_size(E)}
        end,
        {0, 0},
        Events
    ).

%% @private
overlay_row(Event, Origin) ->
    Key = bondy_oplog_event:key(Event),
    {
        Key,
        value_from_event(Event),
        bondy_oplog_event:key_hlc(Key),
        Origin
    }.

%% @private
%% Allocates a fresh `{HLC, Origin, Seq}` for each item, signs the
%% event via the configured validator, and threads the validator state
%% forward. Returns `{Events, Keys, NewState}`.
build_events(State0, Items) ->
    {EventsRev, KeysRev, State1} = lists:foldl(
        fun({Op, Meta}, {EvAcc, KAcc, S0}) ->
            HLC = bondy_oplog_hlc:now(S0#state.hlc),
            Seq = atomics:add_get(S0#state.seq, 1, 1),
            Key = bondy_oplog_event:key(HLC, S0#state.origin, Seq),
            Event0 = bondy_oplog_event:new(Key, Op, Meta),
            {Signed, VS1} =
                (S0#state.validator_module):sign_event(
                    Event0, S0#state.validator_state
                ),
            {[Signed | EvAcc], [Key | KAcc], S0#state{validator_state = VS1}}
        end,
        {[], [], State0},
        Items
    ),
    {lists:reverse(EventsRev), lists:reverse(KeysRev), State1}.

%% @private
%% Sole MST-install path for local-origin events. Driven by the
%% `install_local_batch` cast from the per-instance applier. The
%% applier has already re-verified every event's signature in its
%% own process before dispatching, so this fold trusts the input.
%%
%% The batch is split by `max_local_installed_seq`:
%%
%% 1. **Fast suffix** — events with local origin and `Seq` strictly
%%    greater than the cached max. The seq atomic is monotonic per
%%    origin so these keys cannot yet be in the tree. We collect
%%    them into a list of `{Key, Value}` pairs and install them via
%%    `bondy_mst:put_batch/2`, which builds a small in-process MST
%%    from the batch and merges it into the live tree in a single
%%    traversal — one spine rebuild for the whole batch instead of
%%    one per event.
%% 2. **Slow prefix** — events whose seq has already been observed
%%    (resume-overlap) or that carry a non-local origin. These fall
%%    back to the per-event safety path that probes the tree with
%%    `bondy_mst:get` and either re-applies idempotently, ignores a
%%    matching value, or records an equivocation.
%%
%% Within a single WAL frame the local-origin seqs are contiguous and
%% strictly increasing (one batch from one writer), so the partition
%% is a single `lists:splitwith/2`. The merger configured on the live
%% tree is never invoked from the fast suffix because every key is
%% guaranteed new — the only callers of put_batch here are batches
%% that the seq filter has already promised contain no collisions.
-spec install_local_batch(#state{}, [bondy_oplog_event:t()]) -> #state{}.

install_local_batch(State, []) ->
    State;
install_local_batch(#state{} = State0, Events) ->
    Origin = State0#state.origin,
    MaxSeq = State0#state.max_local_installed_seq,
    {Slow, Fast} =
        lists:splitwith(
            fun(E) -> not is_fast_install(E, Origin, MaxSeq) end,
            Events
        ),
    State1 = install_slow_events(State0, Slow),
    install_fast_events(State1, Fast).

%% @private
is_fast_install(Event, Origin, MaxSeq) ->
    Key = bondy_oplog_event:key(Event),
    bondy_oplog_event:key_origin(Key) =:= Origin andalso
        bondy_oplog_event:key_seq(Key) > MaxSeq.

%% @private
install_slow_events(State, []) ->
    State;
install_slow_events(State0, [Event | Rest]) ->
    Key = bondy_oplog_event:key(Event),
    NewValue = value_from_event(Event),
    State1 = install_local_safe(State0, Event, Key, NewValue),
    install_slow_events(State1, Rest).

%% @private
%% Bulk-install a list of known-new local-origin events. Builds a
%% `{Key, Value}` list, drives `bondy_mst:put_batch/2`, then updates
%% the cached aggregates (`max_local_installed_seq`, `last_event_key`,
%% `live_size`) and bumps the HLC once for the maximum HLC seen.
%% Emits a single batch-level telemetry event with `count => N`.
install_fast_events(State, []) ->
    State;
install_fast_events(#state{} = State0, Events) ->
    {Pairs, MaxSeq, MaxKey, MaxHlc, Count} = scan_fast_events(Events, State0),
    InstallT0 = erlang:monotonic_time(microsecond),
    MST1 = bondy_mst:put_batch(State0#state.mst, Pairs),
    InstallUs = erlang:monotonic_time(microsecond) - InstallT0,
    _ = bondy_oplog_hlc:update(State0#state.hlc, MaxHlc),
    %% Mirror the HLC update for the local-Seq atomic. Rebuilding the
    %% MST from the WAL on restart (init seeds SeqRef from
    %% `max_local_seq/2`, which returns `undefined` for an empty MST)
    %% would otherwise leave SeqRef at 0 while WAL-replayed events
    %% carry seqs 1..N. A concurrent `append_fast/3` mid-replay would
    %% then allocate a colliding seq and the resulting event would
    %% clobber a pre-restart event at the same `{HLC, Origin, Seq}`
    %% key. CAS-loop because concurrent local appenders can race here.
    ok = maybe_bump_seq_atomic(State0#state.seq, MaxSeq),
    telemetry:execute(
        [bondy_oplog, instance, apply_event, ok],
        #{count => Count},
        #{instance_id => State0#state.instance_id, new => true}
    ),
    %% Per-batch wall-time of the MST install — the single
    %% `bondy_mst:put_batch/2` spine rebuild that writes into the
    %% pluggable store. For the pack-store backend this is the durable
    %% MST page churn, and it runs in THIS (instance) process, off the
    %% applier's critical path: the applier's `batch_install_cast` only
    %% times the async cast, so without this event the pack-store cost is
    %% invisible. `count` lets handlers derive per-event install cost and
    %% the mean batch size (`count / calls`).
    telemetry:execute(
        [bondy_oplog, instance, mst_install],
        #{duration_us => InstallUs, count => Count},
        #{instance_id => State0#state.instance_id}
    ),
    State0#state{
        mst = MST1,
        max_local_installed_seq = MaxSeq,
        last_event_key = greater_key(State0#state.last_event_key, MaxKey),
        live_size = State0#state.live_size + Count
    }.

%% @private
%% Lift `SeqRef` to at least `Target`. No-op when the atomic is
%% already at or above `Target`. Concurrent appenders that allocate a
%% higher seq mid-call land naturally on the no-op branch on retry.
maybe_bump_seq_atomic(_SeqRef, 0) ->
    ok;
maybe_bump_seq_atomic(SeqRef, Target) ->
    Cur = atomics:get(SeqRef, 1),
    case Target =< Cur of
        true ->
            ok;
        false ->
            case atomics:compare_exchange(SeqRef, 1, Cur, Target) of
                ok ->
                    ok;
                _Observed ->
                    maybe_bump_seq_atomic(SeqRef, Target)
            end
    end.

%% @private
%% Single pass over the fast-suffix events: builds the `{Key, Value}`
%% list (in batch order), tracks the maximum seq / key / HLC, and
%% counts the events. `lists:foldl/3` accumulates the pair list in
%% reverse, so the result is flipped before return — preserving the
%% per-event path's observable ordering for any telemetry handlers
%% downstream.
scan_fast_events(Events, #state{max_local_installed_seq = StartSeq}) ->
    {PairsRev, MaxSeq, MaxKey, MaxHlc, Count} =
        lists:foldl(
            fun(Event, {PairsAcc, SeqAcc, KeyAcc, HlcAcc, N}) ->
                Key = bondy_oplog_event:key(Event),
                Value = value_from_event(Event),
                Seq = bondy_oplog_event:key_seq(Key),
                Hlc = bondy_oplog_event:key_hlc(Key),
                {
                    [{Key, Value} | PairsAcc],
                    erlang:max(SeqAcc, Seq),
                    greater_key(KeyAcc, Key),
                    erlang:max(HlcAcc, Hlc),
                    N + 1
                }
            end,
            {[], StartSeq, undefined, 0, 0},
            Events
        ),
    {lists:reverse(PairsRev), MaxSeq, MaxKey, MaxHlc, Count}.

%% @private
%% Returns the greater of two event keys, treating `undefined` as
%% absent. Distinct from `max_key/2` further down (which uses the
%% `{ok, _} | empty` envelope expected by readers).
greater_key(undefined, K) -> K;
greater_key(K, undefined) -> K;
greater_key(A, B) when A > B -> A;
greater_key(_A, B) -> B.

%% @private
%% Safety path used for resume-overlap and any non-local seq event.
%% Probes the MST so an existing entry is detected as either an
%% idempotent re-apply (same value) or an equivocation (different
%% value at the same key).
install_local_safe(State, Event, Key, NewValue) ->
    case bondy_mst:get(State#state.mst, Key) of
        undefined ->
            install_event(State, Key, NewValue, apply_event, true);
        NewValue ->
            install_event(State, Key, NewValue, apply_event, false);
        ExistingValue ->
            record_equivocation(State, Key, ExistingValue, Event),
            State
    end.

%% @private
%% Evicts every overlay row for a freshly-installed batch via N
%% O(log N) `ets:delete/2` point deletes — the overlay is an
%% `ordered_set` so a deletion keyed by the row's primary key uses
%% the index. The earlier approach built an N-way OR guard for a
%% single `ets:select_delete/2`, but the resulting match spec did not
%% pin the key in the head and so triggered a full table scan; on a
%% 10k-row overlay with batches of 100 events that pattern cost
%% ~1M comparisons per call. Point deletes drop that to ~1.3k.
%%
%% No HLC guard is needed: an event key `{Hlc, Origin, Seq}` is
%% globally unique by construction (HLC and Seq are atomics; Origin
%% is per-instance), so an overlay row at that key corresponds to
%% exactly this event and no other.
-spec evict_overlay_batch(#state{}, [bondy_oplog_event:t()]) -> #state{}.

evict_overlay_batch(#state{overlay = undefined} = State, _Events) ->
    State;
evict_overlay_batch(State, []) ->
    State;
evict_overlay_batch(
    #state{overlay = Tab, overlay_counters = Ctrs} = State, Events
) ->
    Count =
        try
            lists:foreach(
                fun(E) ->
                    ets:delete(Tab, bondy_oplog_event:key(E))
                end,
                Events
            ),
            length(Events)
        catch
            error:badarg -> 0
        end,
    overlay_counters_sub(Ctrs, Count),
    State.

%% @private
%% Removes `Deleted` events from the overlay counters atomics. The
%% byte delta is computed proportionally to the surviving fraction —
%% same approximation the prior in-state version used. We snapshot
%% both slots together, compute the new pair, and write each slot
%% individually with `atomics:put/3`; a concurrent `append_fast/3`
%% that races between the two writes can transiently underestimate
%% byte usage by one increment, which is acceptable (backpressure
%% accounting is best-effort, not exact).
overlay_counters_sub(_Ctrs, 0) ->
    ok;
overlay_counters_sub(Ctrs, Deleted) ->
    OldCount = atomics:get(Ctrs, 1),
    OldBytes = atomics:get(Ctrs, 2),
    NewCount = max(0, OldCount - Deleted),
    NewBytes =
        case OldCount of
            0 -> 0;
            _ -> OldBytes * NewCount div OldCount
        end,
    ok = atomics:put(Ctrs, 1, NewCount),
    ok = atomics:put(Ctrs, 2, NewBytes),
    ok.

%% @private
%% Returns `{ok, WalPid, State1}` with `State1` carrying a monitored
%% cached pid so subsequent appends skip the registry lookup. If the
%% registry does not yet have a `wal_pid` (subtree mid-restart), the
%% cache is left empty and the caller surfaces `{error, wal_unavailable}`.
ensure_wal_pid(#state{wal_pid = Pid} = State) when is_pid(Pid) ->
    {ok, Pid, State};
ensure_wal_pid(#state{instance_id = Id} = State) ->
    case bondy_oplog_registry:wal_pid(Id) of
        undefined ->
            {error, wal_unavailable};
        Pid when is_pid(Pid) ->
            Ref = erlang:monitor(process, Pid),
            {ok, Pid, State#state{wal_pid = Pid, wal_pid_monitor = Ref}}
    end.

%% @private
%% Drops the cached WAL pid + its monitor. Used after a synchronous
%% append surfaces `noproc` so the next append rolls forward to the
%% new writer once the supervisor brings it up.
invalidate_wal_pid(#state{wal_pid_monitor = undefined} = State) ->
    State#state{wal_pid = undefined};
invalidate_wal_pid(#state{wal_pid_monitor = Ref} = State) ->
    _ = erlang:demonitor(Ref, [flush]),
    State#state{wal_pid = undefined, wal_pid_monitor = undefined}.

%% @private
%% Install path for peer-received events. The applier has already
%% re-verified the signature in its own process before forwarding
%% here, so this function trusts the input and runs the remaining
%% accept/reject logic:
%%
%% - Idempotent below-watermark filter (compaction may have advanced
%%   past this key already).
%% - `bondy_mst:get` three-way:
%%   - `undefined`: fresh insert.
%%   - bit-identical existing value: idempotent re-receive, no-op.
%%   - different existing value: equivocation; record proof in the
%%     quarantine table, leave the MST unchanged, return
%%     `{error, equivocation_detected}`. Keeping the gen_server alive
%%     on bad input avoids a crash-loop on poisoned peer traffic.
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
                    {ok,
                        install_event(
                            State, Key, NewValue, append_remote, true
                        )};
                NewValue ->
                    %% Idempotent re-receive (bit-identical).
                    {ok,
                        install_event(
                            State, Key, NewValue, append_remote, false
                        )};
                ExistingValue ->
                    record_equivocation(State, Key, ExistingValue, Event),
                    {error, equivocation_detected}
            end
    end.

%% @private
%% Shared insert path for `install_local_batch` (local-origin events
%% dispatched by the applier after S1 re-verify) and `do_append_remote`
%% (peer-received events). Mutates the MST, refreshes `last_event_key`
%% and `live_size`, advances the HLC, and emits a
%% `[bondy_oplog, instance, Source, ok]` telemetry event so callers
%% can tell the two paths apart in dashboards.
install_event(#state{} = State, Key, Value, Source, IsNew) ->
    MST1 = bondy_mst:put(State#state.mst, Key, Value),
    LastKey =
        case State#state.last_event_key of
            undefined -> Key;
            Prev when Key > Prev -> Key;
            Prev -> Prev
        end,
    _ = bondy_oplog_hlc:update(
        State#state.hlc, bondy_oplog_event:key_hlc(Key)
    ),
    %% If the installed event's origin is **ours**, bump the SeqRef
    %% atomic so a concurrent local `append_fast/3` can't allocate a
    %% colliding seq. This handles two paths:
    %%   1. Slow batch (`install_local_safe`): WAL-replayed local-
    %%      origin events that the fast batcher skipped (resume-
    %%      overlap probe).
    %%   2. Peer loopback (`do_append_remote`): a peer ships back our
    %%      own events via sync — `Origin == self`. Without this bump,
    %%      a subsequent local append after crash recovery could
    %%      allocate a seq that already lives in the MST (installed by
    %%      the peer-shipped copy).
    %% The fast batch path bumps once at end-of-batch in
    %% `install_fast_events/2` (cheaper) and does **not** go through
    %% `install_event/5`, so the per-event bump here doesn't duplicate.
    case bondy_oplog_event:key_origin(Key) of
        Origin when Origin =:= State#state.origin ->
            ok = maybe_bump_seq_atomic(
                State#state.seq, bondy_oplog_event:key_seq(Key)
            );
        _Other ->
            ok
    end,
    SizeDelta =
        case IsNew of
            true -> 1;
            false -> 0
        end,
    telemetry:execute(
        [bondy_oplog, instance, Source, ok],
        #{count => 1},
        #{instance_id => State#state.instance_id, new => IsNew}
    ),
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
%% Combined admission test: overlay pressure first, then the MST
%% working-set cap. Both checks are O(1) — overlay numbers come
%% from `ets:info/2`, working-set from cached `live_size`. The order
%% does not affect correctness because either failure is decisive;
%% overlay-first surfaces the more specific `backpressure` error
%% name when both would fire.
admit(State, Delta) ->
    case overlay_admit(State, Delta) of
        ok -> backpressure_admit(State, Delta);
        Err -> Err
    end.

%% @private
%% Pressure-check before the WAL append. Returns
%% `{error, backpressure}` when either cap is breached. `drop` is the
%% only supported strategy; `block` is reserved.
%%
%% Reads both slots of the shared `overlay_counters` atomics — slot
%% 1 the event count, slot 2 the byte estimate. Both are maintained
%% by `stage_to_overlay/2` and `evict_overlay_batch/2`, and by
%% lock-free `append_fast/2,3` callers. Pre-history this read pair
%% `ets:info/2` on the overlay table for size and memory; under
%% heavy concurrent appends those calls aggregated decentralised
%% counters across every scheduler and dominated the gen_server's
%% per-call cost.
overlay_admit(
    #state{
        instance_id = Id,
        overlay_counters = Ctrs,
        max_overlay_events = MaxEvents,
        max_overlay_bytes = MaxBytes
    },
    Delta
) ->
    {Size, Bytes} = overlay_counters_get(Ctrs),
    case Size + Delta > MaxEvents of
        true ->
            emit_overlay_backpressure(Id, events, Size, MaxEvents, Delta),
            {error, backpressure};
        false ->
            case Bytes >= MaxBytes of
                true ->
                    emit_overlay_backpressure(
                        Id, bytes, Bytes, MaxBytes, Delta
                    ),
                    {error, backpressure};
                false ->
                    ok
            end
    end.

%% @private
emit_overlay_backpressure(Id, Dimension, Current, Cap, Delta) ->
    telemetry:execute(
        [bondy_oplog, instance, overlay, backpressure_drop],
        #{count => 1},
        #{
            instance_id => Id,
            dimension => Dimension,
            current => Current,
            cap => Cap,
            requested => Delta
        }
    ).

%% @private
%% Backpressure admission test. Returns `ok` if the instance can
%% absorb `Delta` more events under its `max_working_set` cap, or
%% `{error, working_set_full}` otherwise. `infinity` disables the
%% cap.
%%
%% The cap is on **total events visible to readers** = MST live_size
%% + overlay rows (matching `size/1`), because events arriving via
%% `append`/`append_many` enter the overlay before the applier
%% promotes them to the MST. Counting only `live_size` would let the
%% caller burst arbitrarily many writes into the overlay before the
%% cap fires.
backpressure_admit(#state{max_working_set = infinity}, _Delta) ->
    ok;
backpressure_admit(
    #state{
        max_working_set = Cap,
        live_size = Size,
        overlay_counters = Ctrs
    } = State,
    Delta
) ->
    OverlaySize = atomics:get(Ctrs, 1),
    Total = Size + OverlaySize,
    case Total + Delta =< Cap of
        true ->
            ok;
        false ->
            telemetry:execute(
                [bondy_oplog, instance, backpressure],
                #{count => 1},
                #{
                    instance_id => State#state.instance_id,
                    requested => Delta,
                    live_size => Size,
                    overlay_size => OverlaySize,
                    cap => Cap
                }
            ),
            {error, working_set_full}
    end.

%% @private
%% Monotone watermark advance: returns whichever of the two values is
%% higher, treating `undefined` as the bottom. Used by both compaction
%% (via direct assignment, which is safe by construction — compaction
%% rejects frontiers ≤ current watermark) and operator-driven
%% `truncate_prefix`, where the caller's value could in principle be
%% lower than a previously installed compaction watermark.
advance_watermark(undefined, New) -> New;
advance_watermark(Cur, New) when New > Cur -> New;
advance_watermark(Cur, _New) -> Cur.

%% @private
%% Drops every key in MST that is `=< Watermark`, keeping the suffix of
%% keys `> Watermark`. Used both by explicit truncation and post-merge
%% re-truncation.
%%
%% Delegates to `bondy_mst:truncate/2` — a structural prefix-truncate
%% that walks only the tree's left spine, rewriting `O(log N)` pages
%% instead of issuing one `O(log N)` `delete/2` per stale key. This is
%% what lets compaction keep the live MST bounded under sustained write
%% saturation: the truncation cost is decoupled from the prefix size, so
%% a single cycle removes the whole stable prefix in time independent of
%% how many events accumulated. (The old per-key delete loop was
%% `O(P·log N)` and ran inside the gen_server, so at saturation the
%% truncation could not keep pace with the write rate and the MST grew
%% without bound — `mst_install` degraded and throughput collapsed.)
%%
%% The result is byte-identical to the equivalent delete sequence (the
%% MST is history-independent), so the root hash that peers sync against
%% is unchanged.
truncate_below_or_equal(MST, Watermark) ->
    bondy_mst:truncate(MST, Watermark).

%% @private
%% Runs a full compaction cycle synchronously, in the instance
%% gen_server. The frontier is now O(diff) (read-only `diff_to_list` + an
%% O(log N) `get/3` false-positive filter — see `compute_frontier_for/2`),
%% so the cycle is cheap enough to run inline rather than off-process.
%% Running in the gen_server is what makes the durable (pack-store)
%% backend work: the MST is read by the process that OWNS its sealed-pack
%% fds, so `prim_file:pread` no longer raises `not_on_controlling_process`.
%% The truncate + projection flush + checkpoint always ran here; only the
%% frontier moved in.
%%
%% A compaction is fully serial with every other gen_server message
%% (appends, reads, `load_snapshot`), so there is no in-flight bookkeeping
%% and no concurrency window. (This replaces the earlier async worker,
%% whose sole justification — keeping an O(N) frontier off the gen_server —
%% is gone now that the frontier is O(diff).)
do_compact_sync(
    #state{crdt_module = undefined, fold_module = undefined} = State,
    _PeerRoots
) ->
    {reply, {error, no_crdt_module}, State};
do_compact_sync(#state{} = State, PeerRoots) ->
    Started = erlang:monotonic_time(),
    Result = run_compaction(
        State#state.instance_id,
        State#state.mst,
        State#state.watermark,
        PeerRoots,
        State#state.compaction_checkpoint,
        State#state.compaction_checkpoint_state,
        State#state.cached_checkpoint,
        State#state.crdt_module
    ),
    {Reply, State1} = commit_compaction(State, Started, Result),
    ok = publish(State1),
    {reply, Reply, State1}.

%% @private
run_compaction(
    InstanceId,
    MST,
    Watermark0,
    PeerRoots,
    CkptMod,
    CkptState,
    CachedCheckpoint,
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
                %% Path is chosen by whether a PROJECTION materialises the
                %% state — NOT by whether `crdt_module` is set. A
                %% projection-backed instance (every `bondy_db` table:
                %% the applier's cell kernel maintains each cell via
                %% `interpret_cog` on write) takes the catalogue path even
                %% though it also has a `crdt_module`. See
                %% `architecture_regrounding_plan.md` §7 step 4.
                case has_projection(InstanceId) of
                    true ->
                        %% Catalogue (projection-backed): the projection IS
                        %% the durable checkpoint, so compaction only bounds
                        %% the MST — NO per-cycle `interpret_cog` re-fold of
                        %% the stable range (that O(range) per-event CRDT
                        %% work is what made sustained-write compaction fall
                        %% behind → unbounded MST → throughput collapse).
                        %% The truncate (and a synchronous flush of any
                        %% not-yet-replayed remote events first) runs in
                        %% `commit_compaction`. `EventCount` is derived there
                        %% from the live-size delta (O(remaining)), so this
                        %% does not fold the whole tree to count.
                        {ok, {catalogue_compacted, Frontier}};
                    false when CrdtMod =/= undefined ->
                        %% Bare CRDT instance with no projection: it owns its
                        %% own single-CRDT checkpoint, so fold the newly
                        %% stable range into it via `interpret_cog`.
                        Events = events_in_open_range(
                            MST, Watermark0, Frontier
                        ),
                        BaseCheckpoint =
                            case CachedCheckpoint of
                                undefined ->
                                    case CkptMod:get_checkpoint(CkptState) of
                                        {ok, _W, S} -> S;
                                        not_found -> CrdtMod:init()
                                    end;
                                {_, S0} ->
                                    S0
                            end,
                        NewCheckpoint = CrdtMod:interpret_cog(
                            Events, BaseCheckpoint
                        ),
                        ok = CkptMod:put_checkpoint(
                            CkptState, Frontier, NewCheckpoint
                        ),
                        {ok,
                            {compacted, Frontier, NewCheckpoint,
                                length(Events)}};
                    false ->
                        %% No projection and no CRDT module — nothing holds
                        %% the state, so truncating would lose it. Defer.
                        {ok, no_change}
                end
        end
    catch
        Class:Reason:Stack ->
            ?LOG_ERROR(#{
                description => "compaction raised",
                instance_id => InstanceId,
                class => Class,
                reason => Reason,
                stacktrace => Stack
            }),
            {error, {compaction_failed, Class, Reason}}
    end.

%% @private
%% The stability frontier: the largest local key K such that every local
%% key `=< K` is present (with the same value) in EVERY peer's confirmed
%% root.
%%
%% O(diff) and read-only. For each peer root it takes the read-only
%% structural diff of the live MST against that root (`diff_to_list/2` no
%% longer mutates the store — see `bondy_mst`), then walks the diff in key
%% order to the FIRST genuinely-divergent key. The structural diff is a
%% superset (a key that rides along in a changed leaf but is in fact
%% present-and-equal in the peer is a false-positive), so each candidate
%% is confirmed with an O(log N) `bondy_mst:get/3` against the peer root.
%% The global first hole is the smallest such key across peers; the
%% frontier is its predecessor in the local tree (`last_n/3`). With no
%% holes the whole local tree is confirmed and the frontier is the local
%% max key.
%%
%% This early-stops at the first divergence instead of folding every peer
%% into a full key set. It is equivalent to the previous O(N) set
%% longest-common-prefix: the instance MST uses the default term-order
%% comparator, and event keys (dots) carry a fixed value per key, so
%% presence and value agree.
compute_frontier_for(_MST, []) ->
    undefined;
compute_frontier_for(MST, PeerRoots) ->
    case [R || R <- PeerRoots, is_binary(R)] of
        [] ->
            undefined;
        [_ | _] = Roots ->
            case global_first_hole(MST, Roots) of
                no_hole ->
                    %% Every local key is confirmed by every peer.
                    case bondy_mst:last(MST) of
                        {K, _V} -> K;
                        undefined -> undefined
                    end;
                Hole ->
                    %% Largest local key strictly below the first hole.
                    case bondy_mst:last_n(MST, Hole, 1) of
                        [{K, _V}] -> K;
                        [] -> undefined
                    end
            end
    end.

%% @private
%% Smallest local key absent-or-different in some peer root, or `no_hole`
%% when the local tree is fully confirmed by every peer.
global_first_hole(MST, Roots) ->
    lists:foldl(
        fun(R, Acc) ->
            case peer_first_hole(MST, R) of
                no_hole -> Acc;
                H -> min_hole(H, Acc)
            end
        end,
        no_hole,
        Roots
    ).

%% @private
%% Term-order min; the instance MST's default comparator is `<`.
min_hole(H, no_hole) -> H;
min_hole(H, Acc) when H < Acc -> H;
min_hole(_H, Acc) -> Acc.

%% @private
%% First (smallest) local key genuinely divergent from peer root `R`,
%% walking the read-only structural diff (ascending key order) and
%% skipping present-and-equal false-positives. `no_hole` when local is a
%% subset of the peer (diff empty or all false-positives).
peer_first_hole(MST, R) ->
    first_genuine_hole(MST, R, bondy_mst:diff_to_list(MST, R)).

%% @private
first_genuine_hole(_MST, _R, []) ->
    no_hole;
first_genuine_hole(MST, R, [{K, V} | Rest]) ->
    case bondy_mst:get(MST, K, R) of
        V ->
            %% Structural false-positive: present and equal in the peer.
            first_genuine_hole(MST, R, Rest);
        _ ->
            %% Absent (`undefined`) or a different value: a genuine hole.
            K
    end.

%% @private
%% True when this instance has a projection that materialises its state
%% (an applier with a resolved `cell_apply_target`). Catalogue
%% compaction only truncates the MST when a projection holds the state;
%% a bare fold instance with no projection must keep its events in the
%% MST.
has_projection(InstanceId) ->
    try
        case bondy_oplog_registry:applier_pid(InstanceId) of
            undefined ->
                false;
            ApplierPid ->
                case bondy_oplog_applier:cell_apply_target(ApplierPid) of
                    {ok, _} -> true;
                    undefined -> false
                end
        end
    catch
        _:_ -> false
    end.

%% @private
%% Folds any events not yet applied to the projection into it, so the
%% projection reflects every event in the MST before the catalogue
%% truncate drops the stable prefix. Local events are folded before their
%% MST install, so only remote (peer-merged) events can lag.
%%
%% The catch-up diff is computed HERE — in the instance gen_server, which
%% OWNS the pack store's sealed-pack raw fds — and only the apply (a
%% projection write that never reads the MST) is delegated to the applier
%% (`apply_replayed_pairs/3`). This is what makes durable compaction work:
%% a stale `last_replayed_root` makes the diff a full MST fold that reaches
%% sealed packs, and only the fd owner can `prim_file:pread` them. (Earlier
%% this called `replay_cell_events_sync/1`, which ran the fold in the
%% applier process and raised `not_on_controlling_process` on sealed packs
%% → durable compaction deferred.)
%%
%% Returns `ok` only when the apply succeeded; any other result defers the
%% truncate so a transient applier failure never drops un-folded events.
ensure_projection_caught_up(InstanceId, MST) ->
    case bondy_oplog_registry:applier_pid(InstanceId) of
        undefined ->
            {error, no_applier};
        ApplierPid ->
            try
                LastRoot = bondy_oplog_applier:last_replayed_root(ApplierPid),
                case bondy_mst:root(MST) of
                    LastRoot ->
                        %% Projection already current — nothing to fold.
                        ok;
                    CurrentRoot ->
                        Pairs = replay_diff_pairs(MST, LastRoot),
                        bondy_oplog_applier:apply_replayed_pairs(
                            ApplierPid, Pairs, CurrentRoot
                        )
                end
            catch
                C:R -> {error, {C, R}}
            end
    end.

%% @private
%% The `[{Key, Value}]` to fold into the projection: the read-only diff of
%% the live MST against the applier's last replayed root. Computed in the
%% instance (the sealed-pack fd owner). A GC'd/unknown prior root falls
%% back to the full list (same contract as the applier's `diff_pairs/3`).
replay_diff_pairs(MST, undefined) ->
    bondy_mst:to_list(MST);
replay_diff_pairs(MST, LastRoot) ->
    try
        bondy_mst:diff_to_list(MST, LastRoot)
    catch
        _:_ -> bondy_mst:to_list(MST)
    end.

%% @private
%% Counts events in the open range (Watermark0, Frontier] over the
%% captured MST without materialising the event records. Used by the
%% catalogue truncate-only path for `live_size` bookkeeping (the
%% monolithic path counts via `length(events_in_open_range/3)` because
%% it already builds the list for `interpret_cog`).
count_in_open_range(MST, undefined, Frontier) ->
    bondy_mst:fold(
        MST,
        fun
            ({K, _V}, N) when K =< Frontier -> N + 1;
            (_, N) -> N
        end,
        0
    );
count_in_open_range(MST, W0, Frontier) ->
    bondy_mst:fold(
        MST,
        fun
            ({K, _V}, N) when K > W0, K =< Frontier -> N + 1;
            (_, N) -> N
        end,
        0
    ).

%% @private
%% Commits the compaction result atomically inside the gen_server, and
%% returns `{Reply, NewState}` to the synchronous `compact` handler.
%% Truncation uses the *current* state.mst.
commit_compaction(State, Started, {ok, no_change}) ->
    emit_compaction_telemetry(State, Started, undefined, 0),
    {{ok, no_change}, State};
commit_compaction(
    State,
    Started,
    {ok, {catalogue_compacted, Frontier}}
) ->
    %% Projection-backed (catalogue) compaction. The per-cell
    %% `interpret_cog` checkpoint IS the durable projection: the applier's
    %% cell kernel maintains each cell's materialised state via
    %% `interpret_cog` on write (`apply_op` == `interpret_cog` for the
    %% commutative case; the read path merges live events via
    %% `interpret_cog` too — `architecture_regrounding_plan.md` §7 step 4).
    %% So compaction does NOT fold a separate per-cell checkpoint blob —
    %% that would duplicate the projection (double storage + writes) for
    %% no gain; the checkpoint store is "not a durability layer for the
    %% projection" (`bondy_oplog_compaction_checkpoint` moduledoc).
    %% Compaction's only job here is to bound the MST.
    %%
    %% Before dropping the stable prefix, synchronously fold any remote
    %% events the async `replay_cell_events` path has not yet applied
    %% (local events are folded before their MST install, so only remote
    %% events can lag). This handler is serial with `integrate_peer_root`
    %% in the instance gen_server, so nothing un-folded can slip in
    %% between the flush and the truncate. If the flush fails (applier
    %% gone / errored) we defer rather than risk truncating an un-folded
    %% event; the next tick retries. The checkpoint envelope records only
    %% the watermark; its state slot is the `projection_managed` marker —
    %% "the materialised state is the projection" — which on restart is
    %% loaded but never fed to `interpret_cog` (the projection is the
    %% authoritative read source). Verified by
    %% `bondy_oplog_catalogue_compaction_test:crdt_kernel_compaction_matches_from_scratch`
    %% (post-compaction read == from-scratch `interpret_cog`).
    case ensure_projection_caught_up(State#state.instance_id, State#state.mst) of
        ok ->
            ok = (State#state.compaction_checkpoint):put_checkpoint(
                State#state.compaction_checkpoint_state,
                Frontier,
                projection_managed
            ),
            MST1 = truncate_below_or_equal(State#state.mst, Frontier),
            %% Derive the removed-event count from the live-size delta
            %% computed over the *truncated* tree (O(remaining)), rather
            %% than folding the whole pre-truncate tree to count the stable
            %% prefix (O(N)). When compaction keeps up, the remaining tree
            %% is small, so this — like the frontier scan over the live
            %% tree — stays cheap and the cycle cost stays bounded.
            LiveSize1 = compute_live_size(MST1),
            EventCount = max(0, State#state.live_size - LiveSize1),
            _ = bondy_oplog_hlc:update(
                State#state.hlc, bondy_oplog_event:key_hlc(Frontier)
            ),
            State1 = State#state{
                mst = MST1,
                watermark = Frontier,
                cached_checkpoint = {Frontier, projection_managed},
                live_size = LiveSize1
            },
            emit_compaction_telemetry(State, Started, Frontier, EventCount),
            {{ok, {compacted, Frontier, EventCount}}, State1};
        {error, _} ->
            {{ok, no_change}, State}
    end;
commit_compaction(
    State,
    Started,
    {ok, {compacted, Frontier, NewCheckpoint, EventCount}}
) ->
    MST1 = truncate_below_or_equal(State#state.mst, Frontier),
    _ = bondy_oplog_hlc:update(
        State#state.hlc, bondy_oplog_event:key_hlc(Frontier)
    ),
    State1 = State#state{
        mst = MST1,
        watermark = Frontier,
        cached_checkpoint = {Frontier, NewCheckpoint},
        live_size = max(0, State#state.live_size - EventCount)
    },
    emit_compaction_telemetry(State, Started, Frontier, EventCount),
    {{ok, {compacted, Frontier, EventCount}}, State1};
commit_compaction(State, _Started, {error, _} = Error) ->
    {Error, State}.

%% @private
emit_compaction_telemetry(State, Started, Frontier, EventCount) ->
    Duration = erlang:monotonic_time() - Started,
    telemetry:execute(
        [bondy_oplog, compaction, ok],
        #{
            duration => Duration,
            duration_us => erlang:convert_time_unit(
                Duration, native, microsecond
            ),
            event_count => EventCount
        },
        #{instance_id => State#state.instance_id, frontier => Frontier}
    ).

%% @private
%% Bootstrap: install a peer-supplied snapshot at the given watermark.
%% See `load_snapshot/3` for the contract.
%%
%% Compaction is fully synchronous in this gen_server, so a `load_snapshot`
%% call can never interleave with a compaction cycle — they serialise
%% naturally as separate messages.
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
    ok = (State#state.compaction_checkpoint):put_checkpoint(
        State#state.compaction_checkpoint_state, NewWatermark, Snapshot
    ),
    %% Drop any live events that the new checkpoint already covers.
    MST1 = truncate_below_or_equal(State#state.mst, NewWatermark),
    LiveSize1 = compute_live_size(MST1),
    %% Advance HLC to keep future local appends above the watermark.
    _ = bondy_oplog_hlc:update(
        State#state.hlc, bondy_oplog_event:key_hlc(NewWatermark)
    ),
    State1 = State#state{
        mst = MST1,
        watermark = NewWatermark,
        cached_checkpoint = {NewWatermark, Snapshot},
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
backend_opts(bondy_mst_pack_store, InstanceId, Opts) ->
    %% The pack-store backend wants `dir` (instance directory) and
    %% `instance_id` in its open opts. Mirror the ETS pattern: derive
    %% the per-instance dir from `storage_path` via the same path
    %% strategy that other persistent backends use, then inject the
    %% required keys as defaults that `backend_options` may override.
    Base = maps:get(backend_options, Opts, #{}),
    Defaults0 = #{instance_id => InstanceId},
    Defaults =
        case maps:find(storage_path, Opts) of
            {ok, BaseDir} ->
                Strategy = maps:get(
                    path_strategy, Opts, bondy_oplog_path_sharded
                ),
                Path = Strategy:storage_path(InstanceId, BaseDir),
                Defaults0#{dir => unicode:characters_to_binary(Path)};
            error ->
                Defaults0
        end,
    maps:merge(Defaults, Base);
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
%% Resolve the compaction-checkpoint backend module + opts.
%%
%% Precedence:
%%   1. Explicit `compaction_checkpoint` in Opts wins; checkpoint opts
%%      are passed through unchanged.
%%   2. Otherwise, if a `path` is set in `compaction_checkpoint_opts`
%%      OR `storage_path` is set on the instance, default to the file
%%      backend, deriving `path` from `storage_path` (via
%%      `path_strategy`) when not explicit.
%%   3. Otherwise default to the in-memory ETS backend (ephemeral).
%%
%% Path derivation when deriving from `storage_path`: the
%% `path_strategy` returns the per-instance dir (terminates in
%% `<InstanceId>`); the file backend then appends `<InstanceId>` again.
%% Pass the parent (the shard dir) so the final file lands at
%% `<storage_path>/<shard>/<InstanceId>/checkpoint.etf` alongside the
%% other per-instance artefacts (WAL, MST, projection).
resolve_checkpoint_backend(InstanceId, Opts, CkptOpts) ->
    case maps:find(compaction_checkpoint, Opts) of
        {ok, Mod} ->
            {Mod, CkptOpts};
        error ->
            case maps:is_key(path, CkptOpts) of
                true ->
                    {bondy_oplog_compaction_checkpoint_file, CkptOpts};
                false ->
                    case maps:find(storage_path, Opts) of
                        {ok, BaseDir} ->
                            Strategy = maps:get(
                                path_strategy,
                                Opts,
                                bondy_oplog_path_sharded
                            ),
                            InstanceDir = Strategy:storage_path(
                                InstanceId, BaseDir
                            ),
                            ShardDir = filename:dirname(InstanceDir),
                            {
                                bondy_oplog_compaction_checkpoint_file,
                                CkptOpts#{
                                    path => unicode:characters_to_binary(
                                        ShardDir
                                    )
                                }
                            };
                        error ->
                            {
                                bondy_oplog_compaction_checkpoint_ets,
                                CkptOpts
                            }
                    end
            end
    end.

%% @private
%% Publishes the current state's read-relevant fields to the registry.
%% Called after every state-mutating handle_call so that lock-free
%% read paths see fresh data without round-tripping the gen_server.
publish(#state{} = State) ->
    bondy_oplog_registry:publish(#{
        instance_id => State#state.instance_id,
        instance_pid => self(),
        origin => State#state.origin,
        mst => State#state.mst,
        watermark => State#state.watermark,
        snapshot => State#state.cached_checkpoint,
        crdt_module => State#state.crdt_module,
        fold_module => State#state.fold_module,
        fold_opts => State#state.fold_opts,
        live_size => State#state.live_size
    }).

%% @private
ets_member(InstanceId) ->
    bondy_oplog_registry:instance_pid(InstanceId) =/= undefined.

%% @private
%% Shape: `{Key, Value, Hlc, Origin}` per ?OVERLAY_KEY_POS macros.
%% Tolerates `Tab = undefined` (subtree mid-restart) — `ets:lookup`
%% on `undefined` raises `badarg`, which we treat as a clean miss
%% and let the caller fall through to the MST.
overlay_lookup_tab(Tab, Key) ->
    try ets:lookup(Tab, Key) of
        [{Key, Value, _Hlc, _Origin}] -> {ok, event_from_value(Key, Value)};
        [] -> not_found
    catch
        %% Tolerates a torn-down table during one_for_all restart.
        error:badarg -> not_found
    end.

%% @private
%% Returns overlay rows in `[From, To]` as a sorted list of
%% `{Key, Event}` tuples. `ets:select/2` on `ordered_set` yields rows
%% in key order, so the result list is already sorted. Returns `[]`
%% when the overlay tid is missing (subtree mid-restart).
overlay_range_tab(undefined, _From, _To) ->
    [];
overlay_range_tab(Tab, From, To) ->
    MatchSpec = [
        {
            {'$1', '$2', '_', '_'},
            [
                {'>=', '$1', {const, From}},
                {'=<', '$1', {const, To}}
            ],
            [{{'$1', '$2'}}]
        }
    ],
    try ets:select(Tab, MatchSpec) of
        Rows -> [{K, event_from_value(K, V)} || {K, V} <- Rows]
    catch
        error:badarg -> []
    end.

%% @private
%% Streaming merge of an MST fold with a pre-sorted overlay queue.
%% Returns the user accumulator after every entry in `[From, To]`
%% from both sources has been yielded in strict ascending key order.
%% Overlay wins on tied keys.
fold_range_merged(MST, From, To, OverlayQueue, Fun, Acc0) ->
    {Leftover, Acc1} = bondy_mst:fold(
        MST,
        fun({K, V}, {Queue, A}) ->
            case K >= From andalso K =< To of
                false -> {Queue, A};
                true -> merge_step(Queue, K, V, Fun, A)
            end
        end,
        {OverlayQueue, Acc0}
    ),
    drain_overlay_queue(Leftover, Fun, Acc1).

%% @private
merge_step([{OK, OEvent} | Rest], MstK, _MstV, Fun, Acc) when OK < MstK ->
    %% Overlay key strictly precedes MST key: yield overlay, recurse
    %% so we keep emitting overlay rows below the current MST entry.
    merge_step(Rest, MstK, _MstV, Fun, Fun(OEvent, Acc));
merge_step([{MstK, OEvent} | Rest], MstK, _MstV, Fun, Acc) ->
    %% Tied keys: overlay-wins; do not also emit the MST value.
    {Rest, Fun(OEvent, Acc)};
merge_step(Queue, MstK, MstV, Fun, Acc) ->
    %% Overlay queue empty, or its head is greater than MstK: emit
    %% MST entry.
    {Queue, Fun(event_from_value(MstK, MstV), Acc)}.

%% @private
drain_overlay_queue([], _Fun, Acc) ->
    Acc;
drain_overlay_queue([{_K, Event} | Rest], Fun, Acc) ->
    drain_overlay_queue(Rest, Fun, Fun(Event, Acc)).

%% @private
%% Min over (overlay.first, MST.first). Either can be empty.
merge_first_key_tab(Tab, MST) ->
    OverlayFirst = overlay_first_key_tab(Tab),
    MstFirst =
        case bondy_mst:first(MST) of
            undefined -> undefined;
            {K, _V} -> K
        end,
    min_key(OverlayFirst, MstFirst).

%% @private
%% Max over (overlay.last, MST.last). Either can be empty.
merge_latest_key_tab(Tab, MST) ->
    OverlayLast = overlay_last_key_tab(Tab),
    MstLast =
        case bondy_mst:last(MST) of
            undefined -> undefined;
            {K, _V} -> K
        end,
    max_key(OverlayLast, MstLast).

%% @private
overlay_first_key_tab(undefined) ->
    undefined;
overlay_first_key_tab(Tab) ->
    try ets:first(Tab) of
        '$end_of_table' -> undefined;
        K -> K
    catch
        error:badarg -> undefined
    end.

%% @private
overlay_last_key_tab(undefined) ->
    undefined;
overlay_last_key_tab(Tab) ->
    try ets:last(Tab) of
        '$end_of_table' -> undefined;
        K -> K
    catch
        error:badarg -> undefined
    end.

%% @private
%% Decrement the applier→instance in-flight counter after an
%% `install_local_batch` cast has been fully processed. If we just
%% freed the saturated slot (post-decrement value equals `cap - 1`),
%% wake the applier so it can resume reading the WAL. Cheaper than a
%% timer-based poll on the applier side; the cast is a noop if the
%% applier is already draining.
release_install_slot(#state{
    install_in_flight = undefined
}) ->
    ok;
release_install_slot(#state{
    instance_id = InstanceId,
    install_in_flight = Ref,
    max_install_in_flight = Cap
}) ->
    %% atomics is unsigned so add_get with -1 wraps if we underflow.
    %% Use sub_get for a signed-safe decrement.
    Now = atomics:sub_get(Ref, 1, 1),
    case Now =:= Cap - 1 of
        true ->
            %% Just below the cap; the applier was (or may have been)
            %% gated. Resume it.
            case bondy_oplog_registry:applier_pid(InstanceId) of
                undefined ->
                    ok;
                ApplierPid ->
                    bondy_oplog_applier:notify_drain_resume(ApplierPid)
            end;
        false ->
            ok
    end,
    ok.

%% @private
%% A4 — release N install slots after a coalesced batch (one per
%% coalesced cast). Calling `release_install_slot/1` N times decrements
%% the atomic N times; because the value descends monotonically through
%% the calls, exactly one of them observes the `Cap - 1` crossing and
%% wakes the applier — so coalescing N casts still resumes a gated
%% applier exactly once, without bespoke threshold arithmetic.
release_install_slots(_State, 0) ->
    ok;
release_install_slots(State, N) when N > 0 ->
    ok = release_install_slot(State),
    release_install_slots(State, N - 1).

%% @private
%% A4 — drain queued `install_local_batch` casts into `Acc` (a list of
%% per-cast event lists, most-recent first) without blocking. Stops at
%% `Max` casts (counting the one already being handled) or when the
%% mailbox holds no more install casts. Selective receive returns
%% mailbox-FIFO order, so prepending preserves WAL order once reversed.
drain_install_casts(Acc, N, Max) when N >= Max ->
    {Acc, N};
drain_install_casts(Acc, N, Max) ->
    receive
        {'$gen_cast', {install_local_batch, Events}} ->
            drain_install_casts([Events | Acc], N + 1, Max)
    after 0 ->
        {Acc, N}
    end.

%% @private
%% A4 — validate the coalescing cap. A malformed value crashes `init/1`
%% loudly (same convention as the other startup-validated opts) rather
%% than silently degrading to a default.
validate_coalesce_max(N) when is_integer(N), N >= 1 ->
    N;
validate_coalesce_max(Bad) ->
    error({invalid_opt, install_coalesce_max, Bad}).

%% @private
%% Replies `ok` to every caller queued in `drain_waiters` once the
%% overlay has reached size 0. Idempotent — re-running with an empty
%% waiter list or a non-empty overlay is a no-op. Called from every
%% handler that can shrink the overlay (`install_local_batch`,
%% `check_drain_waiters`).
maybe_signal_drain_waiters(#state{drain_waiters = []} = State) ->
    State;
maybe_signal_drain_waiters(
    #state{drain_waiters = Waiters, overlay = Tab} = State
) ->
    case overlay_size_tab(Tab) of
        0 ->
            lists:foreach(fun(From) -> gen_server:reply(From, ok) end, Waiters),
            State#state{drain_waiters = []};
        _ ->
            State
    end.

%% @private
overlay_size_tab(undefined) ->
    0;
overlay_size_tab(Tab) ->
    try ets:info(Tab, size) of
        N when is_integer(N) -> N;
        _ -> 0
    catch
        error:badarg -> 0
    end.

%% @private
min_key(undefined, undefined) -> empty;
min_key(undefined, K) -> {ok, K};
min_key(K, undefined) -> {ok, K};
min_key(A, B) when A =< B -> {ok, A};
min_key(_, B) -> {ok, B}.

%% @private
max_key(undefined, undefined) -> empty;
max_key(undefined, K) -> {ok, K};
max_key(K, undefined) -> {ok, K};
max_key(A, B) when A >= B -> {ok, A};
max_key(_, B) -> {ok, B}.

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

%% @private
%% Resolves and validates the `fold_module` / `fold_opts` instance
%% opts. `undefined` means "no fold configured" — the legacy event-
%% storage path remains in effect. Invalid configurations crash
%% init/1 with a structured error.
resolve_fold_config(InstanceId, Opts) ->
    case maps:get(fold_module, Opts, undefined) of
        undefined ->
            FoldOpts0 = maps:get(fold_opts, Opts, #{}),
            ok = assert_fold_opts(FoldOpts0),
            {undefined, FoldOpts0};
        Strategy ->
            %% PR-Z: the per-instance projection runs the native CRDT twin
            %% of the `fold_module` label. A label is valid iff it resolves
            %% to a twin; an unknown label has none.
            case bondy_oplog_cell_kernel:default_crdt_for_fold(Strategy) of
                undefined ->
                    erlang:error(
                        {invalid_fold_module, InstanceId, {unknown, Strategy}}
                    );
                _CrdtMod ->
                    FoldOpts = maps:get(fold_opts, Opts, #{}),
                    ok = assert_fold_opts(FoldOpts),
                    {Strategy, FoldOpts}
            end
    end.

%% @private
assert_fold_opts(M) when is_map(M) -> ok;
assert_fold_opts(Other) -> erlang:error({invalid_fold_opts, Other}).

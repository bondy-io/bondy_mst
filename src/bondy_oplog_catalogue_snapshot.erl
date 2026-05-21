%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_catalogue_snapshot).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Peer-side responder for the catalogue-snapshot bootstrap protocol
(`_design/catalogue_expansion_plan.md` §4.12).

Two entry points:

- `init/1,2` — opens a session. Detects whether the instance has a
  catalogue projection at all. Returns `{ok, {Watermark, Cursor}}` on
  success, or `{ok, no_snapshot}` for legacy single-CRDT instances and
  for catalogue instances that have not yet wired a `cell_apply_target`.
- `next/2` — pulls the next chunk of `(Bucket, Key, Frame)` triples
  from the projection. Returns `{ok, {batch, {Cursor, Cells}}}` while
  there is more, `{ok, {done, []}}` on end-of-keyspace, or
  `{error, cursor_expired}` when the session's cursor was reaped.

The implementation is direct ETS / direct adapter — no instance
gen_server round-trip in the hot path so multiple bootstrap sessions
on the same peer run fully in parallel. Only the initial `init/1` call
hits the applier (to discover `cell_apply_target`).

## Single-shard, single-bucket assumption

This v1 services single-shard catalogues that store all cells in one
bucket. The default bucket is `<<>>` (matching the convention in
existing test instances). Multi-shard / multi-bucket catalogue
bootstrap is a follow-up; see Architecture QA §4.12.

## Snapshot consistency

The cursor captures the high-water HLC at session start. Cells
returned in subsequent batches MAY include writes past that HLC — the
range scan is live, not a frozen snapshot. The bootstrap install
contract (PR-D2 §3) does NOT depend on snapshot freezing: each cell is
applied via the fold's idempotent `apply_event/3`, and live events
arriving during the bootstrap window are guarded by the per-cell HLC
skip-if-older check on `pre_bootstrap`. See [[project-prd1-landed-2026-05-22]]
for the consistency contract.
""").

-export([init/1]).
-export([init/2]).
-export([next/2]).

%% Default bucket for catalogue projections. Matches the convention in
%% `bondy_oplog_applier_cell_apply_test` and the e2e test suites: cell
%% events are appended with `Bucket = <<>>`.
-define(DEFAULT_BUCKET, <<>>).

%% Max binary sentinel for unbounded-high range scans. 256 bytes of
%% 0xFF — beyond any production catalogue key.
-define(MAX_KEY_SENTINEL,
        <<255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,
          255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255>>).

%% Default batch size. Configurable via app env
%% `catalogue_snapshot_batch_size`.
-define(DEFAULT_BATCH_SIZE, 64).

%% =============================================================================
%% API
%% =============================================================================

-spec init(instance_id()) ->
    {ok, {non_neg_integer(), bondy_oplog_catalogue_cursor:cursor()}}
    | {ok, no_snapshot}.

?DOC("""
Opens a catalogue-snapshot session on the given instance using the
default bucket (`<<>>`). See `init/2` for bucket override.
""").
init(InstanceId) ->
    init(InstanceId, default_bucket()).

-spec init(instance_id(), Bucket :: binary()) ->
    {ok, {non_neg_integer(), bondy_oplog_catalogue_cursor:cursor()}}
    | {ok, no_snapshot}.

init(InstanceId, Bucket)
        when is_binary(InstanceId), is_binary(Bucket) ->
    %% Step 1 — detect catalogue mode. Single-CRDT mode has a defined
    %% `crdt_module`; catalogue mode does not.
    case bondy_oplog_instance:crdt_module(InstanceId) of
        Mod when is_atom(Mod), Mod =/= undefined ->
            {ok, no_snapshot};
        undefined ->
            init_catalogue(InstanceId, Bucket)
    end.

-spec next(
    instance_id(),
    bondy_oplog_catalogue_cursor:cursor()
) ->
    {ok, {batch, {bondy_oplog_catalogue_cursor:cursor(),
                  [bondy_oplog_transport:cell()]}}}
    | {ok, {done, []}}
    | {error, cursor_expired}
    | {error, term()}.

?DOC("""
Pulls the next batch from the given cursor. The cursor is opaque to
the initiator and is returned unchanged on `batch` so the caller can
chain calls without state.
""").
next(InstanceId, Cursor)
        when is_binary(InstanceId), is_binary(Cursor) ->
    case bondy_oplog_catalogue_cursor:lookup(Cursor) of
        not_found ->
            %% A cursor unknown to the peer means the session was
            %% started on a different peer, or the peer was restarted —
            %% either way the initiator must retry from `init/1`. We
            %% report this as `expired` for protocol simplicity (the
            %% initiator's recovery path is the same).
            {error, cursor_expired};
        expired ->
            {error, cursor_expired};
        {ok, #{instance_id := SessionId} = _CState}
                when SessionId =/= InstanceId ->
            %% Cursor belongs to a different instance. Treat as expired
            %% so the initiator restarts cleanly on the correct
            %% instance.
            {error, cursor_expired};
        {ok, CState} ->
            do_next(Cursor, CState)
    end.

%% =============================================================================
%% PRIVATE — init flow
%% =============================================================================

%% @private
init_catalogue(InstanceId, Bucket) ->
    case bondy_oplog_registry:applier_pid(InstanceId) of
        undefined ->
            {ok, no_snapshot};
        ApplierPid ->
            case bondy_oplog_applier:cell_apply_target(ApplierPid) of
                undefined ->
                    %% Catalogue instance with no projection wiring —
                    %% nothing to snapshot.
                    {ok, no_snapshot};
                {ok, {NS, Index, Shard}} ->
                    init_with_target(InstanceId, NS, Index, Shard, Bucket)
            end
    end.

%% @private
init_with_target(InstanceId, NS, Index, Shard, Bucket) ->
    case bondy_db_core_registry:high_water_hlc(NS, Index, Shard) of
        not_found ->
            %% Shard was unregistered between the applier opening it
            %% and us reading the watermark. Bail out as no_snapshot —
            %% the initiator can retry.
            {ok, no_snapshot};
        {ok, no_watermark} ->
            %% Fresh shard, no cells applied yet. A snapshot would be
            %% empty; we still mint a cursor so the initiator's pull
            %% loop terminates cleanly via `{ok, {done, []}}`.
            Cursor = bondy_oplog_catalogue_cursor:mint(
                InstanceId, NS, Index, Shard, Bucket, 0
            ),
            {ok, {0, Cursor}};
        {ok, Watermark} when is_integer(Watermark) ->
            Cursor = bondy_oplog_catalogue_cursor:mint(
                InstanceId, NS, Index, Shard, Bucket, Watermark
            ),
            {ok, {Watermark, Cursor}}
    end.

%% =============================================================================
%% PRIVATE — next flow
%% =============================================================================

%% @private
do_next(Cursor, CState) ->
    #{
        ns := NS,
        index := Index,
        shard := Shard,
        bucket := Bucket,
        last_key := LastKey
    } = CState,
    case bondy_db_core_registry:lookup(NS, Index, Shard) of
        not_found ->
            %% Shard vanished mid-session — initiator must restart.
            ok = bondy_oplog_catalogue_cursor:discard(Cursor),
            {error, cursor_expired};
        {ok, Entry} ->
            Adapter = bondy_db_core_registry:entry_projection_adapter(Entry),
            Handle  = bondy_db_core_registry:entry_projection_handle(Entry),
            Low     = next_key_after(LastKey),
            High    = ?MAX_KEY_SENTINEL,
            BatchSize = batch_size(),
            case Adapter:range(Handle, Bucket, Low, High, #{limit => BatchSize}) of
                {ok, []} ->
                    ok = bondy_oplog_catalogue_cursor:discard(Cursor),
                    {ok, {done, []}};
                {ok, Pairs} ->
                    Cells = [{Bucket, K, F} || {K, F} <- Pairs],
                    {LastK, _} = lists:last(Pairs),
                    case bondy_oplog_catalogue_cursor:advance(Cursor, LastK) of
                        ok ->
                            {ok, {batch, {Cursor, Cells}}};
                        not_found ->
                            %% Cursor was reaped concurrently — rare but
                            %% possible if the session sat idle past the
                            %% TTL right at the moment the GC ran.
                            {error, cursor_expired}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% @private
%% Lexicographic successor for binary keys: `<<K/binary, 0>>` is the
%% smallest binary strictly greater than `K`. Initial `undefined` maps
%% to `<<>>` (the smallest possible Low).
next_key_after(undefined)             -> <<>>;
next_key_after(K) when is_binary(K)   -> <<K/binary, 0>>.

%% @private
default_bucket() ->
    application:get_env(
        bondy_mst, catalogue_default_bucket, ?DEFAULT_BUCKET
    ).

%% @private
batch_size() ->
    application:get_env(
        bondy_mst, catalogue_snapshot_batch_size, ?DEFAULT_BATCH_SIZE
    ).

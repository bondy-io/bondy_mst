%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_sync_session).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
A single anti-entropy sync session.

Implements the **pull-direction** of the MST reconciliation protocol
(`_design/10_new_design.md` §7): the *initiator* (this side) repeatedly
asks the *peer* for pages it is missing until its local copy of the
peer's tree is complete. Two such sessions — A pulling from B *and* B
pulling from A — converge both replicas to the same root.

## Algorithm

```
1. PeerRoot ← transport:request(Peer, Instance, get_root)
2. if PeerRoot == LocalRoot, done
3. loop:
     Missing ← instance:missing_set(Instance, PeerRoot)
     if Missing is empty, break
     Pages ← transport:request(Peer, Instance, {get_pages, Missing})
     instance:merge_pages(Instance, Pages)
     %% page may reference further pages we still don't have →
     %% next iteration's missing_set picks them up
4. record_sync_complete(Peer, Instance, LocalRoot)
```

The loop is bounded by the depth of the MST plus the number of
divergent pages. With B=16 and a moderate divergence, expect 1–3
iterations.

## API shapes

- `run/3` — synchronous; returns `{ok, FinalRoot}` or `{error, Reason}`.
  Used by tests and consumers that want to await completion.
- `start/3` — asynchronous spawn; the session reports completion via
  `bondy_oplog_peer_state:record_sync_complete/3` and exits.
  Used by the default sync scheduler.

## Bounded iterations

`max_iterations` (default 16) caps the loop to prevent pathological
peers (or buggy transports that always reply with the same incomplete
set) from making the session run forever.
""").

-type opts() :: #{
    transport => module(),
    transport_opts => map(),
    max_iterations => pos_integer(),
    record_in_peer_state => boolean()
}.

-export_type([opts/0]).

-export([run/3]).
-export([run/4]).
-export([start/3]).
-export([start/4]).
-export([bootstrap/3]).
-export([bootstrap_catalogue/3]).
-export([start_bootstrap/3]).
-export([start_bootstrap_catalogue/3]).

%% =============================================================================
%% API
%% =============================================================================

?DOC("""
Synchronously runs a pull-direction sync session. Returns `{ok, Root}`
on success, where `Root` is the local root hash after merging.
""").
-spec run(instance_id(), peer_id(), opts()) ->
    {ok, bondy_mst:hash() | undefined} | {error, term()}.

run(Instance, Peer, Opts) ->
    run(Instance, Peer, Opts, default_max_iterations(Opts)).

-spec run(instance_id(), peer_id(), opts(), non_neg_integer()) ->
    {ok, bondy_mst:hash() | undefined} | {error, term()}.

run(Instance, Peer, Opts, Iterations) when is_binary(Instance) ->
    Transport = maps:get(
        transport,
        Opts,
        bondy_oplog_transport_inline
    ),
    TransportOpts = maps:get(transport_opts, Opts, #{}),
    Record = maps:get(record_in_peer_state, Opts, true),
    Start = erlang:monotonic_time(),
    Result = do_run(Instance, Peer, Transport, TransportOpts, Iterations),
    maybe_record(Result, Instance, Peer, Record),
    Duration = erlang:monotonic_time() - Start,
    Outcome =
        case Result of
            {ok, _} -> ok;
            {error, _} -> error
        end,
    telemetry:execute(
        [bondy_oplog, sync, Outcome],
        #{duration => Duration},
        #{instance_id => Instance, peer => Peer}
    ),
    Result.

?DOC("""
Spawns the session in a separate process and returns immediately.
Completion is reported via `peer_state` (and via telemetry, in a
later stage). The spawned process exits normally on success and with
an error reason on failure.
""").
-spec start(instance_id(), peer_id(), opts()) -> {ok, pid()}.

start(Instance, Peer, Opts) ->
    start(Instance, Peer, Opts, default_max_iterations(Opts)).

-spec start(instance_id(), peer_id(), opts(), non_neg_integer()) ->
    {ok, pid()}.

start(Instance, Peer, Opts, Iterations) ->
    Pid = spawn(fun() ->
        case run(Instance, Peer, Opts, Iterations) of
            {ok, _} ->
                ok;
            {error, Reason} ->
                ?LOG_WARNING(#{
                    description => "sync session failed",
                    instance => Instance,
                    peer => Peer,
                    reason => Reason
                }),
                exit({sync_failed, Reason})
        end
    end),
    {ok, Pid}.

?DOC("""
Bootstrap session: fetch the peer's snapshot first, install it
locally, then run the regular pull-direction sync for events past the
new watermark.

Suitable for a *fresh* replica joining a long-running cluster, or a
*recovering* replica whose watermark is far behind. Falls back to
plain sync if the peer reports `no_snapshot`.

Returns `{ok, FinalRoot}` on success, `{error, Reason}` otherwise.
""").
-spec bootstrap(instance_id(), peer_id(), opts()) ->
    {ok, bondy_mst:hash() | undefined} | {error, term()}.

bootstrap(Instance, Peer, Opts) when is_binary(Instance) ->
    Transport = maps:get(
        transport,
        Opts,
        bondy_oplog_transport_inline
    ),
    TransportOpts = maps:get(transport_opts, Opts, #{}),
    case Transport:request(Peer, Instance, get_snapshot, TransportOpts) of
        {ok, no_snapshot} ->
            %% Peer has nothing to bootstrap from. The local instance
            %% has no path to a `live` projection state through this
            %% peer — but a *fresh* peer with empty state and no events
            %% behind the watermark is still safe to flip live (there
            %% is nothing to apply incorrectly). Skip the snapshot
            %% install and proceed with plain sync; the lifecycle stays
            %% as it was (caller is expected to have seeded a genesis
            %% peer separately, or to try a peer with a snapshot).
            run(Instance, Peer, Opts);
        {ok, Watermark, Snapshot} ->
            case
                bondy_oplog_instance:load_snapshot(
                    Instance, Watermark, Snapshot
                )
            of
                {ok, _} ->
                    %% Bootstrap completion ordering
                    %% (`_design/catalogue_expansion_plan.md` §2.4):
                    %%   1. load_snapshot (done above) installs the
                    %%      snapshot and advances the watermark to
                    %%      H_boot.
                    %%   2. `mark_live/1` writes the durable flag
                    %%      file — the marker that "everything
                    %%      before me succeeded." MUST be last:
                    %%      a crash between (1) and (2) leaves no
                    %%      flag, so restart re-runs bootstrap
                    %%      idempotently; a crash after (2)
                    %%      durably leaves the instance live.
                    %%   3. Run anti-entropy for events past the new
                    %%      watermark. Safe to interleave because
                    %%      the applier is already gated and the WAL
                    %%      is the buffer.
                    ok = bondy_oplog_instance:mark_live(Instance),
                    run(Instance, Peer, Opts);
                {error, watermark_not_advancing} ->
                    %% Local watermark is already ≥ peer's. The local
                    %% instance is either already live (flag exists)
                    %% or was a genesis seed (lifecycle was already
                    %% live). Either way mark_live is idempotent;
                    %% calling it here makes the path uniformly leave
                    %% the lifecycle in `live` regardless of which
                    %% branch was taken.
                    ok = bondy_oplog_instance:mark_live(Instance),
                    run(Instance, Peer, Opts);
                {error, _} = E ->
                    E
            end;
        {error, _} = E ->
            E
    end.

?DOC("""
Catalogue-mode bootstrap session. The peer streams its projection
cells in chunks (`get_catalogue_snapshot_init` +
`{get_catalogue_snapshot_next, Cursor}`); the initiator installs each
batch into its own projection. After the stream ends, the lifecycle is
marked `live` (for a fresh caller) and the regular pull-direction sync
runs to catch up on any events newer than the peer's session-start
watermark.

The local instance MUST be catalogue-mode (`crdt_module = undefined`).
Single-CRDT-mode callers must use `bootstrap/3` instead. A
`{error, not_a_catalogue_instance}` is returned otherwise.

If the peer reports `no_snapshot` (it is itself single-CRDT mode, or
has not yet wired a `cell_apply_target`) the call falls through to
plain `run/3`. This handles the new-cluster genesis case cleanly: an
empty peer + empty local replica produces an immediate `done`.

`WasLive` is captured at session start so `finalize_catalogue_bootstrap`
can decide whether to mark live (fresh) or skip the lifecycle update
(recovering).
""").
-spec bootstrap_catalogue(instance_id(), peer_id(), opts()) ->
    {ok, bondy_mst:hash() | undefined}
    | {error, not_a_catalogue_instance}
    | {error, cursor_expired}
    | {error, term()}.

bootstrap_catalogue(Instance, Peer, Opts) when is_binary(Instance) ->
    case bondy_oplog_instance:crdt_module(Instance) of
        Mod when is_atom(Mod), Mod =/= undefined ->
            {error, not_a_catalogue_instance};
        undefined ->
            do_bootstrap_catalogue(Instance, Peer, Opts)
    end.

%% @private
do_bootstrap_catalogue(Instance, Peer, Opts) ->
    Transport = maps:get(
        transport,
        Opts,
        bondy_oplog_transport_inline
    ),
    TransportOpts = maps:get(transport_opts, Opts, #{}),
    WasLive = is_live(Instance),
    Start = erlang:monotonic_time(),
    Result = do_bootstrap_snapshot(
        Instance, Peer, Opts, Transport, TransportOpts, WasLive
    ),
    Duration = erlang:monotonic_time() - Start,
    Outcome =
        case Result of
            {ok, _} -> ok;
            {error, _} -> error
        end,
    telemetry:execute(
        [bondy_oplog, sync, catalogue_bootstrap, Outcome],
        #{duration => Duration},
        #{instance_id => Instance, peer => Peer, was_live => WasLive}
    ),
    Result.

%% @private
%% Catalogue-snapshot bootstrap: bulk-seed the local projection from the
%% peer snapshot in `replace` mode (skip-if-older by HLC), mark live (a
%% `pre_bootstrap` caller), then anti-entropy + op-replay — the plan's
%% "checkpoint-replace + op-replay" (PR-G removed the CvRDT `merge_states`
%% merge-mode).
do_bootstrap_snapshot(Instance, Peer, Opts, Transport, TransportOpts, WasLive) ->
    case
        Transport:request(
            Peer, Instance, get_catalogue_snapshot_init, TransportOpts
        )
    of
        {ok, no_snapshot} ->
            %% Peer has nothing to ship. Run the regular pull and let the
            %% lifecycle stay where it was — the caller seeded the replica
            %% as `live` (genesis) or expects a future bootstrap against a
            %% non-empty peer.
            run(Instance, Peer, Opts);
        {ok, {init, {Watermark, Cursor}}} ->
            case
                pull_install_loop(
                    Instance, Peer, Transport, TransportOpts, Cursor, 0, 0
                )
            of
                {ok, Installed, Skipped} ->
                    ok = bondy_oplog_instance:finalize_catalogue_bootstrap(
                        Instance, Watermark, WasLive
                    ),
                    telemetry:execute(
                        [bondy_oplog, sync, catalogue_bootstrap, complete],
                        #{
                            installed => Installed,
                            skipped => Skipped,
                            watermark => Watermark
                        },
                        #{
                            instance_id => Instance,
                            peer => Peer,
                            was_live => WasLive
                        }
                    ),
                    finish_bootstrap(Instance, Peer, Opts, WasLive);
                {error, _} = E ->
                    E
            end;
        {error, _} = E ->
            E
    end.

%% @private
%% Run anti-entropy (MST page union + diff-replay), then, for a LIVE
%% re-bootstrap, op-replay: re-derive the projection from the now-merged
%% local+peer event set. The snapshot install is `replace` (skip-if-older
%% by HLC), which is correct for a register but can CLOBBER a CRDT that
%% accumulates per-Origin (a counter, a grow-set) when the peer's
%% higher-HLC cell omits a local Origin's contribution. A full re-fold
%% (`interpret_cog` over the complete event set) restores it — the op-based
%% replacement for the removed CvRDT `merge_states`. On a fresh
%% (`pre_bootstrap`) replica it is unnecessary (the local projection was
%% empty, so `replace` could not clobber, and the cold-start replay already
%% re-folds), so it is skipped to avoid a redundant full fold.
finish_bootstrap(Instance, Peer, Opts, WasLive) ->
    case run(Instance, Peer, Opts) of
        {ok, _} = Ok ->
            case WasLive of
                true -> ok = rederive_projection(Instance);
                false -> ok
            end,
            Ok;
        {error, _} = E ->
            E
    end.

%% @private
rederive_projection(Instance) ->
    case bondy_oplog_registry:applier_pid(Instance) of
        undefined ->
            ok;
        Pid when is_pid(Pid) ->
            bondy_oplog_applier:rederive_projection_sync(Pid)
    end.

%% @private
pull_install_loop(
    Instance, Peer, Transport, TransportOpts, Cursor, Installed, Skipped
) ->
    %% The install is always `replace` (skip-if-older by HLC) — PR-G
    %% removed merge-mode (the CvRDT `merge_states` join). On a fresh
    %% replica the local projection is empty so every cell installs; on a
    %% live re-bootstrap a higher-HLC peer cell can clobber a per-Origin-
    %% accumulating CRDT, which the post-bootstrap op-replay then restores.
    Req = {get_catalogue_snapshot_next, Cursor},
    case Transport:request(Peer, Instance, Req, TransportOpts) of
        {ok, {done, []}} ->
            {ok, Installed, Skipped};
        {ok, {batch, {NextCursor, Cells}}} ->
            case
                bondy_oplog_instance:install_catalogue_batch(
                    Instance, {replace, Cells}
                )
            of
                {ok, #{installed := I, skipped := S} = _Counts} ->
                    pull_install_loop(
                        Instance,
                        Peer,
                        Transport,
                        TransportOpts,
                        NextCursor,
                        Installed + I,
                        Skipped + S
                    );
                {error, _} = E ->
                    E
            end;
        {error, _} = E ->
            E
    end.

?DOC("""
Spawns a `bootstrap/3` (single-CRDT) session in a separate process and
returns immediately. Failures are logged and the process exits with
`{bootstrap_failed, Reason}`. Used by the sync scheduler for
auto-bootstrap of single-CRDT pre_bootstrap instances.
""").
-spec start_bootstrap(instance_id(), peer_id(), opts()) -> {ok, pid()}.

start_bootstrap(Instance, Peer, Opts) ->
    Pid = spawn(fun() ->
        case bootstrap(Instance, Peer, Opts) of
            {ok, _} ->
                ok;
            {error, Reason} ->
                ?LOG_WARNING(#{
                    description => "bootstrap session failed",
                    instance => Instance,
                    peer => Peer,
                    reason => Reason
                }),
                exit({bootstrap_failed, Reason})
        end
    end),
    {ok, Pid}.

?DOC("""
Spawns a `bootstrap_catalogue/3` session in a separate process and
returns immediately. Failures are logged and the process exits with
`{bootstrap_catalogue_failed, Reason}`. Used by the sync scheduler for
auto-bootstrap of catalogue-mode pre_bootstrap instances.
""").
-spec start_bootstrap_catalogue(instance_id(), peer_id(), opts()) ->
    {ok, pid()}.

start_bootstrap_catalogue(Instance, Peer, Opts) ->
    Pid = spawn(fun() ->
        case bootstrap_catalogue(Instance, Peer, Opts) of
            {ok, _} ->
                ok;
            {error, Reason} ->
                ?LOG_WARNING(#{
                    description => "bootstrap_catalogue session failed",
                    instance => Instance,
                    peer => Peer,
                    reason => Reason
                }),
                exit({bootstrap_catalogue_failed, Reason})
        end
    end),
    {ok, Pid}.

%% @private
is_live(Instance) ->
    case bondy_oplog_instance:lifecycle_state(Instance) of
        live -> true;
        _ -> false
    end.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
do_run(Instance, Peer, Transport, TransportOpts, MaxIterations) ->
    case Transport:request(Peer, Instance, get_root, TransportOpts) of
        {ok, undefined} ->
            %% Peer has nothing; nothing to pull.
            {ok, bondy_oplog_instance:root_hash(Instance)};
        {ok, PeerRoot} ->
            LocalRoot = bondy_oplog_instance:root_hash(Instance),
            case PeerRoot =:= LocalRoot of
                true ->
                    {ok, LocalRoot};
                false ->
                    pull_until_complete(
                        Instance,
                        Peer,
                        Transport,
                        TransportOpts,
                        PeerRoot,
                        MaxIterations
                    )
            end;
        {error, _} = E ->
            E
    end.

%% @private
pull_until_complete(
    Instance,
    Peer,
    _Transport,
    _TransportOpts,
    _PeerRoot,
    0
) ->
    ?LOG_WARNING(#{
        description => "sync session hit max iterations",
        instance => Instance,
        peer => Peer
    }),
    {error, max_iterations_exceeded};
pull_until_complete(
    Instance,
    Peer,
    Transport,
    TransportOpts,
    PeerRoot,
    Remaining
) ->
    case bondy_oplog_instance:missing_set(Instance, PeerRoot) of
        [] ->
            %% Every page reachable from PeerRoot is now in our store.
            %% Integrate at the item level — this walks PeerRoot's tree
            %% using the local store and folds its items into ours,
            %% producing a new merged root.
            ok = bondy_oplog_instance:integrate_peer_root(
                Instance, PeerRoot
            ),
            {ok, bondy_oplog_instance:root_hash(Instance)};
        Missing ->
            case
                Transport:request(
                    Peer,
                    Instance,
                    {get_pages, Missing},
                    TransportOpts
                )
            of
                {ok, Pages} when map_size(Pages) =:= 0 ->
                    {error, {peer_returned_empty_pages, Missing}};
                {ok, Pages} ->
                    ok = merge_pages(Instance, Pages),
                    pull_until_complete(
                        Instance,
                        Peer,
                        Transport,
                        TransportOpts,
                        PeerRoot,
                        Remaining - 1
                    );
                {error, _} = E ->
                    E
            end
    end.

%% @private
maybe_record({ok, Root}, Instance, Peer, true) when is_binary(Root) ->
    ok = bondy_oplog_peer_state:record_sync_complete(
        Peer, Instance, Root
    ),
    ok = bump_ae_on_sync(Instance, Peer);
maybe_record(_, _, _, _) ->
    ok.

%% @private
%% Substrate read-side freshness wiring (MST_DB_DESIGN §18 item 8).
%% After a successful AE round, bump every shard the consumer
%% registered for this instance so long-quiet shards (no writer
%% activity) do not trip `{stale, _}` purely on inactivity.
%%
%% Uses `bondy_db_core_registry:bump_ae_targets/2` so the AE-side bump
%% shares a primitive — and timing semantics — with the applier-side
%% bump in `bondy_oplog_applier:bump_ae_targets/1`. Empty target list
%% is a strict no-op.
bump_ae_on_sync(Instance, Peer) ->
    case bondy_oplog_registry:ae_targets(Instance) of
        [] ->
            ok;
        undefined ->
            ok;
        Targets ->
            Now = erlang:monotonic_time(millisecond),
            {Bumped, NotFound} =
                bondy_db_core_registry:bump_ae_targets(Targets, Now),
            telemetry:execute(
                [bondy_oplog, sync, ae_bumped],
                #{count => Bumped, not_found => NotFound},
                #{instance_id => Instance, peer => Peer, now_ms => Now}
            ),
            ok
    end.

%% @private
%% Inserts pages into the local store. When the backend supports
%% concurrent writes (e.g. ETS), runs in this process — no gen_server
%% round-trip. Otherwise falls back to the gen_server merge_pages
%% call, which is required for backends whose store *is* the
%% gen_server's state (e.g. map_store).
merge_pages(Instance, Pages) when is_map(Pages) ->
    merge_pages(Instance, maps:values(Pages));
merge_pages(Instance, Pages) when is_list(Pages) ->
    case bondy_oplog_registry:mst(Instance) of
        undefined ->
            bondy_oplog_instance:merge_pages(Instance, Pages);
        MST ->
            Store = bondy_mst:store(MST),
            Caps = bondy_mst_store:capabilities(Store),
            case maps:get(concurrent_writes, Caps, false) of
                true ->
                    %% Direct insert in this process. The store
                    %% mutates in place (e.g. ETS); the gen_server's
                    %% MST handle wraps the same store and sees the
                    %% new pages on the next read.
                    lists:foreach(
                        fun(Page) ->
                            {_Hash, _MST1} = bondy_mst:put_page(MST, Page)
                        end,
                        Pages
                    ),
                    ok;
                false ->
                    bondy_oplog_instance:merge_pages(Instance, Pages)
            end
    end.

%% @private
default_max_iterations(Opts) ->
    maps:get(max_iterations, Opts, 16).

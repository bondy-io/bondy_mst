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
            run(Instance, Peer, Opts);
        {ok, Watermark, Snapshot} ->
            case
                bondy_oplog_instance:load_snapshot(
                    Instance, Watermark, Snapshot
                )
            of
                {ok, _} ->
                    run(Instance, Peer, Opts);
                {error, watermark_not_advancing} ->
                    %% Local watermark is already ≥ peer's. Skip the
                    %% snapshot install and proceed with plain sync.
                    run(Instance, Peer, Opts);
                {error, _} = E ->
                    E
            end;
        {error, _} = E ->
            E
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
%% Uses `bondy_mst_db_registry:bump_ae_targets/2` so the AE-side bump
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
                bondy_mst_db_registry:bump_ae_targets(Targets, Now),
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

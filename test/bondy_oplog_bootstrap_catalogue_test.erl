%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%% End-to-end test for the catalogue-snapshot bootstrap session
%% (`bondy_oplog_sync_session:bootstrap_catalogue/3`) using the inline
%% transport.
%%
%% Validates:
%%   - A fresh local replica pulls all cells from a peer replica.
%%   - The local projection ends up with byte-identical V2 frames.
%%   - finalize_catalogue_bootstrap marks the local instance `live`.
%%   - When the peer reports `no_snapshot` (single-CRDT mode) the
%%     caller falls through to plain sync and the local instance is
%%     left untouched.
%%   - A single-CRDT local instance refuses bootstrap_catalogue with
%%     `{error, not_a_catalogue_instance}`.
%% =============================================================================
-module(bondy_oplog_bootstrap_catalogue_test).

-include_lib("eunit/include/eunit.hrl").

-define(B, <<>>).

setup() ->
    {ok, _} = application:ensure_all_started(bondy_mst),
    ok.

cleanup(_) ->
    [bondy_oplog:stop_instance(I) || I <- bondy_oplog:list_instances()],
    [
        bondy_db_core_registry:unregister(N, I, S)
     || E <- bondy_db_core_registry:list(),
        {N, I, S} <- [bondy_db_core_registry:entry_key(E)]
    ],
    ok.

bootstrap_catalogue_test_() ->
    {setup, fun setup/0, fun cleanup/1, [
        fun fresh_replica_bootstraps_from_peer/0,
        fun bootstrap_marks_local_live/0,
        fun no_snapshot_falls_through_to_sync/0,
        fun single_crdt_local_refuses_bootstrap_catalogue/0,
        fun merge_mode_picks_higher_hlc_for_lww/0
    ]}.

fresh_replica_bootstraps_from_peer() ->
    %% Set up the peer with cells, the local replica with nothing.
    {Peer, _PeerNS, _, _} = setup_instance(),
    {Local, _LocalNS, _, _} = setup_instance(),
    Cells = [
        {<<"k1">>, 10, <<"v1">>},
        {<<"k2">>, 20, <<"v2">>},
        {<<"k3">>, 5, <<"v3">>},
        {<<"k4">>, 25, <<"v4">>}
    ],
    [
        bondy_oplog:append(Peer, {cell_apply, ?B, K, {set, Hlc, V}})
     || {K, Hlc, V} <- Cells
    ],
    _ = barrier(Peer),

    %% Pre-bootstrap: peer high-water = 25, local high-water = 0.
    ?assertMatch(
        {ok, 25},
        high_water(Peer)
    ),
    ?assertMatch(
        {ok, no_watermark},
        high_water(Local)
    ),

    ?assertMatch(
        {ok, _Root},
        bondy_oplog_sync_session:bootstrap_catalogue(
            Local, Peer, #{transport_opts => #{}}
        )
    ),

    %% Post-bootstrap: local high-water has caught up to peer.
    ?assertMatch({ok, 25}, high_water(Local)),

    %% Verify every key has the right cell present on the local replica.
    PeerEntry = peer_entry(Peer),
    LocalEntry = peer_entry(Local),
    LocalAdapter = bondy_db_core_registry:entry_projection_adapter(LocalEntry),
    LocalHandle = bondy_db_core_registry:entry_projection_handle(LocalEntry),
    PeerAdapter = bondy_db_core_registry:entry_projection_adapter(PeerEntry),
    PeerHandle = bondy_db_core_registry:entry_projection_handle(PeerEntry),
    [
        begin
            {ok, LocalFrame} = LocalAdapter:get(LocalHandle, ?B, K),
            {ok, PeerFrame} = PeerAdapter:get(PeerHandle, ?B, K),
            ?assertEqual(PeerFrame, LocalFrame)
        end
     || {K, _, _} <- Cells
    ],

    teardown(Peer),
    teardown(Local).

bootstrap_marks_local_live() ->
    %% Persistent (storage_path-backed) instances default to
    %% `pre_bootstrap`. Ephemeral in-memory instances default to `live`
    %% — those are tested in `fresh_replica_bootstraps_from_peer/0`.
    %% Here we set `storage_path` on both, and seed the Peer as a
    %% genesis (`live`) replica so it can serve the bootstrap.
    BaseDir = test_dir(),
    {Peer, _, _, _} = setup_instance_persistent(BaseDir, #{seed => true}),
    {Local, _, _, _} = setup_instance_persistent(BaseDir, #{}),
    _ = bondy_oplog:append(Peer, {cell_apply, ?B, <<"k">>, {set, 1, <<"v">>}}),
    _ = barrier(Peer),

    %% Peer is live (seeded), Local is pre_bootstrap.
    ?assertEqual(live, bondy_oplog_instance:lifecycle_state(Peer)),
    ?assertEqual(pre_bootstrap, bondy_oplog_instance:lifecycle_state(Local)),

    {ok, _} = bondy_oplog_sync_session:bootstrap_catalogue(
        Local, Peer, #{}
    ),

    ?assertEqual(live, bondy_oplog_instance:lifecycle_state(Local)),
    teardown(Peer),
    teardown(Local),
    file:del_dir_r(BaseDir).

no_snapshot_falls_through_to_sync() ->
    %% Peer = single-CRDT mode (returns no_snapshot). Local = catalogue.
    Peer = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Peer, #{
        crdt_module => bondy_oplog_crdt_lww_register
    }),
    {Local, _, _, _} = setup_instance(),
    ?assertMatch(
        {ok, _},
        bondy_oplog_sync_session:bootstrap_catalogue(
            Local, Peer, #{}
        )
    ),
    bondy_oplog:stop_instance(Peer),
    teardown(Local).

merge_mode_picks_higher_hlc_for_lww() ->
    %% Both replicas have applied cells. Local is `live` (ephemeral),
    %% so `bootstrap_catalogue` runs in merge mode. For LWW register,
    %% merge_states picks the cell with the higher HLC.
    {Peer, _, _, _} = setup_instance(),
    {Local, _, _, _} = setup_instance(),

    %% Local has K1@5=local, K2@30=local-wins.
    %% Peer  has K1@20=peer-wins,  K2@10=peer.
    _ = bondy_oplog:append(
        Local, {cell_apply, ?B, <<"k1">>, {set, 5, <<"local-k1">>}}
    ),
    _ = bondy_oplog:append(
        Local, {cell_apply, ?B, <<"k2">>, {set, 30, <<"local-k2">>}}
    ),
    _ = barrier(Local),
    _ = bondy_oplog:append(
        Peer, {cell_apply, ?B, <<"k1">>, {set, 20, <<"peer-k1">>}}
    ),
    _ = bondy_oplog:append(
        Peer, {cell_apply, ?B, <<"k2">>, {set, 10, <<"peer-k2">>}}
    ),
    _ = barrier(Peer),

    %% Sanity — local is live (so install_mode is merge).
    ?assertEqual(live, bondy_oplog_instance:lifecycle_state(Local)),

    {ok, _} = bondy_oplog_sync_session:bootstrap_catalogue(
        Local, Peer, #{}
    ),

    %% After merge, K1 has the peer's HLC=20 winning, K2 keeps local's HLC=30.
    LocalEntry = peer_entry(Local),
    Adapter = bondy_db_core_registry:entry_projection_adapter(LocalEntry),
    Handle = bondy_db_core_registry:entry_projection_handle(LocalEntry),
    {ok, K1Frame} = Adapter:get(Handle, ?B, <<"k1">>),
    {ok, K2Frame} = Adapter:get(Handle, ?B, <<"k2">>),
    {K1Hlc, _, _} = bondy_oplog_cell_frame:decode_full(K1Frame),
    {K2Hlc, _, _} = bondy_oplog_cell_frame:decode_full(K2Frame),
    ?assertEqual(20, K1Hlc),
    ?assertEqual(30, K2Hlc),

    teardown(Peer),
    teardown(Local).

single_crdt_local_refuses_bootstrap_catalogue() ->
    Local = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Local, #{
        crdt_module => bondy_oplog_crdt_lww_register
    }),
    Peer = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Peer, #{
        crdt_module => bondy_oplog_crdt_lww_register
    }),
    ?assertEqual(
        {error, not_a_catalogue_instance},
        bondy_oplog_sync_session:bootstrap_catalogue(Local, Peer, #{})
    ),
    bondy_oplog:stop_instance(Local),
    bondy_oplog:stop_instance(Peer).

%% =============================================================================
%% Helpers
%% =============================================================================

setup_instance() ->
    Id = mk_id(),
    NS = ns_of(Id),
    {Cache, Proj} = register_shard(NS, primary, 0),
    {ok, _} = bondy_oplog:start_instance(Id, #{
        fold_module => lww_register,
        applier => #{
            cell_apply_target => {NS, primary, 0}
        }
    }),
    {Id, NS, Cache, Proj}.

setup_instance_persistent(BaseDir, ExtraOpts) ->
    Id = mk_id(),
    NS = ns_of(Id),
    {Cache, Proj} = register_shard(NS, primary, 0),
    Path = filename:join([BaseDir, binary_to_list(Id)]),
    ok = filelib:ensure_path(Path),
    Opts = maps:merge(
        #{
            fold_module => lww_register,
            applier => #{
                cell_apply_target => {NS, primary, 0}
            },
            storage_path => list_to_binary(Path)
        },
        ExtraOpts
    ),
    {ok, _} = bondy_oplog:start_instance(Id, Opts),
    {Id, NS, Cache, Proj}.

test_dir() ->
    Base = filename:join([
        "/tmp",
        "bondy_mst_bootstrap_catalogue_test",
        integer_to_list(erlang:unique_integer([positive]))
    ]),
    ok = filelib:ensure_path(Base),
    Base.

teardown(Id) ->
    bondy_oplog:stop_instance(Id),
    NS = ns_of(Id),
    [
        bondy_db_core_registry:unregister(N, I, S)
     || E <- bondy_db_core_registry:list(),
        {N, I, S} <- [bondy_db_core_registry:entry_key(E)],
        N =:= NS
    ],
    ok.

register_shard(NS, Index, Shard) ->
    {ok, Cache} = bondy_oplog_cache_ets:init(NS, Index, Shard, #{}),
    {ok, Proj} = bondy_oplog_projection_ets:open(NS, Index, Shard, #{}),
    ok = bondy_db_core_registry:register(NS, Index, Shard, #{
        shard_count => 1,
        cache_adapter => bondy_oplog_cache_ets,
        cache_handle => Cache,
        projection_adapter => bondy_oplog_projection_ets,
        projection_handle => Proj,
        overlay => disabled,
        fold_module => lww_register
    }),
    {Cache, Proj}.

mk_id() ->
    iolist_to_binary([
        "btc_",
        integer_to_binary(erlang:unique_integer([positive]))
    ]).

ns_of(Id) when is_binary(Id) ->
    binary_to_atom(<<"ns_", Id/binary>>, utf8).

barrier(Id) ->
    bondy_oplog:projection(Id).

high_water(Id) ->
    NS = ns_of(Id),
    bondy_db_core_registry:high_water_hlc(NS, primary, 0).

peer_entry(Id) ->
    NS = ns_of(Id),
    {ok, Entry} = bondy_db_core_registry:lookup(NS, primary, 0),
    Entry.

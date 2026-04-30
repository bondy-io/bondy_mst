%% Stage 12: crypto validator tests.

-module(bondy_oplog_validator_crypto_test).

-include_lib("eunit/include/eunit.hrl").
-include("bondy_oplog.hrl").

%% =============================================================================
%% UNIT TESTS — direct against the validator module
%% =============================================================================

sign_then_verify_test() ->
    {Pub, Priv} = generate_keypair(),
    Origin = origin_from_pubkey(Pub),
    {ok, SState} = init_state({Pub, Priv}, #{Origin => Pub}),
    Event = mk_event(Origin, 1),
    {Signed, _SState1} =
        bondy_oplog_validator_crypto:sign_event(Event, SState),
    %% Signed event has both fields populated.
    ?assert(is_binary(bondy_oplog_event:prev_hash(Signed))),
    ?assert(is_binary(bondy_oplog_event:signature(Signed))),
    %% A peer with the same pub key map can verify it.
    {ok, VState} = init_state(undefined, #{Origin => Pub}),
    ?assertEqual(
        ok,
        bondy_oplog_validator_crypto:verify_event(Signed, VState)
    ).

verify_rejects_tampered_payload_test() ->
    {Pub, Priv} = generate_keypair(),
    Origin = origin_from_pubkey(Pub),
    {ok, SState} = init_state({Pub, Priv}, #{Origin => Pub}),
    Event = mk_event(Origin, 1),
    {Signed, _} =
        bondy_oplog_validator_crypto:sign_event(Event, SState),
    %% Tamper with the op.
    Tampered = bondy_oplog_event:new(
        bondy_oplog_event:key(Signed),
        {tampered_op},
        bondy_oplog_event:meta(Signed),
        bondy_oplog_event:prev_hash(Signed),
        bondy_oplog_event:signature(Signed)
    ),
    {ok, VState} = init_state(undefined, #{Origin => Pub}),
    ?assertEqual(
        {error, invalid_signature},
        bondy_oplog_validator_crypto:verify_event(Tampered, VState)
    ).

verify_rejects_tampered_signature_test() ->
    {Pub, Priv} = generate_keypair(),
    Origin = origin_from_pubkey(Pub),
    {ok, SState} = init_state({Pub, Priv}, #{Origin => Pub}),
    Event = mk_event(Origin, 1),
    {Signed, _} =
        bondy_oplog_validator_crypto:sign_event(Event, SState),
    BadSig = crypto:strong_rand_bytes(64),
    Tampered = bondy_oplog_event:set_signature(Signed, BadSig),
    {ok, VState} = init_state(undefined, #{Origin => Pub}),
    ?assertEqual(
        {error, invalid_signature},
        bondy_oplog_validator_crypto:verify_event(Tampered, VState)
    ).

verify_rejects_unknown_origin_by_default_test() ->
    {Pub, Priv} = generate_keypair(),
    Origin = origin_from_pubkey(Pub),
    {ok, SState} = init_state({Pub, Priv}, #{Origin => Pub}),
    Event = mk_event(Origin, 1),
    {Signed, _} =
        bondy_oplog_validator_crypto:sign_event(Event, SState),
    %% Verifier knows nothing about this origin.
    {ok, VState} = init_state(undefined, #{}),
    ?assertMatch(
        {error, {unknown_origin, _}},
        bondy_oplog_validator_crypto:verify_event(Signed, VState)
    ).

verify_accepts_unknown_origin_when_opted_in_test() ->
    {Pub, Priv} = generate_keypair(),
    Origin = origin_from_pubkey(Pub),
    {ok, SState} = init_state({Pub, Priv}, #{Origin => Pub}),
    Event = mk_event(Origin, 1),
    {Signed, _} =
        bondy_oplog_validator_crypto:sign_event(Event, SState),
    {ok, VState} =
        bondy_oplog_validator_crypto:init(<<"i">>, #{
            peer_pubkeys => #{},
            accept_unknown_origin => true
        }),
    ?assertEqual(
        ok,
        bondy_oplog_validator_crypto:verify_event(Signed, VState)
    ).

hash_chain_links_consecutive_events_test() ->
    {Pub, Priv} = generate_keypair(),
    Origin = origin_from_pubkey(Pub),
    {ok, S0} = init_state({Pub, Priv}, #{Origin => Pub}),
    E1 = mk_event(Origin, 1),
    {Signed1, S1} =
        bondy_oplog_validator_crypto:sign_event(E1, S0),
    E2 = mk_event(Origin, 2),
    {Signed2, _S2} =
        bondy_oplog_validator_crypto:sign_event(E2, S1),
    %% Signed2's prev_hash must equal hash(Signed1).
    ExpectedPrev = sha_hash(
        canonical_blob(Signed1),
        bondy_oplog_event:signature(Signed1)
    ),
    ?assertEqual(ExpectedPrev, bondy_oplog_event:prev_hash(Signed2)).

equivocation_detected_for_distinct_signatures_at_same_key_test() ->
    {Pub, Priv} = generate_keypair(),
    Origin = origin_from_pubkey(Pub),
    {ok, S0} = init_state({Pub, Priv}, #{Origin => Pub}),
    E = mk_event(Origin, 1),
    {Signed, _} =
        bondy_oplog_validator_crypto:sign_event(E, S0),
    %% Synthesise a "fork": same key, different op, signed with the
    %% same private key.
    EFork = bondy_oplog_event:new(
        bondy_oplog_event:key(E),
        {forked_op},
        undefined
    ),
    {Signed2, _} =
        bondy_oplog_validator_crypto:sign_event(EFork, S0),
    ?assertNotEqual(
        bondy_oplog_event:signature(Signed),
        bondy_oplog_event:signature(Signed2)
    ),
    ?assertMatch(
        {equivocation, #{origin := Origin}},
        bondy_oplog_validator_crypto:detect_equivocation(
            Signed, Signed2
        )
    ).

equivocation_returns_ok_for_identical_events_test() ->
    {Pub, Priv} = generate_keypair(),
    Origin = origin_from_pubkey(Pub),
    {ok, S0} = init_state({Pub, Priv}, #{Origin => Pub}),
    E = mk_event(Origin, 1),
    {Signed, _} =
        bondy_oplog_validator_crypto:sign_event(E, S0),
    %% Same event passed twice — Ed25519 is deterministic, so signatures
    %% match and detect_equivocation must NOT flag it.
    ?assertEqual(
        ok,
        bondy_oplog_validator_crypto:detect_equivocation(
            Signed, Signed
        )
    ).

%% =============================================================================
%% INTEGRATION — wire the crypto validator into a real instance
%% =============================================================================

integration_setup() ->
    {ok, _} = application:ensure_all_started(bondy_mst),
    bondy_oplog_sync_scheduler:set_dispatch(undefined),
    bondy_oplog_gc_scheduler:set_trigger(undefined),
    ok.

integration_cleanup(_) ->
    [
        bondy_oplog:stop_instance(I)
     || I <- bondy_oplog:list_instances()
    ],
    ok.

integration_test_() ->
    {setup, fun integration_setup/0, fun integration_cleanup/1, [
        fun instance_with_crypto_validator_signs_events/0,
        fun instance_rejects_tampered_remote_event/0
    ]}.

%% Bring up an instance configured with the crypto validator. Verify
%% that local appends produce signed events with non-empty prev_hash
%% and signature.
instance_with_crypto_validator_signs_events() ->
    {Pub, Priv} = generate_keypair(),
    Origin = origin_from_pubkey(Pub),
    Id = mk_id(),
    {ok, _} = bondy_oplog:start_instance(Id, #{
        origin => Origin,
        validator => bondy_oplog_validator_crypto,
        validator_opts => #{
            keypair => {Pub, Priv},
            peer_pubkeys => #{Origin => Pub}
        },
        crdt_module => bondy_oplog_test_counter
    }),
    K = bondy_oplog:append(Id, {inc, 7}),
    {ok, Stored} = bondy_oplog:get(Id, K),
    ?assert(is_binary(bondy_oplog_event:signature(Stored))),
    ?assert(is_binary(bondy_oplog_event:prev_hash(Stored))),
    ok = bondy_oplog:stop_instance(Id).

%% Two instances configured with mutual public keys. A tampered
%% remote event delivered via append_remote/2 is rejected.
instance_rejects_tampered_remote_event() ->
    {PubA, PrivA} = generate_keypair(),
    {PubB, PrivB} = generate_keypair(),
    OriginA = origin_from_pubkey(PubA),
    OriginB = origin_from_pubkey(PubB),
    Pubkeys = #{OriginA => PubA, OriginB => PubB},
    IdA = mk_id(),
    IdB = mk_id(),
    {ok, _} = bondy_oplog:start_instance(IdA, #{
        origin => OriginA,
        validator => bondy_oplog_validator_crypto,
        validator_opts => #{
            keypair => {PubA, PrivA},
            peer_pubkeys => Pubkeys
        }
    }),
    {ok, _} = bondy_oplog:start_instance(IdB, #{
        origin => OriginB,
        validator => bondy_oplog_validator_crypto,
        validator_opts => #{
            keypair => {PubB, PrivB},
            peer_pubkeys => Pubkeys
        }
    }),
    %% B appends → has a signed event.
    Key = bondy_oplog:append(IdB, {inc, 1}),
    {ok, BEvent} = bondy_oplog:get(IdB, Key),
    %% Tamper: rewrite the op while keeping the signature.
    Tampered = bondy_oplog_event:new(
        bondy_oplog_event:key(BEvent),
        {evil_op},
        bondy_oplog_event:meta(BEvent),
        bondy_oplog_event:prev_hash(BEvent),
        bondy_oplog_event:signature(BEvent)
    ),
    %% A receives — must reject.
    ?assertEqual(
        {error, invalid_signature},
        bondy_oplog:append_remote(IdA, Tampered)
    ),
    %% A's MST is unchanged.
    ?assertEqual(0, bondy_oplog:size(IdA)),
    ok.

%% =============================================================================
%% HELPERS
%% =============================================================================

generate_keypair() ->
    crypto:generate_key(eddsa, ed25519).

origin_from_pubkey(Pub) ->
    crypto:hash(sha256, Pub).

init_state(Keypair, PeerPubkeys) ->
    bondy_oplog_validator_crypto:init(<<"test_inst">>, #{
        keypair => Keypair,
        peer_pubkeys => PeerPubkeys
    }).

mk_event(Origin, Seq) ->
    Hlc = bondy_oplog_hlc:encode(1_700_000_000_000 + Seq, 0),
    Key = bondy_oplog_event:key(Hlc, Origin, Seq),
    bondy_oplog_event:new(Key, {op, Seq}, undefined).

mk_id() ->
    list_to_binary(
        "cv_" ++
            integer_to_list(
                erlang:unique_integer([positive, monotonic])
            )
    ).

canonical_blob(Event) ->
    erlang:term_to_binary(
        {
            bondy_oplog_event:key(Event),
            bondy_oplog_event:op(Event),
            bondy_oplog_event:meta(Event),
            bondy_oplog_event:prev_hash(Event)
        },
        [{minor_version, 2}]
    ).

sha_hash(Payload, Sig) ->
    crypto:hash(sha256, <<Payload/binary, Sig/binary>>).

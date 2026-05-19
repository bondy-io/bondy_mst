%% =============================================================================
%% EUnit + PropEr suite for `bondy_mst_pack_writer` and
%% `bondy_mst_pack_reader`. Covers:
%%
%% 1. Writer lifecycle:
%%    - open in an empty dir produces a fresh manifest + creates
%%      incoming.pack with just the header.
%%    - append round-trip: hash returned matches sha256(page),
%%      pending_lookup recovers offset, incoming_offset advances.
%%    - append is idempotent for a hash already in pending.
%%    - close/reopen preserves the pending map by scanning
%%      incoming.pack.
%% 2. Seal lifecycle:
%%    - seal on empty pending is a no-op.
%%    - seal materialises pack-NNNN.pack + pack-NNNN.idx and
%%      removes incoming.pack.
%%    - manifest reflects the new pack and incoming_pack=absent.
%%    - next_pack_id advances.
%%    - subsequent appends start a fresh incoming.pack.
%% 3. Reader:
%%    - open after seal sees every sealed pack.
%%    - get/2 returns the original page bytes for every appended
%%      hash; not_found for an arbitrary hash.
%%    - get/2 across multiple sealed packs short-circuits on the
%%      newest pack first.
%%    - list/1 enumerates every appended hash.
%%    - has/2 mirrors get/2's true/false answer.
%% 4. End-to-end PropEr:
%%    - For any sequence of N appends (with possible dedup hits),
%%      sealing + opening a reader resolves every distinct hash
%%      back to its original page.
%% =============================================================================

-module(bondy_mst_pack_writer_test).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("bondy_mst_pack.hrl").

%% =============================================================================
%% Fixture helpers
%% =============================================================================

mktemp_dir() ->
    Base = filename:join(
        ["/tmp", io_lib:format("bondy_mst_pack_writer_test_~p_~p",
                              [erlang:system_time(microsecond),
                               erlang:unique_integer([positive])])]
    ),
    Dir = lists:flatten(Base),
    ok = filelib:ensure_path(Dir),
    Dir.

rmrf(Dir) ->
    _ = file:del_dir_r(Dir),
    ok.

with_tmp_dir(Fun) ->
    Dir = mktemp_dir(),
    try Fun(Dir)
    after rmrf(Dir)
    end.

open_writer(Dir) ->
    bondy_mst_pack_writer:open(Dir, #{instance_id => <<"writer-test">>}).

sha256(Bin) ->
    crypto:hash(sha256, Bin).

%% =============================================================================
%% Open / close
%% =============================================================================

open_empty_dir_creates_manifest_lazy_incoming_test() ->
    with_tmp_dir(fun(Dir) ->
        {ok, W} = open_writer(Dir),
        try
            %% Manifest exists on disk and matches the in-memory state.
            {ok, OnDisk} = bondy_mst_pack_manifest:read(Dir),
            ?assertEqual(bondy_mst_pack_manifest:instance_id(OnDisk),
                         <<"writer-test">>),
            ?assertEqual(sha256, bondy_mst_pack_manifest:hash_algo(OnDisk)),
            ?assertEqual([], bondy_mst_pack_manifest:sealed_packs(OnDisk)),
            ?assertEqual(absent,
                         bondy_mst_pack_manifest:incoming_pack(OnDisk)),
            %% Lazy creation: incoming.pack does NOT exist until the
            %% first append. This keeps `open ; close` cycles a no-op
            %% against the on-disk state.
            ?assertNot(filelib:is_regular(
                bondy_mst_pack_paths:incoming_pack_path(Dir))),
            ?assertEqual(0, bondy_mst_pack_writer:incoming_offset(W)),
            ?assertEqual(1, bondy_mst_pack_writer:next_pack_id(W)),
            ?assertEqual(0, bondy_mst_pack_writer:pending_count(W))
        after
            bondy_mst_pack_writer:close(W)
        end
    end).

first_append_materialises_incoming_and_flips_manifest_test() ->
    with_tmp_dir(fun(Dir) ->
        {ok, W} = open_writer(Dir),
        try
            {ok, _, W1} = bondy_mst_pack_writer:append(W, <<"x">>),
            ?assert(filelib:is_regular(
                bondy_mst_pack_paths:incoming_pack_path(Dir))),
            %% 48-byte pack header + 40-byte record header + 1-byte page.
            ?assertEqual(48 + 40 + 1,
                         bondy_mst_pack_writer:incoming_offset(W1)),
            {ok, OnDisk} = bondy_mst_pack_manifest:read(Dir),
            ?assertEqual(present,
                         bondy_mst_pack_manifest:incoming_pack(OnDisk))
        after
            bondy_mst_pack_writer:close(W)
        end
    end).

open_missing_instance_id_rejected_test() ->
    with_tmp_dir(fun(Dir) ->
        ?assertEqual(
            {error, {missing_field, instance_id}},
            bondy_mst_pack_writer:open(Dir, #{})
        )
    end).

reopen_uses_existing_manifest_test() ->
    with_tmp_dir(fun(Dir) ->
        {ok, W1} = open_writer(Dir),
        bondy_mst_pack_writer:close(W1),
        {ok, W2} = open_writer(Dir),
        try
            ?assertEqual(<<"writer-test">>,
                         bondy_mst_pack_writer:instance_id(W2)),
            ?assertEqual(0, bondy_mst_pack_writer:pending_count(W2))
        after
            bondy_mst_pack_writer:close(W2)
        end
    end).

reopen_with_different_instance_id_rejected_test() ->
    with_tmp_dir(fun(Dir) ->
        {ok, W1} = open_writer(Dir),
        bondy_mst_pack_writer:close(W1),
        ?assertMatch(
            {error, {instance_id_mismatch, _, _}},
            bondy_mst_pack_writer:open(Dir, #{instance_id => <<"other">>})
        )
    end).

%% =============================================================================
%% Append
%% =============================================================================

append_returns_correct_hash_test() ->
    with_tmp_dir(fun(Dir) ->
        {ok, W} = open_writer(Dir),
        try
            Page = <<"hello, world">>,
            {ok, Hash, W1} = bondy_mst_pack_writer:append(W, Page),
            ?assertEqual(sha256(Page), Hash),
            ?assertEqual(1, bondy_mst_pack_writer:pending_count(W1)),
            {ok, {_Off, Len}} = bondy_mst_pack_writer:pending_lookup(W1, Hash),
            ?assertEqual(byte_size(Page), Len)
        after
            bondy_mst_pack_writer:close(W)
        end
    end).

append_advances_offset_test() ->
    with_tmp_dir(fun(Dir) ->
        {ok, W} = open_writer(Dir),
        try
            %% First append also writes the 48-byte pack header. Use a
            %% second append to verify the per-record delta.
            {ok, _, W1} = bondy_mst_pack_writer:append(W, <<"prime">>),
            Off1 = bondy_mst_pack_writer:incoming_offset(W1),
            Page = <<"hello">>,
            {ok, _, W2} = bondy_mst_pack_writer:append(W1, Page),
            Off2 = bondy_mst_pack_writer:incoming_offset(W2),
            ?assertEqual(Off1 + 40 + byte_size(Page), Off2)
        after
            bondy_mst_pack_writer:close(W)
        end
    end).

append_is_idempotent_test() ->
    with_tmp_dir(fun(Dir) ->
        {ok, W} = open_writer(Dir),
        try
            Page = <<"dup">>,
            {ok, H, W1} = bondy_mst_pack_writer:append(W, Page),
            Off1 = bondy_mst_pack_writer:incoming_offset(W1),
            {ok, H, W2} = bondy_mst_pack_writer:append(W1, Page),
            %% Second append is a no-op: offset & pending unchanged.
            ?assertEqual(Off1, bondy_mst_pack_writer:incoming_offset(W2)),
            ?assertEqual(1, bondy_mst_pack_writer:pending_count(W2))
        after
            bondy_mst_pack_writer:close(W)
        end
    end).

append_then_reopen_preserves_pending_test() ->
    with_tmp_dir(fun(Dir) ->
        {ok, W} = open_writer(Dir),
        Pages = [<<"page-", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 5)],
        W1 = lists:foldl(
            fun(P, Acc) ->
                {ok, _, A} = bondy_mst_pack_writer:append(Acc, P),
                A
            end,
            W,
            Pages
        ),
        Off = bondy_mst_pack_writer:incoming_offset(W1),
        Hashes = bondy_mst_pack_writer:pending_hashes(W1),
        bondy_mst_pack_writer:close(W1),
        {ok, W2} = open_writer(Dir),
        try
            ?assertEqual(Off, bondy_mst_pack_writer:incoming_offset(W2)),
            ?assertEqual(Hashes, bondy_mst_pack_writer:pending_hashes(W2))
        after
            bondy_mst_pack_writer:close(W2)
        end
    end).

%% =============================================================================
%% Seal
%% =============================================================================

seal_on_empty_pending_is_noop_test() ->
    with_tmp_dir(fun(Dir) ->
        {ok, W} = open_writer(Dir),
        try
            ?assertMatch({ok, no_op, _}, bondy_mst_pack_writer:seal(W))
        after
            bondy_mst_pack_writer:close(W)
        end
    end).

seal_materialises_pack_and_idx_test() ->
    with_tmp_dir(fun(Dir) ->
        {ok, W} = open_writer(Dir),
        Pages = [<<"alpha">>, <<"beta">>, <<"gamma">>],
        W1 = lists:foldl(
            fun(P, Acc) ->
                {ok, _, A} = bondy_mst_pack_writer:append(Acc, P),
                A
            end,
            W,
            Pages
        ),
        {ok, PackId, W2} = bondy_mst_pack_writer:seal(W1),
        try
            ?assertEqual(1, PackId),
            ?assert(filelib:is_regular(
                bondy_mst_pack_paths:sealed_pack_path(Dir, 1))),
            ?assert(filelib:is_regular(
                bondy_mst_pack_paths:sealed_idx_path(Dir, 1))),
            %% Post-seal the writer is in fresh-state: no incoming fd,
            %% offset 0; the next append re-creates incoming.pack lazily.
            ?assertNot(filelib:is_regular(
                bondy_mst_pack_paths:incoming_pack_path(Dir))),
            ?assertEqual(0, bondy_mst_pack_writer:incoming_offset(W2)),
            ?assertEqual(0, bondy_mst_pack_writer:pending_count(W2)),
            ?assertEqual(2, bondy_mst_pack_writer:next_pack_id(W2)),
            %% Manifest reflects the new pack.
            {ok, M} = bondy_mst_pack_manifest:read(Dir),
            ?assertEqual([1], bondy_mst_pack_manifest:sealed_packs(M)),
            ?assertEqual(absent,
                         bondy_mst_pack_manifest:incoming_pack(M))
        after
            bondy_mst_pack_writer:close(W2)
        end
    end).

seal_then_append_advances_to_next_pack_id_test() ->
    with_tmp_dir(fun(Dir) ->
        {ok, W} = open_writer(Dir),
        {ok, _, W1} = bondy_mst_pack_writer:append(W, <<"first">>),
        {ok, 1, W2} = bondy_mst_pack_writer:seal(W1),
        {ok, _, W3} = bondy_mst_pack_writer:append(W2, <<"second">>),
        {ok, 2, W4} = bondy_mst_pack_writer:seal(W3),
        try
            ?assert(filelib:is_regular(
                bondy_mst_pack_paths:sealed_pack_path(Dir, 1))),
            ?assert(filelib:is_regular(
                bondy_mst_pack_paths:sealed_pack_path(Dir, 2))),
            {ok, M} = bondy_mst_pack_manifest:read(Dir),
            ?assertEqual([1, 2], bondy_mst_pack_manifest:sealed_packs(M)),
            ?assertEqual(3, bondy_mst_pack_writer:next_pack_id(W4))
        after
            bondy_mst_pack_writer:close(W4)
        end
    end).

%% =============================================================================
%% Reader — basic
%% =============================================================================

reader_open_no_sealed_packs_test() ->
    with_tmp_dir(fun(Dir) ->
        {ok, W} = open_writer(Dir),
        bondy_mst_pack_writer:close(W),
        {ok, R} = bondy_mst_pack_reader:open(Dir),
        try
            ?assertEqual([], bondy_mst_pack_reader:sealed_pack_ids(R)),
            ?assertEqual([], bondy_mst_pack_reader:list(R)),
            ?assertEqual(not_found,
                         bondy_mst_pack_reader:get(R, sha256(<<"nope">>)))
        after
            bondy_mst_pack_reader:close(R)
        end
    end).

reader_resolves_every_sealed_page_test() ->
    with_tmp_dir(fun(Dir) ->
        Pages = [<<"alpha">>, <<"beta">>, <<"gamma">>, <<"delta">>],
        Hashes = seal_pages(Dir, Pages),
        {ok, R} = bondy_mst_pack_reader:open(Dir),
        try
            ?assertEqual([1], bondy_mst_pack_reader:sealed_pack_ids(R)),
            lists:foreach(
                fun({H, P}) ->
                    ?assertEqual({ok, P}, bondy_mst_pack_reader:get(R, H)),
                    ?assert(bondy_mst_pack_reader:has(R, H))
                end,
                lists:zip(Hashes, Pages)
            ),
            ?assertEqual(not_found,
                         bondy_mst_pack_reader:get(R,
                                                   sha256(<<"missing">>))),
            ?assertNot(bondy_mst_pack_reader:has(R, sha256(<<"missing">>))),
            ?assertEqual(lists:sort(Hashes), bondy_mst_pack_reader:list(R))
        after
            bondy_mst_pack_reader:close(R)
        end
    end).

reader_iterates_multi_pack_test() ->
    with_tmp_dir(fun(Dir) ->
        PagesA = [<<"a1">>, <<"a2">>, <<"a3">>],
        PagesB = [<<"b1">>, <<"b2">>],
        HashesA = seal_pages(Dir, PagesA),
        HashesB = seal_pages(Dir, PagesB),
        {ok, R} = bondy_mst_pack_reader:open(Dir),
        try
            ?assertEqual([2, 1], bondy_mst_pack_reader:sealed_pack_ids(R)),
            All = lists:zip(HashesA ++ HashesB, PagesA ++ PagesB),
            lists:foreach(
                fun({H, P}) ->
                    ?assertEqual({ok, P}, bondy_mst_pack_reader:get(R, H))
                end,
                All
            ),
            ?assertEqual(
                lists:sort(HashesA ++ HashesB),
                bondy_mst_pack_reader:list(R)
            )
        after
            bondy_mst_pack_reader:close(R)
        end
    end).

reader_open_missing_manifest_test() ->
    with_tmp_dir(fun(Dir) ->
        ?assertMatch(
            {error, {manifest, enoent}},
            bondy_mst_pack_reader:open(Dir)
        )
    end).

%% =============================================================================
%% PropEr — end-to-end
%% =============================================================================

proper_writer_test_() ->
    Opts = [{numtests, 50}, {to_file, user}],
    [
        {timeout, 60,
         ?_assert(proper:quickcheck(prop_seal_then_read(), Opts))}
    ].

prop_seal_then_read() ->
    ?FORALL(
        Pages,
        ?LET(N, choose(0, 30),
             vector(N, ?LET(M, choose(0, 64), binary(M)))),
        with_tmp_dir_prop(fun(Dir) ->
            UniqueByHash = uniq_by_hash(Pages),
            {ok, W0} = open_writer(Dir),
            W1 = lists:foldl(
                fun(P, Acc) ->
                    {ok, _, A} = bondy_mst_pack_writer:append(Acc, P),
                    A
                end,
                W0,
                Pages
            ),
            ResultSeal = bondy_mst_pack_writer:seal(W1),
            ok = bondy_mst_pack_writer:close(
                case ResultSeal of
                    {ok, no_op, X} -> X;
                    {ok, _, X}     -> X
                end
            ),
            {ok, R} = bondy_mst_pack_reader:open(Dir),
            try
                lists:all(
                    fun({H, P}) ->
                        {ok, P} =:= bondy_mst_pack_reader:get(R, H)
                    end,
                    UniqueByHash
                )
            after
                bondy_mst_pack_reader:close(R)
            end
        end)
    ).

%% =============================================================================
%% Helpers
%% =============================================================================

%% @private Open + append + seal in one shot; returns the hashes (in
%% append order). Closes the writer.
seal_pages(Dir, Pages) ->
    {ok, W} = open_writer(Dir),
    {Hashes, W1} = lists:foldl(
        fun(P, {Hs, Acc}) ->
            {ok, H, A} = bondy_mst_pack_writer:append(Acc, P),
            {[H | Hs], A}
        end,
        {[], W},
        Pages
    ),
    case bondy_mst_pack_writer:seal(W1) of
        {ok, no_op, W2} ->
            bondy_mst_pack_writer:close(W2);
        {ok, PackId, W2} when is_integer(PackId) ->
            bondy_mst_pack_writer:close(W2)
    end,
    lists:reverse(Hashes).

%% @private Dedup pages by sha256 hash, keep first occurrence.
uniq_by_hash(Pages) ->
    {Seen, Acc} = lists:foldl(
        fun(P, {S, A}) ->
            H = sha256(P),
            case maps:is_key(H, S) of
                true  -> {S, A};
                false -> {S#{H => true}, [{H, P} | A]}
            end
        end,
        {#{}, []},
        Pages
    ),
    _ = Seen,
    lists:reverse(Acc).

%% @private PropEr expects pure booleans; the standard with_tmp_dir
%% returns whatever its callback returns, so wrap.
with_tmp_dir_prop(Fun) ->
    Dir = mktemp_dir(),
    try Fun(Dir)
    after rmrf(Dir)
    end.

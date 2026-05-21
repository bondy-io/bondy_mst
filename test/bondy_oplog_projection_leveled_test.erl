%% =============================================================================
%% Adapter-level tests for `bondy_oplog_projection_leveled`.
%%
%% Each test starts its own leveled Bookie in a fresh temp directory,
%% exercises the adapter callbacks against it, and tears the Bookie
%% down. There is no `bondy_db` / shared-supervisor plumbing — the
%% adapter is verified in isolation against an inline-managed Bookie.
%%
%% Bucket is a first-class call-time parameter (`MST_DB_DESIGN.md` §18
%% item 14): every data callback (`get/3`, `put_batch/2`,
%% `range/5`, `delete/3`) takes it explicitly.
%%
%% Covers:
%%   - open/4 invalid-opts rejection (missing Bookie)
%%   - close/1 is a no-op against the underlying Bookie
%%   - get hit and miss
%%   - put_batch of varying sizes including empty
%%   - delete removes the (bucket, key) row
%%   - distinct buckets do not collide
%%   - range: empty, asc, desc, limit, half-open exclusion of High,
%%     limit smaller than range, limit larger than range
%%   - info returns the expected map shape
%% =============================================================================

-module(bondy_oplog_projection_leveled_test).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"test">>).
-define(MOD, bondy_oplog_projection_leveled).

%% =============================================================================
%% Test list
%% =============================================================================

adapter_test_() ->
    {foreach,
        fun setup/0,
        fun cleanup/1,
        [
            fun open_with_valid_opts/1,
            fun open_with_missing_bookie_is_rejected/1,
            fun close_is_a_noop/1,
            fun get_returns_not_found_for_missing_key/1,
            fun put_then_get_roundtrip/1,
            fun put_batch_with_multiple_entries/1,
            fun put_batch_with_empty_list/1,
            fun delete_removes_the_key/1,
            fun distinct_buckets_do_not_collide/1,
            fun range_returns_empty_for_no_data/1,
            fun range_excludes_the_high_bound/1,
            fun range_respects_limit/1,
            fun range_limit_larger_than_data_returns_all/1,
            fun range_asc_returns_ascending/1,
            fun range_desc_returns_reversed/1,
            fun info_reports_backend_and_bookie/1
        ]}.

%% =============================================================================
%% Setup / teardown
%% =============================================================================

setup() ->
    %% Per-test fresh Bookie in a fresh temp directory. The leveled
    %% tag hooks are wired here so the adapter can use `?BONDY_FOLD_TAG`
    %% without depending on `bondy_mst_app:start/2`.
    ok = bondy_oplog_leveled_tag:install(),
    Dir = make_tempdir(),
    {ok, Pid} = leveled_bookie:book_start(Dir, 2000, 100_000_000, none),
    {Pid, Dir}.

cleanup({Pid, Dir}) ->
    ok = leveled_bookie:book_close(Pid),
    rmrf(Dir),
    ok.

%% =============================================================================
%% Tests
%% =============================================================================

open_with_valid_opts({Pid, _Dir}) ->
    fun() ->
        {ok, Handle} = ?MOD:open(ns, idx, 0, #{bookie => Pid}),
        ?assertMatch(#{bookie := Pid}, Handle)
    end.


open_with_missing_bookie_is_rejected({_Pid, _Dir}) ->
    fun() ->
        ?assertMatch({error, {invalid_opts, _}},
                     ?MOD:open(ns, idx, 0, #{}))
    end.


close_is_a_noop({Pid, _Dir}) ->
    fun() ->
        H = handle(Pid),
        ?assertEqual(ok, ?MOD:close(H)),
        %% Bookie is still alive — close/1 did not touch it.
        ?assert(is_process_alive(Pid))
    end.


get_returns_not_found_for_missing_key({Pid, _Dir}) ->
    fun() ->
        H = handle(Pid),
        ?assertEqual(not_found, ?MOD:get(H, ?BUCKET, <<"nope">>))
    end.


put_then_get_roundtrip({Pid, _Dir}) ->
    fun() ->
        H = handle(Pid),
        F = mk_frame(<<"v1">>),
        ok = ?MOD:put_batch(H, [{?BUCKET, <<"k1">>, F}]),
        ?assertEqual({ok, F}, ?MOD:get(H, ?BUCKET, <<"k1">>))
    end.


put_batch_with_multiple_entries({Pid, _Dir}) ->
    fun() ->
        H = handle(Pid),
        Entries = [{?BUCKET, key_n(I), mk_frame(value_n(I))}
                   || I <- lists:seq(1, 10)],
        ok = ?MOD:put_batch(H, Entries),
        [?assertEqual({ok, mk_frame(value_n(I))},
                      ?MOD:get(H, ?BUCKET, key_n(I)))
            || I <- lists:seq(1, 10)],
        ok
    end.


put_batch_with_empty_list({Pid, _Dir}) ->
    fun() ->
        H = handle(Pid),
        ?assertEqual(ok, ?MOD:put_batch(H, []))
    end.


delete_removes_the_key({Pid, _Dir}) ->
    fun() ->
        H = handle(Pid),
        F = mk_frame(<<"v">>),
        ok = ?MOD:put_batch(H, [{?BUCKET, <<"k">>, F}]),
        ?assertEqual({ok, F}, ?MOD:get(H, ?BUCKET, <<"k">>)),
        ok = ?MOD:delete(H, ?BUCKET, <<"k">>),
        ?assertEqual(not_found, ?MOD:get(H, ?BUCKET, <<"k">>))
    end.


distinct_buckets_do_not_collide({Pid, _Dir}) ->
    fun() ->
        H = handle(Pid),
        F1 = mk_frame(<<"v1">>),
        F2 = mk_frame(<<"v2">>),
        ok = ?MOD:put_batch(H, [
            {<<"b1">>, <<"k">>, F1},
            {<<"b2">>, <<"k">>, F2}
        ]),
        ?assertEqual({ok, F1}, ?MOD:get(H, <<"b1">>, <<"k">>)),
        ?assertEqual({ok, F2}, ?MOD:get(H, <<"b2">>, <<"k">>))
    end.


range_returns_empty_for_no_data({Pid, _Dir}) ->
    fun() ->
        H = handle(Pid),
        ?assertEqual({ok, []},
                     ?MOD:range(H, ?BUCKET, <<"a">>, <<"z">>, #{}))
    end.


range_excludes_the_high_bound({Pid, _Dir}) ->
    fun() ->
        H = handle(Pid),
        ok = ?MOD:put_batch(H, [
            {?BUCKET, <<"k01">>, mk_frame(<<"v01">>)},
            {?BUCKET, <<"k02">>, mk_frame(<<"v02">>)},
            {?BUCKET, <<"k03">>, mk_frame(<<"v03">>)}
        ]),
        %% [k01, k03) — must include k01 and k02, exclude k03.
        {ok, Rows} = ?MOD:range(H, ?BUCKET, <<"k01">>, <<"k03">>,
                                #{limit => 100}),
        Keys = [K || {K, _} <- Rows],
        ?assertEqual([<<"k01">>, <<"k02">>], Keys)
    end.


range_respects_limit({Pid, _Dir}) ->
    fun() ->
        H = handle(Pid),
        Entries = [{?BUCKET, key_n(I), mk_frame(value_n(I))}
                   || I <- lists:seq(1, 10)],
        ok = ?MOD:put_batch(H, Entries),
        {ok, Rows} = ?MOD:range(H, ?BUCKET, key_n(1), key_n(11),
                                #{limit => 3}),
        ?assertEqual(3, length(Rows)),
        %% First three in ascending order.
        ?assertEqual([key_n(1), key_n(2), key_n(3)], [K || {K, _} <- Rows])
    end.


range_limit_larger_than_data_returns_all({Pid, _Dir}) ->
    fun() ->
        H = handle(Pid),
        Entries = [{?BUCKET, key_n(I), mk_frame(value_n(I))}
                   || I <- lists:seq(1, 5)],
        ok = ?MOD:put_batch(H, Entries),
        {ok, Rows} = ?MOD:range(H, ?BUCKET, key_n(1), key_n(99),
                                #{limit => 100}),
        ?assertEqual(5, length(Rows))
    end.


range_asc_returns_ascending({Pid, _Dir}) ->
    fun() ->
        H = handle(Pid),
        ok = ?MOD:put_batch(H, [
            {?BUCKET, <<"k01">>, mk_frame(<<"v01">>)},
            {?BUCKET, <<"k02">>, mk_frame(<<"v02">>)},
            {?BUCKET, <<"k03">>, mk_frame(<<"v03">>)}
        ]),
        {ok, Rows} = ?MOD:range(H, ?BUCKET, <<"k01">>, <<"k99">>,
                                #{direction => asc}),
        ?assertEqual([<<"k01">>, <<"k02">>, <<"k03">>], [K || {K, _} <- Rows])
    end.


range_desc_returns_reversed({Pid, _Dir}) ->
    fun() ->
        H = handle(Pid),
        ok = ?MOD:put_batch(H, [
            {?BUCKET, <<"k01">>, mk_frame(<<"v01">>)},
            {?BUCKET, <<"k02">>, mk_frame(<<"v02">>)},
            {?BUCKET, <<"k03">>, mk_frame(<<"v03">>)}
        ]),
        {ok, Rows} = ?MOD:range(H, ?BUCKET, <<"k01">>, <<"k99">>,
                                #{direction => desc}),
        %% Matches the ETS adapter's contract: take first Limit rows in
        %% asc, then reverse for desc — i.e., desc with no limit returns
        %% the full range reversed.
        ?assertEqual([<<"k03">>, <<"k02">>, <<"k01">>], [K || {K, _} <- Rows])
    end.


info_reports_backend_and_bookie({Pid, _Dir}) ->
    fun() ->
        H = handle(Pid),
        Info = ?MOD:info(H),
        ?assertMatch(#{backend := leveled, bookie := Pid}, Info)
    end.


%% =============================================================================
%% Helpers
%% =============================================================================

handle(Pid) ->
    {ok, H} = ?MOD:open(ns, idx, 0, #{bookie => Pid}),
    H.

key_n(I) ->
    list_to_binary(io_lib:format("k~3..0B", [I])).

value_n(I) ->
    list_to_binary(io_lib:format("v~3..0B", [I])).

%% Adapter-level tests want to verify get/put/range/delete with opaque
%% byte payloads but the leveled tag extractor now expects a V2 frame
%% (`bondy_oplog_cell_frame:encode/4`). Wrap arbitrary bytes in a
%% minimal V2 frame so the extractor succeeds; the adapter just stores
%% and returns the bytes round-trip.
mk_frame(Bytes) when is_binary(Bytes) ->
    bondy_oplog_cell_frame:encode(0, Bytes, Bytes, false).


make_tempdir() ->
    Base = filename:join([
        "/tmp",
        "bondy_mst_leveled_test",
        integer_to_list(erlang:unique_integer([positive, monotonic]))
    ]),
    ok = filelib:ensure_dir(filename:join(Base, ".keep")),
    Base.


rmrf(Dir) ->
    %% Best-effort cleanup; leveled lays out files under Dir/journal and
    %% Dir/ledger.
    case file:del_dir_r(Dir) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, Reason} ->
            io:format(user, "cleanup of ~p failed: ~p~n", [Dir, Reason]),
            ok
    end.

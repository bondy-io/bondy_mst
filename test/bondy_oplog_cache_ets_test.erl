%% =============================================================================
%% Tests for the reference `bondy_oplog_cache_adapter` implementation
%% (`bondy_oplog_cache_ets`).
%%
%% Pins the cache contract: get/put/delete round-trip, invalidate_all,
%% optional max_entries eviction, info shape.
%% =============================================================================

-module(bondy_oplog_cache_ets_test).

-include_lib("eunit/include/eunit.hrl").

cache_test_() ->
    [
        fun init_returns_handle/0,
        fun get_after_put_returns_value/0,
        fun get_missing_returns_not_found/0,
        fun put_overwrites_existing_entry/0,
        fun delete_removes_entry/0,
        fun delete_missing_is_ok/0,
        fun invalidate_all_clears_table/0,
        fun info_reports_size/0,
        fun max_entries_evicts_overflow/0,
        fun max_entries_does_not_evict_reserved_row/0,
        fun get_on_reserved_key_returns_not_found/0,
        fun put_on_reserved_key_is_noop/0,
        fun close_deletes_table/0
    ].

%% =============================================================================
%% Tests
%% =============================================================================

init_returns_handle() ->
    {ok, H} = bondy_oplog_cache_ets:init(ns, primary, 0, #{}),
    ?assert(is_reference(H) orelse is_atom(H) orelse is_integer(H)),
    ok = bondy_oplog_cache_ets:invalidate_all(H).

get_after_put_returns_value() ->
    {ok, H} = bondy_oplog_cache_ets:init(ns, primary, 0, #{}),
    ok = bondy_oplog_cache_ets:put(H, <<"k">>, {<<"v">>, 42}),
    ?assertEqual({ok, {<<"v">>, 42}}, bondy_oplog_cache_ets:get(H, <<"k">>)),
    ok = bondy_oplog_cache_ets:invalidate_all(H).

get_missing_returns_not_found() ->
    {ok, H} = bondy_oplog_cache_ets:init(ns, primary, 0, #{}),
    ?assertEqual(not_found, bondy_oplog_cache_ets:get(H, <<"absent">>)),
    ok = bondy_oplog_cache_ets:invalidate_all(H).

put_overwrites_existing_entry() ->
    {ok, H} = bondy_oplog_cache_ets:init(ns, primary, 0, #{}),
    ok = bondy_oplog_cache_ets:put(H, <<"k">>, {<<"v1">>, 1}),
    ok = bondy_oplog_cache_ets:put(H, <<"k">>, {<<"v2">>, 2}),
    ?assertEqual({ok, {<<"v2">>, 2}}, bondy_oplog_cache_ets:get(H, <<"k">>)),
    ok = bondy_oplog_cache_ets:invalidate_all(H).

delete_removes_entry() ->
    {ok, H} = bondy_oplog_cache_ets:init(ns, primary, 0, #{}),
    ok = bondy_oplog_cache_ets:put(H, <<"k">>, {<<"v">>, 1}),
    ok = bondy_oplog_cache_ets:delete(H, <<"k">>),
    ?assertEqual(not_found, bondy_oplog_cache_ets:get(H, <<"k">>)),
    ok = bondy_oplog_cache_ets:invalidate_all(H).

delete_missing_is_ok() ->
    {ok, H} = bondy_oplog_cache_ets:init(ns, primary, 0, #{}),
    ?assertEqual(ok, bondy_oplog_cache_ets:delete(H, <<"never">>)),
    ok = bondy_oplog_cache_ets:invalidate_all(H).

invalidate_all_clears_table() ->
    {ok, H} = bondy_oplog_cache_ets:init(ns, primary, 0, #{}),
    [
        bondy_oplog_cache_ets:put(H, K, {V, V})
        || {K, V} <- [{<<"a">>, 1}, {<<"b">>, 2}, {<<"c">>, 3}]
    ],
    ok = bondy_oplog_cache_ets:invalidate_all(H),
    ?assertEqual(not_found, bondy_oplog_cache_ets:get(H, <<"a">>)),
    ?assertEqual(not_found, bondy_oplog_cache_ets:get(H, <<"b">>)),
    ?assertEqual(not_found, bondy_oplog_cache_ets:get(H, <<"c">>)).

info_reports_size() ->
    {ok, H} = bondy_oplog_cache_ets:init(ns, primary, 0, #{}),
    ok = bondy_oplog_cache_ets:put(H, <<"k">>, {<<"v">>, 1}),
    Info = bondy_oplog_cache_ets:info(H),
    ?assertMatch(#{size := _, memory := _, max_entries := _}, Info),
    ?assertEqual(infinity, maps:get(max_entries, Info)),
    ok = bondy_oplog_cache_ets:invalidate_all(H).

max_entries_evicts_overflow() ->
    {ok, H} = bondy_oplog_cache_ets:init(ns, primary, 0, #{max_entries => 3}),
    [
        bondy_oplog_cache_ets:put(H, K, {K, 1})
        || K <- [<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>]
    ],
    Info = bondy_oplog_cache_ets:info(H),
    %% Reserved '$max_entries' row + at most 3 cached entries.
    ?assert(maps:get(size, Info) =< 4),
    ?assertEqual(3, maps:get(max_entries, Info)),
    ok = bondy_oplog_cache_ets:invalidate_all(H).

max_entries_does_not_evict_reserved_row() ->
    {ok, H} = bondy_oplog_cache_ets:init(ns, primary, 0, #{max_entries => 2}),
    [
        bondy_oplog_cache_ets:put(H, K, {K, 1})
        || K <- [<<"a">>, <<"b">>, <<"c">>, <<"d">>]
    ],
    %% max_entries must survive eviction; info must keep reporting the bound.
    Info = bondy_oplog_cache_ets:info(H),
    ?assertEqual(2, maps:get(max_entries, Info)),
    ok = bondy_oplog_cache_ets:invalidate_all(H).

get_on_reserved_key_returns_not_found() ->
    %% The reserved `'$max_entries'` row stashes the eviction bound;
    %% `get/2` must not crash if a consumer happens to use it as a cell
    %% key (the row is a 2-tuple, not the 3-tuple `get/2` expects).
    {ok, H} = bondy_oplog_cache_ets:init(ns, primary, 0, #{max_entries => 2}),
    ?assertEqual(not_found, bondy_oplog_cache_ets:get(H, '$max_entries')),
    ok = bondy_oplog_cache_ets:close(H).

put_on_reserved_key_is_noop() ->
    %% Writing to the reserved key must not overwrite the bound row.
    {ok, H} = bondy_oplog_cache_ets:init(ns, primary, 0, #{max_entries => 2}),
    ok = bondy_oplog_cache_ets:put(H, '$max_entries', {<<"v">>, 1}),
    Info = bondy_oplog_cache_ets:info(H),
    ?assertEqual(2, maps:get(max_entries, Info)),
    ok = bondy_oplog_cache_ets:close(H).

close_deletes_table() ->
    {ok, H} = bondy_oplog_cache_ets:init(ns, primary, 0, #{}),
    ok = bondy_oplog_cache_ets:put(H, <<"k">>, {<<"v">>, 1}),
    ok = bondy_oplog_cache_ets:close(H),
    %% After close, the ETS table is gone; any operation must crash.
    ?assertError(badarg, bondy_oplog_cache_ets:get(H, <<"k">>)).

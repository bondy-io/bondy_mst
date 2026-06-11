#!/usr/bin/env escript
%%! -pa _build/test/lib/bondy_mst/ebin -pa _build/test/lib/telemetry/ebin -pa _build/test/lib/key_value/ebin -pa _build/test/lib/bloomfi/ebin -pa _build/test/lib/memory/ebin -pa _build/test/lib/utils/ebin +S 2
%% MST + ets-store perf probe (task #56).
%% Part 1: crypto hash algo bake-off at MST-representative sizes.
%% Part 2: calc_level — current encode_hex vs direct nibble walk.
%% Part 3: page-put decomposition — t2b vs hash vs ets insert vs free re-copy.
%% Part 4: end-to-end put_batch (oplog-shaped workload) + eprof attribution.
-mode(compile).

main(Args) ->
    _ = application:ensure_all_started(telemetry),
    Parts = case Args of
        [] -> [p1, p2, p3, p4];
        _ -> [list_to_atom(A) || A <- Args]
    end,
    lists:foreach(fun run/1, Parts).

run(p1) -> part1();
run(p2) -> part2();
run(p3) -> part3();
run(p4) -> part4();
run(p4prof) -> part4_eprof().

%% ---------------------------------------------------------------------------
%% Part 1 — hash algo bake-off
%% ---------------------------------------------------------------------------
part1() ->
    io:format("~n=== Part 1: crypto:hash per-call cost (ns/call) ===~n"),
    Sizes = [48, 256, 1024, 2048, 8192, 65536],
    Algos = [sha, sha256, sha512, sha3_256, blake2b, blake2s],
    io:format("~-10s", ["bytes"]),
    [io:format("~12s", [atom_to_list(A)]) || A <- Algos],
    io:format("~12s~n", ["md5(bif)"]),
    lists:foreach(
        fun(Size) ->
            Bin = crypto:strong_rand_bytes(Size),
            io:format("~-10w", [Size]),
            lists:foreach(
                fun(Algo) ->
                    N = iters_for(Size),
                    %% warm
                    _ = crypto:hash(Algo, Bin),
                    T0 = erlang:monotonic_time(nanosecond),
                    hash_loop(Algo, Bin, N),
                    T1 = erlang:monotonic_time(nanosecond),
                    io:format("~12w", [(T1 - T0) div N])
                end,
                Algos
            ),
            N2 = iters_for(Size),
            T2 = erlang:monotonic_time(nanosecond),
            md5_loop(Bin, N2),
            T3 = erlang:monotonic_time(nanosecond),
            io:format("~12w~n", [(T3 - T2) div N2])
        end,
        Sizes
    ).

iters_for(Size) when Size =< 2048 -> 200000;
iters_for(Size) when Size =< 8192 -> 50000;
iters_for(_) -> 5000.

hash_loop(_, _, 0) -> ok;
hash_loop(Algo, Bin, N) ->
    _ = crypto:hash(Algo, Bin),
    hash_loop(Algo, Bin, N - 1).

md5_loop(_, 0) -> ok;
md5_loop(Bin, N) ->
    _ = erlang:md5(Bin),
    md5_loop(Bin, N - 1).

%% ---------------------------------------------------------------------------
%% Part 2 — calc_level: current (t2b + sha256 + encode_hex + count) vs
%% nibble-walk on the raw digest (no hex binary allocation).
%% ---------------------------------------------------------------------------
part2() ->
    io:format("~n=== Part 2: calc_level variants (ns/call) ===~n"),
    Keys = [rep_key(I) || I <- lists:seq(1, 1000)],
    N = 200,
    %% current
    T0 = erlang:monotonic_time(nanosecond),
    lists:foreach(fun(_) -> [calc_level_current(K) || K <- Keys] end, lists:seq(1, N)),
    T1 = erlang:monotonic_time(nanosecond),
    %% nibble walk
    T2 = erlang:monotonic_time(nanosecond),
    lists:foreach(fun(_) -> [calc_level_nibble(K) || K <- Keys] end, lists:seq(1, N)),
    T3 = erlang:monotonic_time(nanosecond),
    %% sanity: identical results
    true = [calc_level_current(K) || K <- Keys] =:= [calc_level_nibble(K) || K <- Keys],
    Calls = N * length(Keys),
    io:format("current (encode_hex):  ~w ns/call~n", [(T1 - T0) div Calls]),
    io:format("nibble-walk:           ~w ns/call~n", [(T3 - T2) div Calls]),
    ok.

calc_level_current(Key) ->
    Hash = binary:encode_hex(bondy_mst_utils:hash(Key, sha256)),
    count_hex_zeroes(Hash, 0).

count_hex_zeroes(<<"0", Rest/binary>>, Acc) -> count_hex_zeroes(Rest, Acc + 1);
count_hex_zeroes(_, Acc) -> Acc.

calc_level_nibble(Key) ->
    count_zero_nibbles(bondy_mst_utils:hash(Key, sha256), 0).

count_zero_nibbles(<<0:4, Rest/bitstring>>, Acc) -> count_zero_nibbles(Rest, Acc + 1);
count_zero_nibbles(_, Acc) -> Acc.

%% ---------------------------------------------------------------------------
%% Part 3 — page-put decomposition on a representative page
%% ---------------------------------------------------------------------------
part3() ->
    io:format("~n=== Part 3: page-put cost decomposition (ns/op) ===~n"),
    lists:foreach(fun part3_for/1, [4, 16, 32]).

part3_for(Fanout) ->
    Entries = [
        {rep_key(I), rep_value(I), crypto:strong_rand_bytes(32)}
     || I <- lists:seq(1, Fanout)
    ],
    Page = bondy_mst_page:new(1, crypto:strong_rand_bytes(32), Entries),
    {bondy_mst_page, Level, Low, List, _} = Page,
    Bin = erlang:term_to_binary({Level, Low, List}, [deterministic, {minor_version, 2}]),
    N = 100000,
    %% t2b
    T0 = erlang:monotonic_time(nanosecond),
    t2b_loop({Level, Low, List}, N),
    T1 = erlang:monotonic_time(nanosecond),
    %% sha256 of the serialised page
    T2 = erlang:monotonic_time(nanosecond),
    hash_loop(sha256, Bin, N),
    T3 = erlang:monotonic_time(nanosecond),
    %% blake2b of the serialised page
    T4 = erlang:monotonic_time(nanosecond),
    hash_loop(blake2b, Bin, N),
    T5 = erlang:monotonic_time(nanosecond),
    %% ets insert (page copy in)
    Tab = ets:new(x, [set, public, {write_concurrency, auto}]),
    Hash = crypto:hash(sha256, Bin),
    T6 = erlang:monotonic_time(nanosecond),
    ets_loop(Tab, Hash, Page, N),
    T7 = erlang:monotonic_time(nanosecond),
    %% free-style re-insert (full page copy to set freed_at)
    Freed = bondy_mst_page:set_freed_at(Page, 1),
    T8 = erlang:monotonic_time(nanosecond),
    ets_loop(Tab, Hash, Freed, N),
    T9 = erlang:monotonic_time(nanosecond),
    %% update_element alternative ({Hash, Page, FreedAt} schema)
    Tab2 = ets:new(y, [set, public, {write_concurrency, auto}]),
    true = ets:insert(Tab2, {Hash, Page, undefined}),
    T10 = erlang:monotonic_time(nanosecond),
    upd_loop(Tab2, Hash, N),
    T11 = erlang:monotonic_time(nanosecond),
    ets:delete(Tab), ets:delete(Tab2),
    io:format(
        "fanout ~2w (page ~5w B): t2b=~w  sha256=~w  blake2b=~w  "
        "ets_insert=~w  free_reinsert=~w  free_upd_elem=~w~n",
        [Fanout, byte_size(Bin),
         (T1 - T0) div N, (T3 - T2) div N, (T5 - T4) div N,
         (T7 - T6) div N, (T9 - T8) div N, (T11 - T10) div N]
    ).

t2b_loop(_, 0) -> ok;
t2b_loop(Term, N) ->
    _ = erlang:term_to_binary(Term, [deterministic, {minor_version, 2}]),
    t2b_loop(Term, N - 1).

ets_loop(_, _, _, 0) -> ok;
ets_loop(Tab, Hash, Page, N) ->
    true = ets:insert(Tab, {Hash, Page}),
    ets_loop(Tab, Hash, Page, N - 1).

upd_loop(_, _, 0) -> ok;
upd_loop(Tab, Hash, N) ->
    true = ets:update_element(Tab, Hash, {3, N}),
    upd_loop(Tab, Hash, N - 1).

%% ---------------------------------------------------------------------------
%% Part 4 — end-to-end put_batch, oplog-shaped (increasing HLC keys)
%% ---------------------------------------------------------------------------
part4() ->
    io:format("~n=== Part 4: end-to-end put_batch (us/event) ===~n"),
    lists:foreach(fun(Algo) -> part4_run(Algo, 300000, 256, false) end,
                  [sha256, sha512]).

part4_eprof() ->
    part4_run(sha256, 300000, 256, true).

part4_run(Algo, Total, BatchSize, Eprof) ->
    T = bondy_mst:new(#{
        store => bondy_mst_ets_store,
        store_opts => #{name => <<"probe">>, persistent => true},
        hash_algorithm => Algo
    }),
    NBatches = Total div BatchSize,
    %% grow, sampling steady-state windows
    {T1, _} = grow(T, 1, NBatches, BatchSize, []),
    %% steady-state measure: 50 more batches
    Base = NBatches * BatchSize,
    case Eprof of
        true ->
            eprof:start(),
            Self = self(),
            eprof:profile([Self]),
            _ = grow(T1, NBatches + 1, NBatches + 20, BatchSize, []),
            eprof:stop_profiling(),
            io:format("~n--- eprof: 20 batches of ~w on a ~w-entry tree (~w) ---~n",
                      [BatchSize, Base, Algo]),
            eprof:analyze(total, [{sort, time}]),
            eprof:stop();
        false ->
            T0 = erlang:monotonic_time(microsecond),
            {T2, _} = grow(T1, NBatches + 1, NBatches + 50, BatchSize, []),
            T1us = erlang:monotonic_time(microsecond) - T0,
            Events = 50 * BatchSize,
            Inner = element(3, bondy_mst:store(T2)),
            Tab = element(3, Inner),
            io:format(
                "~w: steady-state ~.2f us/event at ~w entries "
                "(ets pages=~w mem=~w MB)~n",
                [Algo, T1us / Events, Base,
                 ets:info(Tab, size),
                 ets:info(Tab, memory) * 8 div 1048576]
            ),
            bondy_mst:destroy(T2)
    end.

grow(T, From, To, _BatchSize, Acc) when From > To ->
    {T, Acc};
grow(T0, BatchNo, To, BatchSize, Acc) ->
    Base = (BatchNo - 1) * BatchSize,
    Items = [
        {rep_key(Base + I), rep_value(Base + I)}
     || I <- lists:seq(1, BatchSize)
    ],
    T = bondy_mst:put_batch(T0, Items),
    grow(T, BatchNo + 1, To, BatchSize, Acc).

%% ---------------------------------------------------------------------------
%% representative oplog shapes
%% ---------------------------------------------------------------------------
rep_key(I) ->
    %% #bondy_oplog_event_key{hlc, origin, seq} as built by the oplog:
    %% HLC ~ nanosecond-scale increasing int, 16-byte origin, dense seq.
    {bondy_oplog_event_key, 1749600000000000000 + I * 1000, origin(), I}.

rep_value(I) ->
    %% value_from_event/1 shape: {Op, Meta, PrevHash, Signature}
    {{set, 1749600000000000000 + I * 1000,
      <<"v", (integer_to_binary(I))/binary>>},
     undefined, undefined, undefined}.

origin() ->
    case get(origin) of
        undefined ->
            O = binary:copy(<<16#ab>>, 16),
            put(origin, O),
            O;
        O ->
            O
    end.

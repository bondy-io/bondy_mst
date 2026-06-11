%% Compiled driver for the put_batch steady-state measurement (avoids
%% erl_eval interpreter overhead polluting timings/eprof). Compile with
%% compile:file/2 and call run/0 or eprof/0.
-module(mst_probe_driver).

-export([run/0]).
-export([eprof/0]).

-define(TOTAL, 300000).
-define(BATCH, 256).

run() ->
    T1 = grown_tree(),
    NB = ?TOTAL div ?BATCH,
    T0us = erlang:monotonic_time(microsecond),
    T2 = grow(T1, NB + 1, NB + 50, ?BATCH),
    Dus = erlang:monotonic_time(microsecond) - T0us,
    io:format(
        "bulk put_batch steady-state: ~.2f us/event at ~w entries~n",
        [Dus / (50 * ?BATCH), NB * ?BATCH]
    ),
    bondy_mst:destroy(T2),
    ok.

eprof() ->
    T1 = grown_tree(),
    NB = ?TOTAL div ?BATCH,
    eprof:start(),
    eprof:profile([self()]),
    _ = grow(T1, NB + 1, NB + 20, ?BATCH),
    eprof:stop_profiling(),
    eprof:analyze(total, [{sort, time}]),
    ok.

grown_tree() ->
    T = bondy_mst:new(#{
        store => bondy_mst_ets_store,
        store_opts => #{name => <<"probe">>, persistent => true},
        hash_algorithm => sha256
    }),
    grow(T, 1, ?TOTAL div ?BATCH, ?BATCH).

grow(T0, B, To, _BS) when B > To ->
    T0;
grow(T0, B, To, BS) ->
    Base = (B - 1) * BS,
    Items = [{rep_key(Base + I), rep_val(Base + I)} || I <- lists:seq(1, BS)],
    grow(bondy_mst:put_batch(T0, Items), B + 1, To, BS).

rep_key(I) ->
    {bondy_oplog_event_key, 1749600000000000000 + I * 1000, origin(), I}.

rep_val(I) ->
    {{set, 1749600000000000000 + I * 1000,
      <<"v", (integer_to_binary(I))/binary>>},
     undefined, undefined, undefined}.

origin() ->
    binary:copy(<<16#ab>>, 16).

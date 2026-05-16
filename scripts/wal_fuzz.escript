#!/usr/bin/env escript
%% -*- erlang -*-
%%! +sbtu +A0 -hidden -noinput
%%
%% WAL PropEr fuzz runner.
%%
%% Loops every property defined in `bondy_oplog_wal_proper_test` with a
%% fresh random seed per iteration, for a configurable wall-clock
%% budget. The v1 acceptance gate (WAL_DESIGN §16, WAL_IMPLEMENTATION_PLAN
%% Phase 10) requires this to run for ≥ 24h continuous without failure
%% before the WAL is allowed into production. Shorter runs (e.g. 1
%% hour) are useful as a CI smoke gate.
%%
%% Usage:
%%   scripts/wal_fuzz.escript            # 60-second smoke
%%   scripts/wal_fuzz.escript 3600       # 1-hour run
%%   scripts/wal_fuzz.escript 86400      # 24-hour gate
%%
%% Exit code: 0 on clean run, 1 on any PropEr failure (the minimal
%% counterexample is printed to stderr before exit). Failures are
%% surfaced eagerly — the script does not continue past a failed
%% property because a single counterexample is enough to gate
%% production.

main(Args) ->
    Budget = parse_budget(Args),
    ok = setup_paths(),
    ok = ensure_apps([telemetry, meck, proper]),
    Props = property_funs(),
    io:format(
        "wal_fuzz: budget=~p s, properties=~p~n",
        [Budget, length(Props)]
    ),
    Deadline = erlang:monotonic_time(second) + Budget,
    Outcome = loop(Props, Deadline, 0, 0),
    case Outcome of
        {ok, Iters, Trials} ->
            io:format(
                "wal_fuzz: clean — iterations=~p property-trials=~p~n",
                [Iters, Trials]
            ),
            erlang:halt(0);
        {fail, PropName, Counter} ->
            io:format(
                standard_error,
                "wal_fuzz: FAIL ~p~n  counterexample = ~p~n",
                [PropName, Counter]
            ),
            erlang:halt(1)
    end.

%% --- internals ---------------------------------------------------------

parse_budget([]) -> 60;
parse_budget([S | _]) ->
    case string:to_integer(S) of
        {I, ""} when I > 0 -> I;
        _ ->
            io:format(standard_error,
                      "wal_fuzz: invalid budget ~p (want positive int)~n",
                      [S]),
            erlang:halt(2)
    end.

setup_paths() ->
    Root = filename:dirname(filename:dirname(escript:script_name())),
    EbinPattern = filename:join([Root, "_build", "test", "lib", "*", "ebin"]),
    Ebins = filelib:wildcard(EbinPattern),
    case Ebins of
        [] ->
            io:format(
                standard_error,
                "wal_fuzz: no test ebin dirs under ~s — run "
                "`rebar3 as test compile` first~n",
                [EbinPattern]
            ),
            erlang:halt(3);
        _ ->
            code:add_pathsa(Ebins)
    end,
    TestBeam = filename:join(
        [Root, "_build", "test", "lib", "bondy_mst", "test"]
    ),
    code:add_patha(TestBeam),
    ok.

ensure_apps([]) -> ok;
ensure_apps([App | Rest]) ->
    case application:ensure_all_started(App) of
        {ok, _} -> ensure_apps(Rest);
        {error, Reason} ->
            io:format(standard_error,
                      "wal_fuzz: failed to start ~p: ~p~n",
                      [App, Reason]),
            erlang:halt(4)
    end.

%% All exported `prop_*` functions of the WAL PropEr suite. Reflected so
%% the runner picks up new properties automatically without having to
%% edit this script.
property_funs() ->
    Mod = bondy_oplog_wal_proper_test,
    Exports = Mod:module_info(exports),
    [{Mod, F} || {F, 0} <- Exports, lists:prefix("prop_", atom_to_list(F))].

loop(Props, Deadline, Iters, Trials) ->
    case erlang:monotonic_time(second) >= Deadline of
        true ->
            {ok, Iters, Trials};
        false ->
            case run_iteration(Props) of
                {ok, N} ->
                    case Iters rem 10 of
                        0 ->
                            Remaining =
                                Deadline
                                - erlang:monotonic_time(second),
                            io:format(
                                "wal_fuzz: iter=~p trials=~p "
                                "remaining=~p s~n",
                                [Iters + 1, Trials + N, Remaining]
                            );
                        _ -> ok
                    end,
                    loop(Props, Deadline, Iters + 1, Trials + N);
                {fail, _, _} = Fail ->
                    Fail
            end
    end.

run_iteration(Props) ->
    run_iteration(Props, 0).

run_iteration([], Acc) ->
    {ok, Acc};
run_iteration([{Mod, F} | Rest], Acc) ->
    case proper:quickcheck(Mod:F(), [{numtests, 50}, quiet]) of
        true ->
            run_iteration(Rest, Acc + 50);
        false ->
            {fail, F, proper:counterexample()};
        Other ->
            {fail, F, Other}
    end.

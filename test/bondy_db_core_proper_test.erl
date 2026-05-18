%% =============================================================================
%% PropEr invariants for the read-side substrate.
%%
%% Covers the eight checkable invariants in `MST_DB_DESIGN.md` §17:
%%
%%   - prop_read_returns_latest_fold/0       (§17 D1 — read consistency)
%%   - prop_cache_coherence_write_through/0  (§17 D2 — cache coherence)
%%   - prop_fenced_read_excludes_past_fence/0 (§17 D3 — fenced reads)
%%   - prop_range_monotonicity/0             (§17 D4 — range scans)
%%   - prop_ensure_fresh_correctness/0       (§17 D5 — freshness predicate)
%%   - prop_subscription_delivers_matches/0  (§17 D6 — subscription delivery)
%%   - prop_overlay_projection_merge/0       (§17 D7 — overlay-projection merge)
%%   - prop_concurrent_readers_observe_lineage/0 (§17 D8 — reader safety)
%%
%% D9 (adapter agnosticism) is verified by construction: the substrate
%% only consumes the `bondy_oplog_projection_adapter` / `bondy_oplog_cache_adapter`
%% behaviours, never module-specific entry points. The properties below
%% run against the ETS reference pair; a second adapter pair would
%% reuse the same fixture by swapping `?CACHE_MOD` / `?PROJ_MOD`.
%% =============================================================================

-module(bondy_db_core_proper_test).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CACHE_MOD, bondy_oplog_cache_ets).
-define(PROJ_MOD,  bondy_oplog_projection_ets).
-define(STRATEGY,  lww_register).
-define(NUMTESTS,  50).

-export([prop_read_returns_latest_fold/0]).
-export([prop_cache_coherence_write_through/0]).
-export([prop_fenced_read_excludes_past_fence/0]).
-export([prop_range_monotonicity/0]).
-export([prop_ensure_fresh_correctness/0]).
-export([prop_subscription_delivers_matches/0]).
-export([prop_overlay_projection_merge/0]).
-export([prop_concurrent_readers_observe_lineage/0]).

%% =============================================================================
%% Generators
%% =============================================================================

key_gen() ->
    elements([<<"a">>, <<"b">>, <<"c">>, <<"d">>]).

%% Generate a list of events with strictly ascending, unique HLCs.
%% Each event is `{set, V, H}` or `{clear, H}` keyed implicitly by the
%% caller-chosen Key. HLCs come from the index + 1 to avoid 0.
events_gen() ->
    ?LET(N, integer(0, 8),
         ?LET(Ops, vector(N, op_kind_gen()),
              hlc_index(Ops))).

op_kind_gen() ->
    oneof([set, clear]).

hlc_index(Ops) ->
    hlc_index(Ops, 1, []).

hlc_index([], _Idx, Acc) ->
    lists:reverse(Acc);
hlc_index([set | Rest], Idx, Acc) ->
    hlc_index(Rest, Idx + 1, [{set, Idx, hlc_value(Idx)} | Acc]);
hlc_index([clear | Rest], Idx, Acc) ->
    hlc_index(Rest, Idx + 1, [{clear, Idx} | Acc]).

hlc_value(Idx) ->
    %% Distinct deterministic 4-byte payload tied to the HLC, so
    %% expected values are easy to reconstruct.
    <<Idx:32/big>>.

range_bounds_gen() ->
    %% Always produces `Low =< High` with both drawn from the live
    %% key universe. `<<"z">>` (outside the universe) acts as a
    %% guaranteed upper bound.
    ?LET({L, H},
         {elements([<<"a">>, <<"b">>, <<"c">>]),
          elements([<<"b">>, <<"c">>, <<"d">>, <<"z">>])},
         case L =< H of
             true  -> {L, H};
             false -> {H, L}
         end).

%% =============================================================================
%% Properties
%% =============================================================================

%% D1 — Read consistency. After folding a sequence of events (any
%% interleaving of `set`/`clear`) in HLC order, `read/3` must return
%% the same `{Value, Hlc}` the model computes.
prop_read_returns_latest_fold() ->
    ?FORALL({Key, Events}, {key_gen(), events_gen()},
        with_shard(fun(NS) ->
            populate_overlay(NS, Key, Events),
            Got = bondy_db_core:read(NS, primary, Key),
            Expected = expected_read(Events),
            equal_read_result(Got, Expected)
        end)).

%% D7 — Overlay-projection merge correctness. A pre-materialised
%% projection cell at HLC=H_proj with a pending overlay event E at
%% HLC > H_proj must read back as `fold(S, [E])`.
prop_overlay_projection_merge() ->
    ?FORALL({Key, Events, OverlayOp}, {key_gen(), events_gen(), op_kind_gen()},
        ?IMPLIES(Events =/= [],
            with_shard(fun(NS) ->
                ProjValue = fold_events(initial(), Events),
                materialise(NS, Key, ProjValue),
                ProjHlc = hlc_of(ProjValue),
                OverlayHlc = ProjHlc + 1,
                OverlayEvent = case OverlayOp of
                    set   -> {set, OverlayHlc, hlc_value(OverlayHlc)};
                    clear -> {clear, OverlayHlc}
                end,
                insert_overlay(NS, Key, OverlayEvent),
                Got = bondy_db_core:read(NS, primary, Key),
                Expected = expected_read(Events ++ [OverlayEvent]),
                equal_read_result(Got, Expected)
            end))).

%% D3 — Fenced read consistency. For a fence T, overlay events with
%% HLC > T must NOT contribute to the result.
prop_fenced_read_excludes_past_fence() ->
    ?FORALL({Key, Events}, {key_gen(), events_gen()},
        ?IMPLIES(Events =/= [],
            with_shard(fun(NS) ->
                populate_overlay(NS, Key, Events),
                MaxH = max_hlc(Events),
                Fence = MaxH div 2,
                {ok, Map, _F} = bondy_db_core:read_batch(
                    [{NS, primary, Key}], #{fence => Fence}
                ),
                Got = maps:get({NS, primary, Key}, Map),
                Filtered = [E || E <- Events, hlc_of_event(E) =< Fence],
                Expected = expected_read(Filtered),
                equal_read_result(Got, Expected)
            end))).

%% D4 — Range monotonicity. Range scans return keys in sorted order
%% and cover exactly the cells in `[Low, High)`.
prop_range_monotonicity() ->
    ?FORALL({Keys, {Low, High}},
            {list(elements([<<"a">>, <<"b">>, <<"c">>, <<"d">>])),
             range_bounds_gen()},
        with_shard(fun(NS) ->
            UniqueKeys = lists:usort(Keys),
            lists:foreach(
                fun({K, Idx}) ->
                    materialise(NS, K, {set, hlc_value(Idx), Idx})
                end,
                lists:zip(UniqueKeys, lists:seq(1, length(UniqueKeys)))
            ),
            {ok, Rows} = bondy_db_core:range(NS, primary,
                                            {Low, High}, #{}),
            ResultKeys = [K || {K, _, _} <- Rows],
            Sorted = ResultKeys =:= lists:sort(ResultKeys),
            Expected = [K || K <- UniqueKeys, K >= Low, K < High],
            Sorted andalso ResultKeys =:= Expected
        end)).

%% D5 — Freshness predicate correctness. A registered NS is fresh iff
%% every shard has lag ≤ MaxLag.
prop_ensure_fresh_correctness() ->
    ?FORALL({BumpDeltas, MaxLag},
            {non_empty(list(integer(0, 50))), integer(1, 100)},
        with_shard_count(length(BumpDeltas), fun(NS, Shards) ->
            %% For each shard, sleep nothing but pretend the bump
            %% happened `Delta` ms ago by writing the explicit
            %% timestamp into the atomics. We use the public bump_ae
            %% then immediately compute the model-expected predicate
            %% based on `Delta`.
            Now = erlang:monotonic_time(millisecond),
            lists:foreach(
                fun({S, Delta}) ->
                    set_ae_at(NS, primary, S, Now - Delta)
                end,
                lists:zip(Shards, BumpDeltas)
            ),
            Got = bondy_db_core:ensure_fresh([NS], MaxLag),
            AnyStale = lists:any(fun(D) -> D > MaxLag end, BumpDeltas),
            case Got of
                ok when not AnyStale -> true;
                {stale, [NS]} when AnyStale -> true;
                _ -> false
            end
        end)).

%% D6 — Subscription delivery. Every published event matching the
%% subscriber's pattern is delivered, in publish order.
prop_subscription_delivers_matches() ->
    ?FORALL({NSKey, Keys},
            {atom_ns(), non_empty(list(key_gen()))},
        begin
            {ok, _} = application:ensure_all_started(bondy_mst),
            {ok, Ref} = bondy_db_core:subscribe(NSKey, {prefix, <<"a">>}),
            lists:foreach(
                fun({K, I}) ->
                    bondy_db_core:publish(NSKey, K, I, op)
                end,
                lists:zip(Keys, lists:seq(1, length(Keys)))
            ),
            Got = drain_messages(NSKey, 50),
            ok = bondy_db_core:unsubscribe(Ref),
            Expected = [{bondy_db_core_event, NSKey, K, I, op}
                        || {K, I} <- lists:zip(Keys,
                                               lists:seq(1, length(Keys))),
                           binary:longest_common_prefix([K, <<"a">>]) =:= 1],
            Got =:= Expected
        end).

%% D2 — Cache coherence. After `write_through/4` against a cached
%% cell, the cached value equals what an uncached slow read would
%% return (we simulate slow read by invalidating + re-reading).
prop_cache_coherence_write_through() ->
    ?FORALL({Key, Events, ExtraOp},
            {key_gen(), events_gen(), op_kind_gen()},
        ?IMPLIES(Events =/= [],
            with_shard(fun(NS) ->
                %% Materialise projection from the event stream so the
                %% cache will populate on first read.
                ProjValue = fold_events(initial(), Events),
                materialise(NS, Key, ProjValue),
                _ = bondy_db_core:read(NS, primary, Key),
                %% Write through a new event onto the cached cell.
                NewHlc = max_hlc(Events) + 10,
                Event = case ExtraOp of
                    set   -> mk_event(NewHlc, {set, NewHlc,
                                              hlc_value(NewHlc)});
                    clear -> mk_event(NewHlc, {clear, NewHlc})
                end,
                ok = bondy_db_core:write_through(NS, primary, Key, Event),
                Cached = bondy_db_core:read(NS, primary, Key),
                %% Slow read = drop cache and re-read against
                %% projection + (empty) overlay.
                {ok, Entry} =
                    bondy_db_core_registry:lookup(NS, primary, 0),
                CH = bondy_db_core_registry:entry_cache_handle(Entry),
                ok = ?CACHE_MOD:invalidate_all(CH),
                %% After invalidation the only state is the projection
                %% — the event was never applied projection-side, only
                %% to the cache via write_through. So slow read returns
                %% the projection's value.
                Slow = bondy_db_core:read(NS, primary, Key),
                %% Cache coherence: the value the writer saw must be
                %% equal to fold(ProjValue, [extra_event]).
                Expected = expected_read(Events ++ [event_to_op(Event)]),
                equal_read_result(Cached, Expected)
                  andalso equal_read_result(Slow,
                                            expected_read(Events))
            end))).

%% D8 — Concurrent reader safety. With N readers and 1 writer, every
%% observation must correspond to a valid prefix-fold of the event
%% log (i.e. there must exist some i s.t. observed value equals
%% fold(initial, events[1..i])).
prop_concurrent_readers_observe_lineage() ->
    ?FORALL({Key, Events}, {key_gen(), events_gen()},
        ?IMPLIES(Events =/= [],
            with_shard(fun(NS) ->
                Parent = self(),
                Readers = [
                    spawn_link(fun() ->
                        Snapshots = read_loop(NS, Key, 20, []),
                        Parent ! {snap, self(), Snapshots}
                    end)
                  || _ <- lists:seq(1, 3)
                ],
                %% Writer interleaves with readers.
                populate_overlay(NS, Key, Events),
                %% Collect.
                AllSnaps = lists:flatten(
                    [collect_snaps(P) || P <- Readers]),
                %% Build the valid lineage map: HLC → expected Value.
                Lineage = build_lineage(Events),
                lists:all(
                    fun({V, H}) -> in_lineage({V, H}, Lineage) end,
                    AllSnaps
                )
            end))).

%% =============================================================================
%% Setup helpers
%% =============================================================================

with_shard(Fn) ->
    with_shard_count(1, fun(NS, _Shards) -> Fn(NS) end).

with_shard_count(N, Fn) ->
    {ok, _} = application:ensure_all_started(bondy_mst),
    NS = mk_ns(),
    Shards = lists:seq(0, N - 1),
    Handles = [start_shard(NS, primary, S, N) || S <- Shards],
    try
        Fn(NS, Shards)
    after
        [stop_shard(H) || H <- Handles]
    end.

start_shard(NS, Index, Shard, ShardCount) ->
    {ok, CH} = ?CACHE_MOD:init(NS, Index, Shard, #{}),
    {ok, PH} = ?PROJ_MOD:open(NS, Index, Shard, #{}),
    OV = bondy_oplog_db_overlay:new(),
    ok = bondy_db_core_registry:register(NS, Index, Shard, #{
        shard_count => ShardCount,
        cache_adapter => ?CACHE_MOD,
        cache_handle => CH,
        projection_adapter => ?PROJ_MOD,
        projection_handle => PH,
        overlay => OV,
        fold_module => ?STRATEGY
    }),
    #{ns => NS, index => Index, shard => Shard,
      cache_handle => CH, projection => PH, overlay => OV}.

stop_shard(#{ns := NS, index := Index, shard := Shard,
             cache_handle := CH, projection := PH, overlay := OV}) ->
    ok = bondy_db_core_registry:unregister(NS, Index, Shard),
    ok = ?CACHE_MOD:invalidate_all(CH),
    ok = ?PROJ_MOD:close(PH),
    ok = bondy_oplog_db_overlay:delete(OV).

mk_ns() ->
    list_to_atom("mst_db_proper_" ++
                 integer_to_list(erlang:unique_integer([positive, monotonic]))).

atom_ns() ->
    ?LET(_, integer(),
         list_to_atom("mst_db_proper_pub_" ++
                      integer_to_list(erlang:unique_integer([positive,
                                                              monotonic])))).

%% Set a shard's AE counter to a specific (Now - Delta) monotonic value.
set_ae_at(NS, Index, Shard, AtTs) ->
    {ok, Entry} = bondy_db_core_registry:lookup(NS, Index, Shard),
    Ae = bondy_db_core_registry:entry_ae_atomics(Entry),
    atomics:put(Ae, 1, AtTs).

populate_overlay(NS, Key, Events) ->
    {ok, Entry} = bondy_db_core_registry:lookup(NS, primary, 0),
    OV = bondy_db_core_registry:entry_overlay(Entry),
    lists:foreach(
        fun(E) ->
            Hlc = hlc_of_event(E),
            Event = mk_event(Hlc, E),
            ok = bondy_oplog_db_overlay:insert(OV, Key, Event)
        end,
        Events
    ).

insert_overlay(NS, Key, E) ->
    {ok, Entry} = bondy_db_core_registry:lookup(NS, primary, 0),
    OV = bondy_db_core_registry:entry_overlay(Entry),
    Hlc = hlc_of_event(E),
    Event = mk_event(Hlc, E),
    ok = bondy_oplog_db_overlay:insert(OV, Key, Event).

materialise(NS, Key, {set, _, _} = State) ->
    {ok, Entry} = bondy_db_core_registry:lookup(NS, primary, 0),
    PH = bondy_db_core_registry:entry_projection_handle(Entry),
    Frame = bondy_oplog_cell_frame:encode(
        hlc_of(State),
        bondy_oplog_fold:encode_state(?STRATEGY, State)
    ),
    ok = ?PROJ_MOD:put_batch(PH, [{Key, Frame}]);
materialise(NS, Key, {cleared, _} = State) ->
    {ok, Entry} = bondy_db_core_registry:lookup(NS, primary, 0),
    PH = bondy_db_core_registry:entry_projection_handle(Entry),
    Frame = bondy_oplog_cell_frame:encode(
        hlc_of(State),
        bondy_oplog_fold:encode_state(?STRATEGY, State)
    ),
    ok = ?PROJ_MOD:put_batch(PH, [{Key, Frame}]);
materialise(_NS, _Key, undefined) ->
    ok.

mk_event(Hlc, Op) ->
    Key = bondy_oplog_event:key(Hlc, <<"o">>, Hlc),
    bondy_oplog_event:new(Key, Op, undefined).

event_to_op(EventRecord) ->
    bondy_oplog_event:op(EventRecord).

%% =============================================================================
%% Model helpers
%% =============================================================================

initial() ->
    bondy_oplog_fold:initial_value(?STRATEGY).

fold_events(State, Events) ->
    lists:foldl(
        fun(E, Acc) -> bondy_oplog_fold:apply_event(?STRATEGY, Acc, E) end,
        State,
        Events
    ).

hlc_of({set, _Val, H})  -> H;
hlc_of({cleared, H})    -> H;
hlc_of(undefined)       -> 0.

hlc_of_event({set, H, _})  -> H;
hlc_of_event({clear, H})   -> H.

max_hlc(Events) ->
    lists:max([hlc_of_event(E) || E <- Events]).

expected_read(Events) ->
    Sorted = lists:sort(fun(A, B) -> hlc_of_event(A) =< hlc_of_event(B) end,
                        Events),
    case fold_events(initial(), Sorted) of
        undefined -> undefined;
        V         -> {V, hlc_of(V)}
    end.

equal_read_result(Got, Expected) ->
    case {Got, Expected} of
        {undefined, undefined} -> true;
        {{V1, H1}, {V2, H2}}   -> V1 =:= V2 andalso H1 =:= H2;
        _                      -> false
    end.

drain_messages(NS, TimeoutMs) ->
    drain_messages(NS, TimeoutMs, []).

drain_messages(NS, TimeoutMs, Acc) ->
    receive
        {bondy_db_core_event, NS, _, _, _} = M ->
            drain_messages(NS, TimeoutMs, [M | Acc])
    after TimeoutMs ->
        lists:reverse(Acc)
    end.

read_loop(_NS, _Key, 0, Acc) ->
    Acc;
read_loop(NS, Key, N, Acc) ->
    Snap = case bondy_db_core:read(NS, primary, Key) of
        undefined -> {undefined, 0};
        {V, H}    -> {V, H}
    end,
    read_loop(NS, Key, N - 1, [Snap | Acc]).

collect_snaps(P) ->
    receive
        {snap, P, S} -> S
    after 1000 ->
        []
    end.

build_lineage(Events) ->
    Sorted = lists:sort(fun(A, B) -> hlc_of_event(A) =< hlc_of_event(B) end,
                        Events),
    {Map, _} = lists:foldl(
        fun(E, {Acc, Prev}) ->
            New = bondy_oplog_fold:apply_event(?STRATEGY, Prev, E),
            H = case New of
                undefined -> 0;
                _         -> hlc_of(New)
            end,
            {maps:put(H, New, Acc), New}
        end,
        {#{0 => initial()}, initial()},
        Sorted
    ),
    Map.

in_lineage({undefined, 0}, Lineage) ->
    maps:get(0, Lineage, undefined) =:= initial();
in_lineage({V, H}, Lineage) ->
    case maps:get(H, Lineage, missing) of
        missing -> false;
        Expected -> Expected =:= V
    end.

%% =============================================================================
%% EUnit wrapper
%% =============================================================================

properties_test_() ->
    {timeout, 600,
     fun() ->
        Opts = [{to_file, user}, {numtests, ?NUMTESTS}],
        Props = [
            prop_read_returns_latest_fold(),
            prop_overlay_projection_merge(),
            prop_fenced_read_excludes_past_fence(),
            prop_range_monotonicity(),
            prop_ensure_fresh_correctness(),
            prop_subscription_delivers_matches(),
            prop_cache_coherence_write_through(),
            prop_concurrent_readers_observe_lineage()
        ],
        lists:foreach(
            fun(Prop) -> ?assert(proper:quickcheck(Prop, Opts)) end,
            Props
        )
     end}.

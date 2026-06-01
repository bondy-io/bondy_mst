%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_fold_aw_map).
-behaviour(bondy_oplog_fold).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Add-Wins Map (AW-Map) fold — dynamic-key map CRDT with per-key
sub-CRDT values.

Each key carries its own sub-fold strategy (`lww_register`,
`pn_counter`, `orset`, etc.) and the AW-Map enforces add-wins
resolution at the **key level**: a concurrent `put`-vs-`remove` of
the same key keeps the put when the remove didn't observe its dot.

Distinct from `map_of_fields`, whose key set is **static** (schema-
bound atoms). AW-Map keys are runtime binaries — the right primitive
for user-defined attributes / tags / metadata / capability sets
anywhere the key vocabulary is data, not schema.

## State

```erlang
#{
    entries := #{Key :: binary() => entry()},
    hlc     := hlc()
}

entry() ::
    {SubFold :: atom(), SubState :: term(),
     AddDots :: ordsets:ordset(dot()),
     Tombs   :: ordsets:ordset(dot())}
  | {tombstoned, SubFold :: atom(),
                 Tombs   :: ordsets:ordset(dot())}.

dot() :: {Origin :: binary(), Seq :: non_neg_integer()}.
```

Each live entry tracks the sub-fold, the sub-state, the set of
**AddDots** (substrate-derived `{key_origin, key_seq}` identifiers
that contributed to this key being alive), and the per-key
**Tombs** set (dots observed-removed for this key). When AddDots
empties, the entry transitions to a `{tombstoned, SubFold, Tombs}`
marker — the SubFold is preserved so a later `put`/`apply`
re-instantiation against the same key validates against the
original sub-fold.

## Events

Physical events (on the WAL):

```erlang
{put,    K :: binary(), SubFold :: atom(), SubInitState :: term()}
| {apply,  K :: binary(), SubFold :: atom(), SubEvent :: term()}
| {remove, K :: binary(), ObservedDots :: [dot()]}.
```

Logical event (resolved by `resolve_event/2` before WAL append):

```erlang
{remove_aw_key, K :: binary()}.
```

`put` is "introduce K with this initial sub-state". `apply` is
"absorb this sub-event into K's sub-state, reviving K if absent /
tombstoned". `remove` is the AW-correct observed-remove — tombstones
the listed dots and scrubs them from K's AddDots.

The logical `{remove_aw_key, K}` shape exists so callers don't have
to track per-key dots; the substrate reads current state inside the
cell's single-applier scope and constructs the resolved
`{remove, K, ObservedDots}` event for WAL append.

## Dot source

Dots are substrate-derived from the event key meta:
`{bondy_oplog_event:key_origin(Meta), bondy_oplog_event:key_seq(Meta)}`.
This follows `_design/0_architecture.md` §6 Tier 1 ("the event key
IS the dot") — application code supplies no dot on put/apply.
Globally unique by substrate's per-Origin Seq invariant; no
shim-side dot-counter recovery gaps are possible.

## Sub-fold support

Any sub-fold exporting `merge_states/2`. Excludes
`bondy_oplog_fold_presence_basic` (no merge). The
`map_of_fields` purge-event restriction does **not** apply because
AW-Map's remove operates at the key level (tombstoning dots), not
by dispatching a sub-fold purge.

A sub-fold mismatch — between two events targeting the same key, or
between an event and the entry's stored marker — crashes loudly
with `{strategy_mismatch, K, Stored, New}`.

## Sub-fold event vs state shapes — read the sub-fold spec

The `SubInitState` in `{put, K, SubFold, SubInitState}` is the
sub-fold's **state** shape (what `SubFold:initial_value/0` returns).
The `SubEvent` in `{apply, K, SubFold, SubEvent}` is the sub-fold's
**event** shape — what `SubFold:apply_event/3` accepts as its
middle argument. The two differ for every non-trivial sub-fold, and
passing one where the other is expected crashes inside the
sub-fold's `apply_event/3` clause with `function_clause`. Always
check the sub-fold's `-type state()` / `-type event()` declarations
before building an `apply` event by hand.

The most common foot-gun is `bondy_oplog_fold_lww_register`, where
the two shapes have **different field orders** AND **different
atoms**:

| Aspect | State | Event |
|---|---|---|
| Set form | `{set, V, H}` — value first, HLC second | `{set, H, V}` — HLC first, value second |
| Clear form | `{cleared, H}` (past participle) | `{clear, H}` (imperative) |

A caller who copy-pastes the state shape into an `apply` event
gets `function_clause` inside
`bondy_oplog_fold_lww_register:apply_event/3` at runtime, not a
compile error. Prefer the `bondy_db:aw_apply/5` ergonomics wrapper,
which constructs the event from caller-friendly arguments and
avoids the trap.

## Idempotency

- `put` / `apply` are idempotent: replays add already-present dots
  to AddDots (ordset no-op) and re-apply the same sub-event under
  the sub-fold's own idempotency contract.
- `remove` is idempotent: ordset union of tombstones, ordset
  subtract from AddDots.
- HLC is monotone via `erlang:max/2`.

## Merge

Per-key:
1. Union AddDots from both sides.
2. Union Tombs from both sides.
3. Scrub Tombs from AddDots.
4. Merge sub-states via `sub_fold:merge_states/2` (crash on
   strategy mismatch).
5. If scrubbed AddDots is empty, transition to
   `{tombstoned, SubFold, Tombs}`.

Commutative, associative, idempotent — by inheriting these
properties from the sub-fold's `merge_states/2` and ordset
operations.

## GC

`gc_threshold(State) == max(hlc, max over live SubFold:gc_threshold(SubState))`
once any entry exists; `undefined` for the literal initial value.
Tombstoned entries don't contribute a per-entry threshold — they're
kept sticky via the parent's HLC.

## Value projection

`to_value/1` returns `#{Key => SubFold:to_value(SubState)}` over
**live** entries only. Tombstoned entries are omitted from the
projected value.

Deltas (Contract C):
- `{set_elem, K, V}` — K's value is now V (whether newly added or
  updated).
- `{remove_elem, K}` — K is no longer live.

## Encoding

```
state -> <<HLC:64, NumEntries:32, <Entry>+>>

Entry = <<KSize:32, K/binary, EntryKind:8, ...>>
  EntryKind 1 (live):
    <<SubFoldTag:8,
      NumDots:32,   <Dot>+,
      NumTombs:32,  <Dot>+,
      SubStateSize:32, SubState/binary>>
  EntryKind 2 (tombstoned):
    <<SubFoldTag:8,
      NumTombs:32, <Dot>+>>

Dot = <<OriginSize:16, Origin/binary, Seq:64/big-unsigned>>
```

Entries sorted by `K`; per-entry Dots and Tombs in ordset order.
Canonical: byte-identical encoding for `=:=` states. Sub-fold tags
come from the central registry at `bondy_oplog_fold:tag_of/1`.
""").

-export([initial_value/0]).
-export([apply_event/3]).
-export([to_value/1]).
-export([apply_value_delta/2]).
-export([merge_states/2]).
-export([resolve_event/2]).
-export([hlc/1]).
-export([gc_threshold/1]).
-export([encode_state/1]).
-export([decode_state/1]).
-export([encode_event/1]).
-export([decode_event/1]).

-type key_v() :: binary().
-type sub_fold() :: atom().
-type dot() :: {Origin :: binary(), Seq :: non_neg_integer()}.
-type dot_set() :: ordsets:ordset(dot()).

-type live_entry() :: {sub_fold(), term(), dot_set(), dot_set()}.
-type tomb_entry() :: {tombstoned, sub_fold(), dot_set()}.
-type entry() :: live_entry() | tomb_entry().

-type state() :: #{
    entries := #{key_v() => entry()},
    hlc := bondy_oplog_hlc:hlc()
}.

-type put_event() :: {put, key_v(), sub_fold(), term()}.
-type apply_event_t() :: {apply, key_v(), sub_fold(), term()}.
-type remove_event() :: {remove, key_v(), [dot()]}.
-type physical_event() :: put_event() | apply_event_t() | remove_event().
-type logical_event() :: {remove_aw_key, key_v()}.
-type event() :: physical_event() | logical_event().

-export_type([state/0, event/0, dot/0]).

%% =============================================================================
%% CALLBACKS
%% =============================================================================

-spec initial_value() -> state().

initial_value() ->
    #{entries => #{}, hlc => 0}.

-spec apply_event(state(), physical_event(), bondy_oplog_fold:meta()) ->
    bondy_oplog_fold:apply_result().

apply_event(
    #{entries := E, hlc := H0} = S,
    {put, K, SubFold, SubInitState},
    Meta
) when
    is_binary(K), is_atom(SubFold), Meta =/= undefined
->
    {Dot, H1} = dot_and_hlc(Meta, H0),
    {NewEntry, Delta} = put_into(
        K,
        SubFold,
        SubInitState,
        Dot,
        maps:get(K, E, not_found)
    ),
    case NewEntry of
        keep ->
            {S#{hlc := H1}, none};
        _ ->
            {S#{entries := E#{K => NewEntry}, hlc := H1}, Delta}
    end;
apply_event(
    #{entries := E, hlc := H0} = S,
    {apply, K, SubFold, SubEvent},
    Meta
) when
    is_binary(K), is_atom(SubFold), Meta =/= undefined
->
    {Dot, H1} = dot_and_hlc(Meta, H0),
    {NewEntry, Delta} = apply_into(
        K,
        SubFold,
        SubEvent,
        Dot,
        Meta,
        maps:get(K, E, not_found)
    ),
    case NewEntry of
        keep ->
            {S#{hlc := H1}, none};
        _ ->
            {S#{entries := E#{K => NewEntry}, hlc := H1}, Delta}
    end;
apply_event(
    #{entries := E, hlc := H0} = S,
    {remove, K, ObservedDots},
    Meta
) when
    is_binary(K), is_list(ObservedDots), Meta =/= undefined
->
    H1 = erlang:max(H0, bondy_oplog_event:key_hlc(Meta)),
    DotsSet = ordsets:from_list(ObservedDots),
    case maps:get(K, E, not_found) of
        not_found ->
            %% Pre-emptive remove with no entry: we'd need a SubFold
            %% to create a tombstone marker. Without it, the only
            %% defensible move is to drop the event.
            {S#{hlc := H1}, none};
        {SubFold, _SubState, AddDots, Tombs} ->
            NewTombs = ordsets:union(Tombs, DotsSet),
            NewAddDots = ordsets:subtract(AddDots, DotsSet),
            case NewAddDots of
                [] ->
                    NewEntry = {tombstoned, SubFold, NewTombs},
                    {
                        S#{entries := E#{K => NewEntry}, hlc := H1},
                        {remove_elem, K}
                    };
                _ ->
                    {SubFold0, SubState0, _, _} = maps:get(K, E),
                    NewEntry = {SubFold0, SubState0, NewAddDots, NewTombs},
                    {S#{entries := E#{K => NewEntry}, hlc := H1}, none}
            end;
        {tombstoned, SubFold, Tombs} ->
            NewTombs = ordsets:union(Tombs, DotsSet),
            {
                S#{
                    entries := E#{K => {tombstoned, SubFold, NewTombs}},
                    hlc := H1
                },
                none
            }
    end.

-spec to_value(state()) -> #{key_v() => term()}.

to_value(#{entries := E}) ->
    maps:fold(
        fun
            (K, {F, SS, _AddDots, _Tombs}, Acc) ->
                Acc#{K => bondy_oplog_fold:to_value(F, SS)};
            (_K, {tombstoned, _F, _T}, Acc) ->
                Acc
        end,
        #{},
        E
    ).

-doc """
Combine an AW-Map value with an `apply_event/3` delta.

- `{set_elem, K, V}` — K's value is now V (newly added or updated).
- `{remove_elem, K}` — K is no longer live.
""".
-spec apply_value_delta(
    map(),
    {set_elem, key_v(), term()}
    | {remove_elem, key_v()}
) -> map().

apply_value_delta(Value, {set_elem, K, V}) ->
    Value#{K => V};
apply_value_delta(Value, {remove_elem, K}) ->
    maps:remove(K, Value).

-spec merge_states(state(), state()) -> state().

merge_states(
    #{entries := Ea, hlc := Ha},
    #{entries := Eb, hlc := Hb}
) ->
    Keys = lists:usort(maps:keys(Ea) ++ maps:keys(Eb)),
    Merged = lists:foldl(
        fun(K, Acc) ->
            Acc#{
                K => merge_entries(
                    K,
                    maps:get(K, Ea, undefined),
                    maps:get(K, Eb, undefined)
                )
            }
        end,
        #{},
        Keys
    ),
    #{entries => Merged, hlc => erlang:max(Ha, Hb)}.

-doc """
Translate the logical `{remove_aw_key, K}` event into a physical
`{remove, K, ObservedDots}` event by reading the current set of
AddDots for K.

Returns `passthrough` for absent / tombstoned / zero-dot keys —
the substrate skips the WAL append in those cases.
""".
-spec resolve_event(state(), logical_event()) ->
    physical_event() | passthrough.

resolve_event(#{entries := E}, {remove_aw_key, K}) when is_binary(K) ->
    case maps:get(K, E, not_found) of
        not_found -> passthrough;
        {_F, _SS, [], _T} -> passthrough;
        {_F, _SS, AddDots, _T} -> {remove, K, AddDots};
        {tombstoned, _F, _T} -> passthrough
    end.

-spec hlc(state()) -> bondy_oplog_hlc:hlc().

hlc(#{hlc := H}) -> H.

-spec gc_threshold(state()) -> bondy_oplog_hlc:hlc() | undefined.

gc_threshold(#{entries := E, hlc := 0}) when map_size(E) == 0 ->
    undefined;
gc_threshold(#{entries := E, hlc := H}) ->
    maps:fold(
        fun
            (_K, {F, SS, _AddDots, _Tombs}, Acc) ->
                case bondy_oplog_fold:gc_threshold(F, SS) of
                    undefined -> Acc;
                    G when is_integer(G) -> erlang:max(Acc, G)
                end;
            (_K, {tombstoned, _F, _T}, Acc) ->
                Acc
        end,
        H,
        E
    ).

-spec encode_state(state()) -> binary().

encode_state(#{entries := E, hlc := H}) ->
    Entries = lists:sort(maps:to_list(E)),
    NumEntries = length(Entries),
    Body = iolist_to_binary([encode_entry(K, V) || {K, V} <- Entries]),
    <<H:64/big-unsigned, NumEntries:32/big-unsigned, Body/binary>>.

-spec decode_state(binary()) -> state().

decode_state(<<H:64/big-unsigned, N:32/big-unsigned, Rest0/binary>>) ->
    {Entries, <<>>} = decode_entries(N, Rest0, []),
    #{entries => maps:from_list(Entries), hlc => H}.

-spec encode_event(physical_event()) -> binary().

encode_event({put, K, SubFold, SubInitState}) when
    is_binary(K), is_atom(SubFold)
->
    Tag = bondy_oplog_fold:tag_of(SubFold),
    SubBin = bondy_oplog_fold:encode_state(SubFold, SubInitState),
    <<1, (byte_size(K)):32/big-unsigned, K/binary, Tag:8,
        (byte_size(SubBin)):32/big-unsigned, SubBin/binary>>;
encode_event({apply, K, SubFold, SubEvent}) when
    is_binary(K), is_atom(SubFold)
->
    Tag = bondy_oplog_fold:tag_of(SubFold),
    SubBin = bondy_oplog_fold:encode_event(SubFold, SubEvent),
    <<2, (byte_size(K)):32/big-unsigned, K/binary, Tag:8,
        (byte_size(SubBin)):32/big-unsigned, SubBin/binary>>;
encode_event({remove, K, ObservedDots}) when
    is_binary(K), is_list(ObservedDots)
->
    Sorted = lists:usort(ObservedDots),
    DotsBin = iolist_to_binary([encode_dot(D) || D <- Sorted]),
    <<3, (byte_size(K)):32/big-unsigned, K/binary,
        (length(Sorted)):32/big-unsigned, DotsBin/binary>>.

-spec decode_event(binary()) -> physical_event().

decode_event(
    <<1, KSize:32/big-unsigned, K:KSize/binary, Tag:8, SubSize:32/big-unsigned,
        Sub:SubSize/binary>>
) ->
    SubFold = bondy_oplog_fold:mod_of_tag(Tag),
    SubInitState = bondy_oplog_fold:decode_state(SubFold, Sub),
    {put, K, SubFold, SubInitState};
decode_event(
    <<2, KSize:32/big-unsigned, K:KSize/binary, Tag:8, SubSize:32/big-unsigned,
        Sub:SubSize/binary>>
) ->
    SubFold = bondy_oplog_fold:mod_of_tag(Tag),
    SubEvent = bondy_oplog_fold:decode_event(SubFold, Sub),
    {apply, K, SubFold, SubEvent};
decode_event(
    <<3, KSize:32/big-unsigned, K:KSize/binary, NumDots:32/big-unsigned,
        Rest/binary>>
) ->
    {Dots, <<>>} = decode_dots(NumDots, Rest, []),
    {remove, K, Dots}.

%% =============================================================================
%% INTERNAL — apply_event helpers
%% =============================================================================

dot_and_hlc(Meta, H0) ->
    Dot = {bondy_oplog_event:key_origin(Meta), bondy_oplog_event:key_seq(Meta)},
    H1 = erlang:max(H0, bondy_oplog_event:key_hlc(Meta)),
    {Dot, H1}.

%% put on an absent key
put_into(K, SubFold, SubInitState, Dot, not_found) ->
    NewEntry = {SubFold, SubInitState, [Dot], []},
    Value = bondy_oplog_fold:to_value(SubFold, SubInitState),
    {NewEntry, {set_elem, K, Value}};
%% put on a live key with matching SubFold — merge into existing
put_into(
    K,
    SubFold,
    SubInitState,
    Dot,
    {SubFold, SubState0, AddDots0, Tombs}
) ->
    SubState1 = bondy_oplog_fold:merge_states(SubFold, SubState0, SubInitState),
    AddDots1 = ordsets:add_element(Dot, AddDots0),
    case ordsets:is_element(Dot, Tombs) of
        true ->
            %% Tombstoned dot can't re-add the same observation, but
            %% the sub-state merge stands — still valuable for
            %% replay convergence.
            keep_or_update(
                K,
                SubFold,
                SubState0,
                SubState1,
                AddDots0,
                Tombs
            );
        false ->
            keep_or_update(
                K,
                SubFold,
                SubState0,
                SubState1,
                AddDots1,
                Tombs
            )
    end;
%% put on a tombstoned key with matching SubFold — revive
put_into(
    K,
    SubFold,
    SubInitState,
    Dot,
    {tombstoned, SubFold, Tombs}
) ->
    case ordsets:is_element(Dot, Tombs) of
        true ->
            {keep, none};
        false ->
            NewEntry = {SubFold, SubInitState, [Dot], Tombs},
            Value = bondy_oplog_fold:to_value(SubFold, SubInitState),
            {NewEntry, {set_elem, K, Value}}
    end;
%% Strategy mismatch
put_into(K, NewSubFold, _, _, {StoredSubFold, _, _, _}) ->
    erlang:error({strategy_mismatch, K, StoredSubFold, NewSubFold});
put_into(K, NewSubFold, _, _, {tombstoned, StoredSubFold, _}) ->
    erlang:error({strategy_mismatch, K, StoredSubFold, NewSubFold}).

%% apply on an absent key — implicit revive via SubFold:initial_value()
apply_into(K, SubFold, SubEvent, Dot, Meta, not_found) ->
    SubInit = bondy_oplog_fold:initial_value(SubFold),
    {SubState1, _} = bondy_oplog_fold:apply_event(
        SubFold, SubInit, SubEvent, Meta
    ),
    NewEntry = {SubFold, SubState1, [Dot], []},
    Value = bondy_oplog_fold:to_value(SubFold, SubState1),
    {NewEntry, {set_elem, K, Value}};
%% apply on a live key with matching SubFold
apply_into(
    K,
    SubFold,
    SubEvent,
    Dot,
    Meta,
    {SubFold, SubState0, AddDots0, Tombs}
) ->
    {SubState1, _SubDelta} =
        bondy_oplog_fold:apply_event(SubFold, SubState0, SubEvent, Meta),
    AddDots1 =
        case ordsets:is_element(Dot, Tombs) of
            %% can't re-add a tombstoned observation
            true -> AddDots0;
            false -> ordsets:add_element(Dot, AddDots0)
        end,
    keep_or_update(K, SubFold, SubState0, SubState1, AddDots1, Tombs);
%% apply on a tombstoned key with matching SubFold — revive
apply_into(
    K,
    SubFold,
    SubEvent,
    Dot,
    Meta,
    {tombstoned, SubFold, Tombs}
) ->
    case ordsets:is_element(Dot, Tombs) of
        true ->
            {keep, none};
        false ->
            SubInit = bondy_oplog_fold:initial_value(SubFold),
            {SubState1, _} =
                bondy_oplog_fold:apply_event(SubFold, SubInit, SubEvent, Meta),
            NewEntry = {SubFold, SubState1, [Dot], Tombs},
            Value = bondy_oplog_fold:to_value(SubFold, SubState1),
            {NewEntry, {set_elem, K, Value}}
    end;
%% Strategy mismatch
apply_into(K, NewSubFold, _, _, _, {StoredSubFold, _, _, _}) ->
    erlang:error({strategy_mismatch, K, StoredSubFold, NewSubFold});
apply_into(K, NewSubFold, _, _, _, {tombstoned, StoredSubFold, _}) ->
    erlang:error({strategy_mismatch, K, StoredSubFold, NewSubFold}).

keep_or_update(K, SubFold, SubState0, SubState1, AddDots, Tombs) ->
    NewEntry = {SubFold, SubState1, AddDots, Tombs},
    OldValue = bondy_oplog_fold:to_value(SubFold, SubState0),
    NewValue = bondy_oplog_fold:to_value(SubFold, SubState1),
    case NewValue =:= OldValue of
        true -> {NewEntry, none};
        false -> {NewEntry, {set_elem, K, NewValue}}
    end.

%% =============================================================================
%% INTERNAL — merge_states helpers
%% =============================================================================

merge_entries(_K, undefined, B) ->
    B;
merge_entries(_K, A, undefined) ->
    A;
merge_entries(_K, {F, SA, DA, TA}, {F, SB, DB, TB}) ->
    SM = bondy_oplog_fold:merge_states(F, SA, SB),
    UnionTombs = ordsets:union(TA, TB),
    UnionDots = ordsets:union(DA, DB),
    LiveDots = ordsets:subtract(UnionDots, UnionTombs),
    case LiveDots of
        [] -> {tombstoned, F, UnionTombs};
        _ -> {F, SM, LiveDots, UnionTombs}
    end;
merge_entries(_K, {F, SA, DA, TA}, {tombstoned, F, TB}) ->
    UnionTombs = ordsets:union(TA, TB),
    LiveDots = ordsets:subtract(DA, UnionTombs),
    case LiveDots of
        [] -> {tombstoned, F, UnionTombs};
        _ -> {F, SA, LiveDots, UnionTombs}
    end;
merge_entries(_K, {tombstoned, F, TA}, {F, SB, DB, TB}) ->
    UnionTombs = ordsets:union(TA, TB),
    LiveDots = ordsets:subtract(DB, UnionTombs),
    case LiveDots of
        [] -> {tombstoned, F, UnionTombs};
        _ -> {F, SB, LiveDots, UnionTombs}
    end;
merge_entries(_K, {tombstoned, F, TA}, {tombstoned, F, TB}) ->
    {tombstoned, F, ordsets:union(TA, TB)};
merge_entries(K, A, B) ->
    erlang:error({strategy_mismatch, K, entry_subfold(A), entry_subfold(B)}).

entry_subfold({F, _, _, _}) -> F;
entry_subfold({tombstoned, F, _}) -> F.

%% =============================================================================
%% INTERNAL — encoding
%% =============================================================================

encode_entry(K, {F, SubState, AddDots, Tombs}) ->
    Tag = bondy_oplog_fold:tag_of(F),
    SubBin = bondy_oplog_fold:encode_state(F, SubState),
    AddBin = iolist_to_binary([encode_dot(D) || D <- AddDots]),
    TombBin = iolist_to_binary([encode_dot(D) || D <- Tombs]),
    <<
        (byte_size(K)):32/big-unsigned,
        K/binary,
        1,
        Tag:8,
        (length(AddDots)):32/big-unsigned,
        AddBin/binary,
        (length(Tombs)):32/big-unsigned,
        TombBin/binary,
        (byte_size(SubBin)):32/big-unsigned,
        SubBin/binary
    >>;
encode_entry(K, {tombstoned, F, Tombs}) ->
    Tag = bondy_oplog_fold:tag_of(F),
    TombBin = iolist_to_binary([encode_dot(D) || D <- Tombs]),
    <<
        (byte_size(K)):32/big-unsigned,
        K/binary,
        2,
        Tag:8,
        (length(Tombs)):32/big-unsigned,
        TombBin/binary
    >>.

encode_dot({Origin, Seq}) when
    is_binary(Origin),
    is_integer(Seq),
    Seq >= 0
->
    <<(byte_size(Origin)):16/big-unsigned, Origin/binary, Seq:64/big-unsigned>>.

decode_entries(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_entries(
    N,
    <<KSize:32/big-unsigned, K:KSize/binary, 1, Tag:8, NumDots:32/big-unsigned,
        RestDots/binary>>,
    Acc
) when
    N > 0
->
    {AddDots, Rest1} = decode_dots(NumDots, RestDots, []),
    <<NumTombs:32/big-unsigned, RestTombs/binary>> = Rest1,
    {Tombs, Rest2} = decode_dots(NumTombs, RestTombs, []),
    <<SubSize:32/big-unsigned, Sub:SubSize/binary, Rest3/binary>> = Rest2,
    SubFold = bondy_oplog_fold:mod_of_tag(Tag),
    SubState = bondy_oplog_fold:decode_state(SubFold, Sub),
    Entry = {SubFold, SubState, AddDots, Tombs},
    decode_entries(N - 1, Rest3, [{K, Entry} | Acc]);
decode_entries(
    N,
    <<KSize:32/big-unsigned, K:KSize/binary, 2, Tag:8, NumTombs:32/big-unsigned,
        RestTombs/binary>>,
    Acc
) when
    N > 0
->
    {Tombs, Rest} = decode_dots(NumTombs, RestTombs, []),
    SubFold = bondy_oplog_fold:mod_of_tag(Tag),
    Entry = {tombstoned, SubFold, Tombs},
    decode_entries(N - 1, Rest, [{K, Entry} | Acc]).

decode_dots(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_dots(
    N,
    <<OSize:16/big-unsigned, O:OSize/binary, Seq:64/big-unsigned, Rest/binary>>,
    Acc
) when
    N > 0
->
    decode_dots(N - 1, Rest, [{O, Seq} | Acc]).

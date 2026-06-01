%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_fold_g_counter).
-behaviour(bondy_oplog_fold).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Grow-only counter (G-Counter) fold.

A monotone integer counter that only accepts non-negative increments.
Concurrent increments across replicas converge without coordination
via per-Origin accumulation; the projected value is the sum across
all observed Origins.

## Why a G-Counter when `pn_counter` exists

`pn_counter` is the general-purpose unbounded integer counter and
accepts both positive and negative deltas. A G-Counter is the
specialisation for "increment-only" semantics: request-rate counters,
byte counters, processed-event high-water marks, anywhere a decrement
is meaningless and admitting one indicates a caller bug.

G-Counter's state is half the size of `pn_counter`'s
(`{Count, MaxSeq}` per Origin vs `{Pos, Neg, MaxSeq}`), and a
negative delta crashes loudly at the `apply_event/3` guard rather
than silently producing a `Pos`/`Neg` imbalance. The applier catches
and logs the crash (`apply_one_cell` already handles fold-side
failures gracefully).

## State

```
#{
    counters := #{Origin :: binary() => {Count :: non_neg_integer(),
                                         MaxSeq :: non_neg_integer()}},
    hlc      := hlc()
}
```

Each Origin contributes a `{Count, MaxSeq}` pair. `MaxSeq` is the
highest per-Origin sequence number absorbed for that Origin; events
with `Seq <= MaxSeq` are treated as duplicates.

## Events

```
{inc, Delta :: non_neg_integer()}
```

`Delta` must be non-negative; the `apply_event/3` guard enforces it.
Origin and Seq come from the WAL event key via `Meta` and are not
embedded in the payload.

## Idempotency and ordering

Idempotency rests on the per-Origin `MaxSeq` dedup. Replay or
duplicate sync delivery is a no-op because the per-Origin Seq
monotonic counter never reuses a value
(see `_design/0_architecture.md` §4.2).

`Count` accumulation correctness relies on the substrate's
**contiguous-prefix invariant** (`_design/catalogue_expansion_plan.md`
§5.2): events from a single Origin arrive in Seq order with no gaps
within a cell. Per-Origin max-merge over `{Count, MaxSeq}` is then
correct because the higher MaxSeq always reflects a strictly larger
prefix of the same Origin's contiguous event stream.

## Merge

Per-Origin element-wise max across `{Count, MaxSeq}`. Naturally
commutative, associative, and idempotent.

## GC

`gc_threshold(State) == hlc(State)` once any event has been absorbed;
`undefined` for the literal initial value.

## Encoding

```
state -> <<HLC:64,
           NumOrigins:32,
           <encoded counter entries, sorted by Origin>>>

counter entry -> <<OriginSize:16, Origin/binary,
                   Count:64, MaxSeq:64>>

event (inc) -> <<1, Delta:64/unsigned>>
```

The encoding is canonical: counter entries are sorted by Origin so
two `=:=` states produce byte-identical encodings.
""").

-export([initial_value/0]).
-export([apply_event/3]).
-export([to_value/1]).
-export([apply_value_delta/2]).
-export([merge_states/2]).
-export([hlc/1]).
-export([gc_threshold/1]).
-export([encode_state/1]).
-export([decode_state/1]).
-export([encode_event/1]).
-export([decode_event/1]).

-type origin() :: binary().
-type counter() :: {non_neg_integer(), non_neg_integer()}.

-type state() :: #{
    counters := #{origin() => counter()},
    hlc := bondy_oplog_hlc:hlc()
}.

-type event() :: {inc, non_neg_integer()}.

-export_type([state/0, event/0]).

%% =============================================================================
%% CALLBACKS
%% =============================================================================

-spec initial_value() -> state().

initial_value() ->
    #{counters => #{}, hlc => 0}.

-spec apply_event(state(), event(), bondy_oplog_fold:meta()) ->
    bondy_oplog_fold:apply_result().

apply_event(#{counters := C0, hlc := H0} = S, {inc, Delta}, Meta) when
    is_integer(Delta), Delta >= 0, Meta =/= undefined
->
    Origin = bondy_oplog_event:key_origin(Meta),
    EventSeq = bondy_oplog_event:key_seq(Meta),
    EventHlc = bondy_oplog_event:key_hlc(Meta),
    {Count, MaxSeq} = maps:get(Origin, C0, {0, 0}),
    H1 = erlang:max(H0, EventHlc),
    case EventSeq > MaxSeq of
        true ->
            C1 = C0#{Origin => {Count + Delta, EventSeq}},
            {S#{counters := C1, hlc := H1}, Delta};
        false ->
            %% Duplicate / replay — absorb HLC bump but no value change.
            {S#{hlc := H1}, none}
    end.

-spec to_value(state()) -> non_neg_integer().

to_value(#{counters := C}) ->
    maps:fold(
        fun(_O, {Count, _S}, Acc) -> Acc + Count end,
        0,
        C
    ).

-spec apply_value_delta(non_neg_integer(), non_neg_integer()) ->
    non_neg_integer().

apply_value_delta(Value, Delta) when
    is_integer(Value), is_integer(Delta), Delta >= 0
->
    Value + Delta.

-spec merge_states(state(), state()) -> state().

merge_states(
    #{counters := Ca, hlc := Ha},
    #{counters := Cb, hlc := Hb}
) ->
    Origins = lists:usort(maps:keys(Ca) ++ maps:keys(Cb)),
    Merged = lists:foldl(
        fun(O, Acc) ->
            {CA, SA} = maps:get(O, Ca, {0, 0}),
            {CB, SB} = maps:get(O, Cb, {0, 0}),
            Acc#{O => {erlang:max(CA, CB), erlang:max(SA, SB)}}
        end,
        #{},
        Origins
    ),
    #{counters => Merged, hlc => erlang:max(Ha, Hb)}.

-spec hlc(state()) -> bondy_oplog_hlc:hlc().

hlc(#{hlc := H}) -> H.

-spec gc_threshold(state()) -> bondy_oplog_hlc:hlc() | undefined.

gc_threshold(#{counters := C, hlc := 0}) when map_size(C) == 0 ->
    undefined;
gc_threshold(#{hlc := H}) ->
    H.

-spec encode_state(state()) -> binary().

encode_state(#{counters := C, hlc := H}) ->
    Entries = lists:sort(maps:to_list(C)),
    NumOrigins = length(Entries),
    EntriesBin = iolist_to_binary([encode_entry(O, T) || {O, T} <- Entries]),
    <<H:64/big-unsigned, NumOrigins:32/big-unsigned, EntriesBin/binary>>.

-spec decode_state(binary()) -> state().

decode_state(<<H:64/big-unsigned, NumOrigins:32/big-unsigned, Rest0/binary>>) ->
    {Entries, <<>>} = decode_entries(NumOrigins, Rest0, []),
    #{counters => maps:from_list(Entries), hlc => H}.

-spec encode_event(event()) -> binary().

encode_event({inc, Delta}) when is_integer(Delta), Delta >= 0 ->
    <<1, Delta:64/big-unsigned>>.

-spec decode_event(binary()) -> event().

decode_event(<<1, Delta:64/big-unsigned>>) ->
    {inc, Delta}.

%% =============================================================================
%% INTERNAL
%% =============================================================================

encode_entry(Origin, {Count, MaxSeq}) when
    is_binary(Origin),
    is_integer(Count),
    Count >= 0,
    is_integer(MaxSeq),
    MaxSeq >= 0
->
    OriginSize = byte_size(Origin),
    <<OriginSize:16/big-unsigned, Origin/binary, Count:64/big-unsigned,
        MaxSeq:64/big-unsigned>>.

decode_entries(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_entries(
    N,
    <<OriginSize:16/big-unsigned, Origin:OriginSize/binary,
        Count:64/big-unsigned, MaxSeq:64/big-unsigned, Rest/binary>>,
    Acc
) when N > 0 ->
    decode_entries(N - 1, Rest, [{Origin, {Count, MaxSeq}} | Acc]).

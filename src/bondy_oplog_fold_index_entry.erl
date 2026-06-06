%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_fold_index_entry).
-behaviour(bondy_oplog_fold).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Fold backing one cell of the secondary-index keyspace.

Each `(Term, PrimaryKey)` composite key (see `bondy_oplog_index_key`) is
a cell whose value is the index entry's denormalised columns (or `<<>>`
for a pointer-only index). The fold is an **LWW register over presence**
keyed by the *primary* HLC carried in the event payload — not the
secondary writer's own WAL HLC, which is why the HLC is embedded in the
event rather than read from `Meta`.

## State

```
{Presence :: live | dead, Columns :: binary(), Hlc :: hlc()}
```

`initial_value/0` is `{dead, <<>>, 0}` — absent. `live` carries the
projected columns; `dead` is a tombstone (a retracted entry whose term
the primary value no longer yields).

## Events

```
{put,    Columns :: binary(), Hlc} | {remove, Hlc}
```

A `put` represents the state `{live, Columns, Hlc}`; a `remove`
represents `{dead, <<>>, Hlc}`. `apply_event/3` is therefore exactly
`merge_states(State, state_of(Event))`, which makes it **commutative,
associative, and idempotent by construction** — the property the design
calls out as mandatory (risk 3: out-of-order cross-shard delivery of a
`put`/`remove` for the same `(Term, PK)` from a local drain vs a peer
replay).

### LWW order and the equal-HLC tie-break

States are ordered by `{Hlc, presence_rank, Columns}` under standard
term order, where `live` ranks above `dead`. Higher HLC always wins;
the rank/columns tie-break only matters at equal HLC and exists solely
to keep the merge a deterministic total order. By construction a single
cell never receives a genuine `put` and `remove` at the same primary
HLC (each primary value-version has a distinct, monotone HLC and emits
at most one event per term), so the tie-break is for robustness, not a
modelled case.

### Deviation from the design's `>=` condition

`MST_DB_DESIGN.md §13` specifies "apply only when `H >= StoredHlc`". A
bare `>=` (or `>`) inequality is *not* commutative for a conflicting
`put`/`remove` at equal HLC — the result depends on arrival order.
Reframing apply as a merge against a total order makes convergence
hold for all inputs while still being HLC-conditional (the `Hlc`
component dominates the order). Same intent, a construction that is
provably order-independent.

## value_equals_state/0 -> true

The substrate omits the value column and treats the state bytes as the
value bytes on HEAD reads; the reader decodes the state and projects via
`to_value/1` (`{live, Cols, _} -> Cols`, `{dead, _, _} -> undefined`,
the latter filtered by the substrate's existing `undefined` handling).
Every `apply_event/3` clause therefore returns a `none` delta.

## Encoding

```
state {P, Cols, H} -> <<Rank:8, H:64, ColsSize:32, Cols/binary>>
event {put,Cols,H} -> <<1, H:64, ColsSize:32, Cols/binary>>
event {remove, H}  -> <<2, H:64>>
```

`Rank` is `1` for `live`, `0` for `dead`.
""").

-export([initial_value/0]).
-export([apply_event/3]).
-export([to_value/1]).
-export([value_equals_state/0]).
-export([merge_states/2]).
-export([hlc/1]).
-export([gc_threshold/1]).
-export([encode_state/1]).
-export([decode_state/1]).
-export([encode_event/1]).
-export([decode_event/1]).

-type presence() :: live | dead.
-type columns() :: binary().
-type state() :: {presence(), columns(), bondy_oplog_hlc:hlc()}.
-type event() ::
    {put, columns(), bondy_oplog_hlc:hlc()}
    | {remove, bondy_oplog_hlc:hlc()}.

-export_type([state/0, event/0]).

%% =============================================================================
%% CALLBACKS
%% =============================================================================

-spec initial_value() -> state().

initial_value() ->
    {dead, <<>>, 0}.

-spec apply_event(state(), event(), bondy_oplog_fold:meta()) ->
    bondy_oplog_fold:apply_result().

apply_event(State, {put, Cols, H}, _Meta) when
    is_binary(Cols), is_integer(H)
->
    %% `value_equals_state/0 -> true`: never a separate delta.
    {merge_states(State, {live, Cols, H}), none};

apply_event(State, {remove, H}, _Meta) when is_integer(H) ->
    {merge_states(State, {dead, <<>>, H}), none}.

-spec to_value(state()) -> columns() | undefined.

to_value({live, Cols, _H}) -> Cols;
to_value({dead, _Cols, _H}) -> undefined.

-spec value_equals_state() -> true.

value_equals_state() -> true.

-spec merge_states(state(), state()) -> state().

merge_states(A, B) ->
    case sort_key(A) >= sort_key(B) of
        true -> A;
        false -> B
    end.

-spec hlc(state()) -> bondy_oplog_hlc:hlc().

hlc({_P, _C, H}) -> H.

-spec gc_threshold(state()) -> bondy_oplog_hlc:hlc() | undefined.

gc_threshold({dead, <<>>, 0}) -> undefined;
gc_threshold({_P, _C, H}) -> H.

-spec encode_state(state()) -> binary().

encode_state({Presence, Cols, H}) when is_binary(Cols), is_integer(H) ->
    ColsSize = byte_size(Cols),
    <<(rank(Presence)):8, H:64/big-unsigned, ColsSize:32/big-unsigned,
        Cols/binary>>.

-spec decode_state(binary()) -> state().

decode_state(
    <<R:8, H:64/big-unsigned, ColsSize:32/big-unsigned, Cols:ColsSize/binary>>
) ->
    {presence(R), Cols, H}.

-spec encode_event(event()) -> binary().

encode_event({put, Cols, H}) when is_binary(Cols), is_integer(H) ->
    ColsSize = byte_size(Cols),
    <<1, H:64/big-unsigned, ColsSize:32/big-unsigned, Cols/binary>>;
encode_event({remove, H}) when is_integer(H) ->
    <<2, H:64/big-unsigned>>.

-spec decode_event(binary()) -> event().

decode_event(
    <<1, H:64/big-unsigned, ColsSize:32/big-unsigned, Cols:ColsSize/binary>>
) ->
    {put, Cols, H};
decode_event(<<2, H:64/big-unsigned>>) ->
    {remove, H}.

%% =============================================================================
%% INTERNAL
%% =============================================================================

%% Total order for the LWW merge: HLC dominates, then live > dead, then
%% the columns bytes. Equal sort keys imply identical states.
sort_key({Presence, Cols, H}) ->
    {H, rank(Presence), Cols}.

rank(live) -> 1;
rank(dead) -> 0.

presence(1) -> live;
presence(0) -> dead.

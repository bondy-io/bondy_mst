%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_fold_map_of_fields).
-behaviour(bondy_oplog_fold).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Composite fold over records with **multiple independent fields**, each
with its own merge semantics.

Each field is folded by its own embedded strategy (`lww_register`,
`strict_register`, `ttl_presence`). Two replicas converging on
the same record see field-level convergence — a concurrent edit to
field `A` and field `B` does not contend.

## State

```
#{field() => {strategy(), sub_state()}}
```

Each map entry tags the embedded sub-fold strategy alongside the
sub-state. There is **no separate cell-level tombstone** entry —
field removal is expressed as a sub-fold "purge" event (see below).
This is essential for CRDT-safe composition: a cell-level tombstone
that destroys sub-state on tie would break `merge_states/2`
associativity when the sub-fold has a non-LWW-by-HLC merge rule
(e.g. strict_register's `{revoked, _}` is a dominant absorber that
promotes its HLC when merged with later writes — a tombstone wiping
the `{revoked, 0}` before that promotion produces a different
result than merging the `{revoked, 0}` into the later write first).

## Strategy embedded in state and events

`FOLD_STRATEGY_DESIGN.md` §4.4 describes the per-field strategy as
"fold opts" passed alongside events. The `bondy_oplog_fold` behaviour
callback `apply_event/3` does not take a config parameter, so the
strategy is instead **carried in every event** (set on first
occurrence of a field; subsequent events must match the field's
existing strategy) and **recorded in the state**. A strategy
mismatch crashes loudly — it indicates a namespace configuration bug.

## Events

```
{field_event, field(), strategy(), inner_event()}
| {remove_field, hlc(), field(), strategy()}
```

`field_event` is a heterogeneous wrapper: the inner event's vocabulary
is the sub-fold's native vocabulary (e.g. `{set, H, V}` for
`lww_register`; `{revoke, H}` for `strict_register`; `{issue, H, E, P}`
for `ttl_presence`). This deviates from the doc's flat
`{set_field, H, F, V}` form, which cannot carry strict_register's
`{revoke, H}` / `{resolve, H, V}` or ttl_presence's expiry+payload.

`remove_field` is **dispatched** to the sub-fold's native purge event:

| Strategy          | Purge event   |
|-------------------|---------------|
| `lww_register`    | `{clear, H}`  |
| `strict_register` | `{revoke, H}` |
| `ttl_presence`    | `{revoke, H}` |

So `{remove_field, H, F, lww_register}` semantically equals
`{field_event, F, lww_register, {clear, H}}`. The strategy tag is
required on `remove_field` because the field may not yet exist —
without it we couldn't know which sub-fold's purge to dispatch.

## Supported sub-strategies

```
lww_register
strict_register
ttl_presence
```

All supported sub-strategies expose a "purge" event that produces a
sub-state at the requested HLC. **`orset` is not supported** because
its only "remove" event observes per-element dots, which the
cell-level `remove_field` cannot enumerate without leaking sub-fold
internals. **`presence_basic` is not supported** because it has no
`merge_states/2` callback.

## Conflict resolution

Per-field: each sub-fold resolves its own conflicts. Field-level
concurrent edits do not contend. Strategy mismatches between two
sides of a merge for the same field crash with
`{strategy_mismatch, Field, S_a, S_b}`.

## Idempotency and HLC monotonicity

Both follow from sub-fold properties: each sub-fold is idempotent and
HLC-monotonic, and `apply_event/3` only ever delegates to sub-fold
callbacks.

`hlc/1` returns the maximum HLC across all field entries. Empty map → 0.

## GC

`gc_threshold(Map)` is the **max** of sub-fold gc thresholds. Empty
map → `undefined`.

## Encoding

```
<<N:32, Entry_1, Entry_2, ...>>

Entry = <<FSize:32, F/binary, StrategyTag:8, SubSize:32, SubState/binary>>
StrategyTag =
  lww_register:    1
  strict_register: 2
  ttl_presence:    4
```

(Tag 3 is reserved for orset to preserve forward-compatible numbering
if a future design accommodates it.)

Fields are sorted by binary order for canonical encoding (necessary
for `decode_state(encode_state(S)) =:= S` and stable hashing).

Events:

```
{field_event, F, S, IE} ->
    <<1, FSize:32, F/binary, StratTag:8, IESize:32, IE/binary>>
{remove_field, H, F, S} ->
    <<2, H:64, FSize:32, F/binary, StratTag:8>>
```
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

-type field()       :: binary().
-type strategy()    :: lww_register
                     | strict_register
                     | ttl_presence.

-type sub_state()   :: any().
-type inner_event() :: any().

-type entry()       :: {strategy(), sub_state()}.

-type state()       :: #{field() => entry()}.

-type event()       :: {field_event, field(), strategy(), inner_event()}
                     | {remove_field, bondy_oplog_hlc:hlc(), field(), strategy()}.

-export_type([state/0, event/0, strategy/0, field/0]).

%% =============================================================================
%% CALLBACKS
%% =============================================================================

-spec initial_value() -> state().

initial_value() ->
    #{}.


-spec apply_event(state(), event(), bondy_oplog_fold:meta()) ->
    bondy_oplog_fold:apply_result().

apply_event(Map, {field_event, F, S, IE}, Meta)
        when is_binary(F), is_atom(S) ->
    ok = assert_supported(S),
    case maps:get(F, Map, undefined) of
        undefined ->
            Sub0 = bondy_oplog_fold:initial_value(S),
            {Sub1, SubDelta} =
                bondy_oplog_fold:apply_event(S, Sub0, IE, Meta),
            NewMap = Map#{F => {S, Sub1}},
            {NewMap, lift_field_delta(F, S, SubDelta)};
        {S, SubState} ->
            {NewSub, SubDelta} =
                bondy_oplog_fold:apply_event(S, SubState, IE, Meta),
            NewMap = Map#{F => {S, NewSub}},
            {NewMap, lift_field_delta(F, S, SubDelta)};
        {OtherS, _} ->
            erlang:error({strategy_mismatch, F, OtherS, S})
    end;

apply_event(Map, {remove_field, H, F, S}, Meta)
        when is_integer(H), is_binary(F), is_atom(S) ->
    ok = assert_supported(S),
    apply_event(Map, {field_event, F, S, purge_event(S, H)}, Meta).


-spec to_value(state()) -> #{field() => bondy_oplog_fold:fold_value()}.

to_value(Map) ->
    maps:map(
        fun(_F, {S, SubState}) -> bondy_oplog_fold:to_value(S, SubState) end,
        Map
    ).


-doc """
The map-level delta is a per-field directive
`{field, F, SubStrategy, SubDelta}` that defers sub-value computation
until combine-time. The sub-strategy tag is part of the delta because
the value map (`#{F => SubValue}`) does not carry it; without the tag
we could not call back into the right sub-fold's `apply_value_delta`.
""".
-spec apply_value_delta(
        #{field() => bondy_oplog_fold:fold_value()},
        {field, field(), strategy(), bondy_oplog_fold:value_delta()}
    ) -> #{field() => bondy_oplog_fold:fold_value()}.

apply_value_delta(MapValue, {field, F, S, SubDelta}) ->
    SubInit = bondy_oplog_fold:to_value(
        S, bondy_oplog_fold:initial_value(S)
    ),
    OldSubValue = maps:get(F, MapValue, SubInit),
    NewSubValue = bondy_oplog_fold:apply_value_delta(S, OldSubValue, SubDelta),
    MapValue#{F => NewSubValue}.


-spec merge_states(state(), state()) -> state().

merge_states(A, B) when is_map(A), is_map(B) ->
    AllFields = maps:keys(A) ++ maps:keys(B),
    Uniq = lists:usort(AllFields),
    lists:foldl(
        fun(F, Acc) ->
            case {maps:get(F, A, undefined), maps:get(F, B, undefined)} of
                {undefined, EB}     -> Acc#{F => EB};
                {EA, undefined}     -> Acc#{F => EA};
                {EA, EB}            -> Acc#{F => merge_entries(F, EA, EB)}
            end
        end,
        #{},
        Uniq).


-spec hlc(state()) -> bondy_oplog_hlc:hlc().

hlc(Map) when map_size(Map) =:= 0 ->
    0;
hlc(Map) ->
    lists:max([entry_hlc(E) || E <- maps:values(Map)]).


-spec gc_threshold(state()) -> bondy_oplog_hlc:hlc() | undefined.

gc_threshold(Map) when map_size(Map) =:= 0 ->
    undefined;
gc_threshold(Map) ->
    lists:max([entry_gc(E) || E <- maps:values(Map)]).


-spec encode_state(state()) -> binary().

encode_state(Map) when is_map(Map) ->
    N = maps:size(Map),
    Entries = lists:sort(maps:to_list(Map)),
    Body = << <<(byte_size(F)):32/big-unsigned,
                F/binary,
                (encode_entry(E))/binary>>
              || {F, E} <- Entries >>,
    <<N:32/big-unsigned, Body/binary>>.


-spec decode_state(binary()) -> state().

decode_state(<<N:32/big-unsigned, Rest/binary>>) ->
    decode_entries(N, Rest, #{}).


-spec encode_event(event()) -> binary().

encode_event({field_event, F, S, IE})
        when is_binary(F), is_atom(S) ->
    ok = assert_supported(S),
    Tag = bondy_oplog_fold:tag_of(S),
    IEBin = bondy_oplog_fold:encode_event(S, IE),
    <<1,
      (byte_size(F)):32/big-unsigned, F/binary,
      Tag:8,
      (byte_size(IEBin)):32/big-unsigned, IEBin/binary>>;

encode_event({remove_field, H, F, S})
        when is_integer(H), is_binary(F), is_atom(S) ->
    ok = assert_supported(S),
    Tag = bondy_oplog_fold:tag_of(S),
    <<2, H:64/big-unsigned,
         (byte_size(F)):32/big-unsigned, F/binary,
         Tag:8>>.


-spec decode_event(binary()) -> event().

decode_event(<<1,
               FSize:32/big-unsigned, F:FSize/binary,
               Tag:8,
               IESize:32/big-unsigned, IE:IESize/binary>>) ->
    S = bondy_oplog_fold:mod_of_tag(Tag),
    InnerEvent = bondy_oplog_fold:decode_event(S, IE),
    {field_event, F, S, InnerEvent};

decode_event(<<2, H:64/big-unsigned,
                  FSize:32/big-unsigned, F:FSize/binary,
                  Tag:8>>) ->
    S = bondy_oplog_fold:mod_of_tag(Tag),
    {remove_field, H, F, S}.


%% =============================================================================
%% INTERNAL
%% =============================================================================

assert_supported(lww_register)    -> ok;
assert_supported(strict_register) -> ok;
assert_supported(ttl_presence)    -> ok;
assert_supported(Other) ->
    erlang:error({unsupported_field_strategy, Other}).


lift_field_delta(_F, _S, none)     -> none;
lift_field_delta(F,  S,  SubDelta) -> {field, F, S, SubDelta}.


purge_event(lww_register, H)    -> {clear, H};
purge_event(strict_register, H) -> {revoke, H};
purge_event(ttl_presence, H)    -> {revoke, H}.


entry_hlc({S, SubState}) ->
    bondy_oplog_fold:hlc(S, SubState).


entry_gc({S, SubState}) ->
    case bondy_oplog_fold:gc_threshold(S, SubState) of
        undefined -> 0;
        N when is_integer(N) -> N
    end.


merge_entries(_F, {S, SA}, {S, SB}) ->
    Merged = bondy_oplog_fold:merge_states(S, SA, SB),
    {S, Merged};

merge_entries(F, {Sa, _}, {Sb, _}) ->
    erlang:error({strategy_mismatch, F, Sa, Sb}).


encode_entry({S, SubState}) ->
    ok = assert_supported(S),
    Tag = bondy_oplog_fold:tag_of(S),
    Bin = bondy_oplog_fold:encode_state(S, SubState),
    <<Tag:8, (byte_size(Bin)):32/big-unsigned, Bin/binary>>.


decode_entries(0, <<>>, Acc) ->
    Acc;

decode_entries(N, <<FSize:32/big-unsigned, F:FSize/binary,
                    Tag:8, SubSize:32/big-unsigned,
                    Sub:SubSize/binary, Rest/binary>>, Acc)
        when N > 0 ->
    S = bondy_oplog_fold:mod_of_tag(Tag),
    SubState = bondy_oplog_fold:decode_state(S, Sub),
    decode_entries(N - 1, Rest, Acc#{F => {S, SubState}}).

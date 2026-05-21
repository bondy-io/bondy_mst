%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_fold_max_register).
-behaviour(bondy_oplog_fold).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Max-Register fold — an integer register whose value is the **maximum**
of every value ever written.

Lattice register: monotone, naturally CRDT. Concurrent writes converge
because `max` is commutative, associative, and idempotent. To "reset"
a Max-Register, the application allocates a fresh key — there is no
clear/revoke operation.

## State

```
undefined
| {V :: integer(), MaxHlc :: hlc()}
```

`undefined` is the bottom state (no write observed). `{V, MaxHlc}` is
the current maximum value `V` and the maximum HLC across every event
absorbed. HLC is tracked via `max/2` for projection metadata (last-
modified semantics).

## Events

```
{set, V :: integer()}
```

The HLC is read from `Meta` (the WAL event key), not from the payload.

## Idempotency and HLC monotonicity

- `apply_event` is idempotent: replaying the same event leaves the
  state unchanged (`max(V, V) == V`, `max(H, H) == H`).
- `hlc/1` is non-decreasing.

## Merge

Element-wise max — naturally CAI.

## GC

`gc_threshold(undefined) == undefined`. Otherwise the cell's HLC.

## Encoding

```
undefined  -> <<0>>
{V, H}     -> <<1, V:64/big-signed, H:64/big-unsigned>>

event {set, V} -> <<1, V:64/big-signed>>
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

-type state() ::
        undefined
      | {integer(), bondy_oplog_hlc:hlc()}.

-type event() :: {set, integer()}.

-export_type([state/0, event/0]).

%% =============================================================================
%% CALLBACKS
%% =============================================================================

-spec initial_value() -> state().

initial_value() ->
    undefined.


-spec apply_event(state(), event(), bondy_oplog_fold:meta()) ->
    bondy_oplog_fold:apply_result().

apply_event(undefined, {set, V}, Meta)
        when is_integer(V), Meta =/= undefined ->
    {{V, bondy_oplog_event:key_hlc(Meta)}, V};

apply_event({Old, OldH}, {set, V}, Meta)
        when is_integer(V), Meta =/= undefined ->
    H = bondy_oplog_event:key_hlc(Meta),
    NewV = erlang:max(Old, V),
    NewState = {NewV, erlang:max(OldH, H)},
    Delta = case NewV =:= Old of
        true  -> none;
        false -> NewV
    end,
    {NewState, Delta}.


-spec to_value(state()) -> undefined | integer().

to_value(undefined)  -> undefined;
to_value({V, _H})    -> V.


-spec apply_value_delta(undefined | integer(), integer()) -> integer().

apply_value_delta(_OldValue, NewValue) ->
    NewValue.


-spec merge_states(state(), state()) -> state().

merge_states(undefined, B) -> B;
merge_states(A, undefined) -> A;
merge_states({Va, Ha}, {Vb, Hb}) ->
    {erlang:max(Va, Vb), erlang:max(Ha, Hb)}.


-spec hlc(state()) -> bondy_oplog_hlc:hlc().

hlc(undefined) -> 0;
hlc({_V, H})   -> H.


-spec gc_threshold(state()) -> bondy_oplog_hlc:hlc() | undefined.

gc_threshold(undefined) -> undefined;
gc_threshold({_V, H})   -> H.


-spec encode_state(state()) -> binary().

encode_state(undefined) ->
    <<0>>;
encode_state({V, H}) when is_integer(V), is_integer(H) ->
    <<1, V:64/big-signed, H:64/big-unsigned>>.


-spec decode_state(binary()) -> state().

decode_state(<<0>>) ->
    undefined;
decode_state(<<1, V:64/big-signed, H:64/big-unsigned>>) ->
    {V, H}.


-spec encode_event(event()) -> binary().

encode_event({set, V}) when is_integer(V) ->
    <<1, V:64/big-signed>>.


-spec decode_event(binary()) -> event().

decode_event(<<1, V:64/big-signed>>) ->
    {set, V}.

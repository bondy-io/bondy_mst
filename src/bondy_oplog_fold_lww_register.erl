%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_fold_lww_register).
-behaviour(bondy_oplog_fold).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Last-writer-wins (LWW) register fold.

A single-value register with LWW conflict resolution by HLC. Concurrent
writes are visible briefly during convergence; the register eventually
settles on the value carried by the event with the highest HLC.

## State

```
undefined
| {set, register_value(), hlc()}
| {cleared, hlc()}
```

- `undefined` — initial; no event observed.
- `{set, V, H}` — value `V` written at HLC `H`.
- `{cleared, H}` — register was cleared at HLC `H`. Not terminal — a
  later-HLC `set` can re-populate the register (this distinguishes LWW
  from `presence_basic`'s terminal `dead`).

## Events

```
{set, hlc(), register_value()}
| {clear, hlc()}
```

`register_value()` is opaque to the fold; the application provides already-
serialised binaries.

## Conflict resolution

Higher HLC wins, regardless of event type. For two `set` events at the
same HLC with different payloads (a concurrent-writer race), the fold
picks the lexicographically larger payload — this is a deterministic
tie-break that preserves commutativity. The same rule applies to
`{set, V, H}` vs `{cleared, H}` at the same HLC: cleared wins
(lexicographic order on the encoded form).

## Idempotency and monotonicity

- Same event applied twice yields the same state.
- `hlc/1` is non-decreasing: every accepted transition moves the
  register HLC forward; rejected events leave HLC unchanged.

## Merge

`merge_states/2` implements per-state LWW: the state with the higher
HLC wins; tied HLCs resolve by the same lex tie-break as `apply_event`.
Commutative, associative, idempotent — see §5 PropEr properties.

## GC

`gc_threshold(State) == hlc(State)` for `{set, _, H}` and `{cleared, H}`.
`gc_threshold(undefined) == 0`. All events with `hlc <= threshold` are
either absorbed or were superseded by the current state, and are safe to
drop from the WAL.

## Encoding

```
undefined       -> <<0>>
{set, V, H}     -> <<1, H:64/big-unsigned, VSize:32/big-unsigned, V/binary>>
{cleared, H}    -> <<2, H:64/big-unsigned>>
```

Events use the same leading-tag pattern.
""").

-export([initial_value/0]).
-export([apply_event/2]).
-export([merge_states/2]).
-export([hlc/1]).
-export([gc_threshold/1]).
-export([encode_state/1]).
-export([decode_state/1]).
-export([encode_event/1]).
-export([decode_event/1]).

-type register_value() :: binary().
-type state() ::
        undefined
        | {set, register_value(), bondy_oplog_hlc:hlc()}
        | {cleared, bondy_oplog_hlc:hlc()}.

-type event() ::
        {set, bondy_oplog_hlc:hlc(), register_value()}
        | {clear, bondy_oplog_hlc:hlc()}.

-export_type([state/0, event/0, value/0]).

%% =============================================================================
%% CALLBACKS
%% =============================================================================

-spec initial_value() -> state().

initial_value() ->
    undefined.


-spec apply_event(state(), event()) -> state().

apply_event(undefined, {set, H, V}) when is_binary(V) ->
    {set, V, H};

apply_event(undefined, {clear, H}) ->
    {cleared, H};

%% set vs current set:
apply_event({set, _OldV, OldH}, {set, H, V}) when H > OldH, is_binary(V) ->
    {set, V, H};

apply_event({set, OldV, OldH} = S, {set, H, V}) when H == OldH, is_binary(V) ->
    %% Tie at same HLC — deterministic resolution on the payload.
    case V > OldV of
        true  -> {set, V, OldH};
        false -> S
    end;

apply_event({set, _, _} = S, {set, _, _}) ->
    %% Older HLC; rejected.
    S;

%% set vs incoming clear:
apply_event({set, _OldV, OldH}, {clear, H}) when H > OldH ->
    {cleared, H};

apply_event({set, _OldV, OldH}, {clear, H}) when H == OldH ->
    %% Tie — cleared deterministically wins.
    {cleared, OldH};

apply_event({set, _, _} = S, {clear, _}) ->
    %% Older clear; rejected.
    S;

%% cleared vs incoming set:
apply_event({cleared, OldH}, {set, H, V}) when H > OldH, is_binary(V) ->
    %% Later-HLC set resurrects the register (LWW: latest wins).
    {set, V, H};

apply_event({cleared, OldH} = S, {set, H, _}) when H =< OldH ->
    %% Older or tied set; cleared retains (cleared wins at tie).
    S;

%% cleared vs incoming clear:
apply_event({cleared, OldH}, {clear, H}) ->
    {cleared, erlang:max(OldH, H)}.


-spec merge_states(state(), state()) -> state().

merge_states(undefined, B) -> B;
merge_states(A, undefined) -> A;

%% set vs set
merge_states({set, _, Ha} = A, {set, _, Hb}) when Ha > Hb -> A;
merge_states({set, _, Ha}, {set, _, Hb} = B) when Hb > Ha -> B;
merge_states({set, Va, H} = A, {set, Vb, H} = B) ->
    case Vb > Va of
        true -> B;
        false -> A
    end;

%% cleared vs cleared
merge_states({cleared, Ha}, {cleared, Hb}) ->
    {cleared, erlang:max(Ha, Hb)};

%% set vs cleared (and reverse)
merge_states({set, _, Hs}, {cleared, Hc}) when Hc > Hs ->
    {cleared, Hc};
merge_states({cleared, Hc}, {set, _, Hs}) when Hc > Hs ->
    {cleared, Hc};
merge_states({set, _, Hs} = A, {cleared, Hc}) when Hs > Hc ->
    A;
merge_states({cleared, Hc}, {set, _, Hs} = B) when Hs > Hc ->
    B;
merge_states({set, _, H}, {cleared, H}) ->
    %% Tie at same HLC — cleared deterministically wins.
    {cleared, H};
merge_states({cleared, H}, {set, _, H}) ->
    {cleared, H}.


-spec hlc(state()) -> bondy_oplog_hlc:hlc().

hlc(undefined)         -> 0;
hlc({set, _, H})       -> H;
hlc({cleared, H})      -> H.


-spec gc_threshold(state()) -> bondy_oplog_hlc:hlc() | undefined.

gc_threshold(undefined)        -> undefined;
gc_threshold({set, _, H})      -> H;
gc_threshold({cleared, H})     -> H.


-spec encode_state(state()) -> binary().

encode_state(undefined) ->
    <<0>>;

encode_state({set, V, H}) when is_binary(V), is_integer(H) ->
    VSize = byte_size(V),
    <<1, H:64/big-unsigned, VSize:32/big-unsigned, V/binary>>;

encode_state({cleared, H}) when is_integer(H) ->
    <<2, H:64/big-unsigned>>.


-spec decode_state(binary()) -> state().

decode_state(<<0>>) ->
    undefined;

decode_state(<<1, H:64/big-unsigned, VSize:32/big-unsigned, V:VSize/binary>>) ->
    {set, V, H};

decode_state(<<2, H:64/big-unsigned>>) ->
    {cleared, H}.


-spec encode_event(event()) -> binary().

encode_event({set, H, V}) when is_integer(H), is_binary(V) ->
    VSize = byte_size(V),
    <<1, H:64/big-unsigned, VSize:32/big-unsigned, V/binary>>;

encode_event({clear, H}) when is_integer(H) ->
    <<2, H:64/big-unsigned>>.


-spec decode_event(binary()) -> event().

decode_event(<<1, H:64/big-unsigned, VSize:32/big-unsigned, V:VSize/binary>>) ->
    {set, H, V};

decode_event(<<2, H:64/big-unsigned>>) ->
    {clear, H}.

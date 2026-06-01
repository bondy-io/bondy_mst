%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_fold_ttl_presence).
-behaviour(bondy_oplog_fold).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
TTL-bounded presence fold.

For cells with an explicit expiry that should self-evict once a
deadline is reached (tokens, leases, time-bounded grants). Each issue
carries an `expiry_hlc` separate from the issue's own HLC; consumers
check freshness via `is_currently_valid/2` against an externally-
supplied "now" HLC.

## State

```
undefined
| {issued, hlc(), expiry_hlc(), payload()}
| {revoked, hlc()}
```

- `undefined` — initial.
- `{issued, H, E, P}` — issued at HLC `H` with expiry `E` and payload
  `P`. Valid until `E`; consumers check via `is_currently_valid/2`.
- `{revoked, H}` — revoked at HLC `H`. **No payload retention**: the
  audit trail is the WAL itself (query events ≤ H for the preceding
  issue). Doc §4.6's `{revoked, H, payload}` form was relaxed to
  preserve CRDT-safe `merge_states/2` associativity (see below).

## Events

```
{issue, hlc(), expiry_hlc(), payload()}
| {revoke, hlc()}
```

## Conflict resolution

LWW by HLC. Newer event wins; ties resolved deterministically:

- `issue` vs `issue` at same HLC: larger `{payload, expiry}` wins —
  payload-first lex order is associative.
- `issue` vs `revoke` at same HLC: revoke wins (security-critical
  operation wins ties — matches `strict_register` and `lww_register`'s
  clear-wins-tie rule).

## Re-issue after revoke (deviation from doc §4.6)

The doc says "Once revoked OR expired, the cell is terminal". This
fold relaxes that: a later-HLC `issue` after a `revoked` state
**reanimates** the cell as `{issued, _, _, _}`. The rationale is
real-world lease lifecycle — revoking and re-granting a lease for the
same resource is a normal admin workflow. Applications that need
strict one-shot semantics (bearer tokens that must never be reused)
should use `strict_register` instead, where `{revoked, _}` is genuinely
terminal.

## Why no payload retention in revoked

The doc's `{revoked, hlc(), payload()}` retains the issued payload as
an audit trail. Under CRDT-safe merge, payload retention requires
either:

1. A join-lattice over all observed payloads (revoked.payload =
   lex-max), which makes a revoke's "retained payload" potentially
   reflect an *older*, *overwritten* issue rather than the issue
   directly revoked. Audit-incorrect.
2. A per-merge-tree "payload at time of revoke" field, which is not
   expressible without tracking the full event history.

PropEr counter-example: `A={issued,0,0,<<>>}, B={revoked,0,undef},
C={revoked,1,undef}` violates `merge(merge(A,B),C) =
merge(A,merge(B,C))` under any local payload-retention rule.

The pragmatic resolution: drop the payload from revoked and treat
the WAL as the source of truth for audit. Reading the event preceding
the revoke (filter on cell_id, HLC < revoke.H, last `issue` wins)
gives an exact audit answer.

## Idempotency and HLC monotonicity

- `apply_event` is idempotent on every transition.
- `hlc/1` is non-decreasing under any event. Late events with HLC ≤
  state HLC are rejected; their HLC does **not** bump state's HLC
  (consistent with the LWW family).

## Merge

`merge_states/2` is pure LWW with revoke-dominant tie-break:

- `undefined ∪ X = X`.
- Two `issued`: higher HLC wins; same HLC resolved by
  `max({payload, expiry})`.
- Two `revoked`: max HLC wins.
- `issued ∪ revoked`: higher HLC wins; if revoke wins → `{revoked,
  Hr}`; if issue wins (re-issue after revoke) → `{issued, Hi, Ei, Pi}`;
  same HLC → revoke wins.

Commutative, associative, idempotent — verified by PropEr (§5.3).

## GC

`gc_threshold({issued, _, E, _}) = E` (the expiry — events leading to
this issue can be GC'd once we pass expiry). `gc_threshold({revoked,
H}) = H`. `gc_threshold(undefined) = undefined`.

This matches doc §4.6. The §5.5 replay-safe property holds: events
with hlc > expiry are a subset of events with hlc > state.hlc, which
is empty for a state folded from all input events.

## Encoding

```
undefined         -> <<0>>
{issued, H, E, P} -> <<1, H:64, E:64, PSize:32, P/binary>>
{revoked, H}      -> <<2, H:64>>
```

Events:

```
{issue, H, E, P}  -> <<1, H:64, E:64, PSize:32, P/binary>>
{revoke, H}       -> <<2, H:64>>
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

%% Public helpers (not behaviour callbacks).
-export([is_currently_valid/2]).

-type payload() :: binary().
-type expiry_hlc() :: bondy_oplog_hlc:hlc().

-type state() ::
    undefined
    | {issued, bondy_oplog_hlc:hlc(), expiry_hlc(), payload()}
    | {revoked, bondy_oplog_hlc:hlc()}.

-type event() ::
    {issue, bondy_oplog_hlc:hlc(), expiry_hlc(), payload()}
    | {revoke, bondy_oplog_hlc:hlc()}.

-export_type([state/0, event/0]).

%% =============================================================================
%% CALLBACKS
%% =============================================================================

-spec initial_value() -> state().

initial_value() ->
    undefined.

-spec apply_event(state(), event(), bondy_oplog_fold:meta()) ->
    bondy_oplog_fold:apply_result().

%% --- from undefined ---------------------------------------------------------

apply_event(undefined, {issue, H, E, P}, _Meta) when
    is_integer(H), is_integer(E), is_binary(P)
->
    {{issued, H, E, P}, P};
apply_event(undefined, {revoke, H}, _Meta) when is_integer(H) ->
    %% Tombstone — preserves idempotency under out-of-order delivery (a
    %% later-arriving `issue` with H' < H would otherwise reanimate).
    %% Value stays undefined.
    {{revoked, H}, none};
%% --- from {issued, _, _, _} -------------------------------------------------

apply_event({issued, OldH, OldE, OldP} = S, {issue, H, E, P}, _Meta) when
    is_binary(P)
->
    if
        H > OldH ->
            {{issued, H, E, P}, P};
        H == OldH andalso E == OldE andalso P == OldP ->
            {S, none};
        H == OldH ->
            %% Tie at HLC — deterministic resolution by `{payload,
            %% expiry}` (payload primary; required for merge associativity).
            case {P, E} > {OldP, OldE} of
                true -> {{issued, OldH, E, P}, P};
                false -> {S, none}
            end;
        true ->
            {S, none}
    end;
apply_event({issued, OldH, _OldE, _OldP}, {revoke, H}, _Meta) when
    H >= OldH
->
    %% Revoke wins ties (security-critical).
    {{revoked, H}, undefined};
apply_event({issued, _, _, _} = S, {revoke, _}, _Meta) ->
    {S, none};
%% --- from {revoked, _} ------------------------------------------------------

apply_event({revoked, OldH}, {issue, H, E, P}, _Meta) when
    H > OldH, is_binary(P)
->
    %% Re-issue after revoke (LWW); see deviation note in moduledoc.
    {{issued, H, E, P}, P};
apply_event({revoked, _} = S, {issue, _, _, _}, _Meta) ->
    {S, none};
apply_event({revoked, OldH}, {revoke, H}, _Meta) ->
    {{revoked, erlang:max(OldH, H)}, none}.

-spec to_value(state()) -> undefined | payload().

to_value(undefined) -> undefined;
to_value({issued, _H, _E, P}) -> P;
to_value({revoked, _}) -> undefined.

-spec apply_value_delta(undefined | payload(), undefined | payload()) ->
    undefined | payload().

apply_value_delta(_OldValue, NewValue) ->
    NewValue.

-spec merge_states(state(), state()) -> state().

merge_states(undefined, B) ->
    B;
merge_states(A, undefined) ->
    A;
%% issued vs issued
merge_states({issued, Ha, _, _} = A, {issued, Hb, _, _}) when Ha > Hb -> A;
merge_states({issued, Ha, _, _}, {issued, Hb, _, _} = B) when Hb > Ha -> B;
merge_states({issued, H, Ea, Pa} = A, {issued, H, Eb, Pb}) ->
    %% Payload-first tie-break (associativity-preserving).
    case {Pb, Eb} > {Pa, Ea} of
        true -> {issued, H, Eb, Pb};
        false -> A
    end;
%% revoked vs revoked
merge_states({revoked, Ha}, {revoked, Hb}) ->
    {revoked, erlang:max(Ha, Hb)};
%% issued vs revoked (and reverse)
merge_states({issued, Hi, _, _}, {revoked, Hr}) when Hr > Hi ->
    {revoked, Hr};
merge_states({revoked, Hr}, {issued, Hi, _, _}) when Hr > Hi ->
    {revoked, Hr};
merge_states({issued, Hi, _, _} = A, {revoked, Hr}) when Hi > Hr -> A;
merge_states({revoked, Hr}, {issued, Hi, _, _} = B) when Hi > Hr -> B;
merge_states({issued, H, _, _}, {revoked, H}) ->
    %% Tie at same HLC — revoke wins.
    {revoked, H};
merge_states({revoked, H}, {issued, H, _, _}) ->
    {revoked, H}.

-spec hlc(state()) -> bondy_oplog_hlc:hlc().

hlc(undefined) -> 0;
hlc({issued, H, _, _}) -> H;
hlc({revoked, H}) -> H.

-spec gc_threshold(state()) -> bondy_oplog_hlc:hlc() | undefined.

gc_threshold(undefined) -> undefined;
gc_threshold({issued, _, E, _}) -> E;
gc_threshold({revoked, H}) -> H.

-spec encode_state(state()) -> binary().

encode_state(undefined) ->
    <<0>>;
encode_state({issued, H, E, P}) when
    is_integer(H), is_integer(E), is_binary(P)
->
    PSize = byte_size(P),
    <<1, H:64/big-unsigned, E:64/big-unsigned, PSize:32/big-unsigned,
        P/binary>>;
encode_state({revoked, H}) when is_integer(H) ->
    <<2, H:64/big-unsigned>>.

-spec decode_state(binary()) -> state().

decode_state(<<0>>) ->
    undefined;
decode_state(
    <<1, H:64/big-unsigned, E:64/big-unsigned, PSize:32/big-unsigned,
        P:PSize/binary>>
) ->
    {issued, H, E, P};
decode_state(<<2, H:64/big-unsigned>>) ->
    {revoked, H}.

-spec encode_event(event()) -> binary().

encode_event({issue, H, E, P}) when
    is_integer(H), is_integer(E), is_binary(P)
->
    PSize = byte_size(P),
    <<1, H:64/big-unsigned, E:64/big-unsigned, PSize:32/big-unsigned,
        P/binary>>;
encode_event({revoke, H}) when is_integer(H) ->
    <<2, H:64/big-unsigned>>.

-spec decode_event(binary()) -> event().

decode_event(
    <<1, H:64/big-unsigned, E:64/big-unsigned, PSize:32/big-unsigned,
        P:PSize/binary>>
) ->
    {issue, H, E, P};
decode_event(<<2, H:64/big-unsigned>>) ->
    {revoke, H}.

%% =============================================================================
%% Public helpers
%% =============================================================================

-doc """
Check whether the cell is currently valid (issued and unexpired)
against the given "now" HLC.

`NowHlc` is supplied by the caller — typically from `bondy_oplog_hlc:
now/0` or equivalent. The fold itself doesn't observe wall time.
""".
-spec is_currently_valid(state(), bondy_oplog_hlc:hlc()) -> boolean().

is_currently_valid({issued, _, E, _}, NowHlc) ->
    NowHlc < E;
is_currently_valid({revoked, _}, _) ->
    false;
is_currently_valid(undefined, _) ->
    false.

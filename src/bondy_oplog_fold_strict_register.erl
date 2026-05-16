%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_fold_strict_register).
-behaviour(bondy_oplog_fold).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Strict register fold — a single-value register that **surfaces** rather
than silently resolves concurrent writes.

Where `lww_register` collapses two concurrent writes by HLC and (for
ties) lexicographic ordering, `strict_register` lifts the tie into a
visible `{conflict, [...]}` state. This is the right fold for any
namespace where two simultaneous writes are an **invariant violation**
(authorisation grants, single-policy registrations, leader leases) and
must be resolved out-of-band rather than papered over.

## State

```
undefined
| {set, register_value(), hlc()}
| {conflict, [{register_value(), hlc()}]}
| {revoked, hlc()}
```

- `undefined` — initial.
- `{set, V, H}` — value `V` written at HLC `H`.
- `{conflict, States}` — two or more concurrent writers produced
  distinct values at the same HLC. `States` is `lists:usort`-canonical
  so the cell converges to the same representation regardless of arrival
  order. Cleared only by an explicit `{resolve, _, _}` or `{revoke, _}`
  event.
- `{revoked, H}` — **terminal**. A revoke is a security-critical
  operation; once revoked, the cell absorbs further events (HLC bump
  only) but never transitions to another state.

## Events

```
{set, hlc(), register_value()}
| {revoke, hlc()}
| {resolve, hlc(), register_value()}
```

`resolve` is the admin-issued escape hatch out of `{conflict, _}`. It
also works on `{set, _, _}` (admin overwrite) and `undefined` (degrades
to a normal set).

## Conflict detection

The fold compares encoded HLCs by integer order, so the only signal of
**concurrent** writes available to it is *equal HLC, distinct value*.
Two writers producing the same `(H, V)` are idempotent; two producing
the same `H` with different `V` are surfaced as a conflict. Different
HLCs are always causally ordered and the higher-HLC write wins (LWW
within the non-conflict path).

This is a deliberate deviation from `FOLD_STRATEGY_DESIGN.md` §4.3,
whose pseudo-code references `causally_before/2`. The fold has no node
identity beyond what the HLC encodes, so the strongest defensible
predicate is HLC equality — anything stricter would require breaking
the fold's pure-function contract.

## Revocation is terminal

`{revoked, H}` absorbs all subsequent events. A late `{set, H', V}`
with `H' > H` does NOT resurrect the register, by analogy with
`presence_basic`'s `dead`. This is required for CRDT-safe merge: if
revoke is to be a security-critical signal, it must dominate the merge
order, which in turn requires terminal absorption under sequenced
apply. The same `merge_states/2` ↔ folded-apply consistency forced this
choice.

## Idempotency and monotonicity

- Same event applied twice yields the same state.
- `hlc/1` is non-decreasing on every transition. Where a transition
  would otherwise produce a lower HLC than the state already holds,
  the HLC is bumped via `erlang:max`.

## Merge

`merge_states/2` is symmetric union with the same rules as
`apply_event/2`:

- `undefined ∪ X = X`
- `{revoked, _} ∪ X = {revoked, max_hlc}` (revoke dominates)
- `{set, V_a, H_a} ∪ {set, V_b, H_b}`: higher HLC wins; same HLC and
  same V is idempotent; same HLC and different V produces a 2-entry
  conflict.
- `{set, V, H} ∪ {conflict, S}` (and vice versa): `(V, H)` is folded
  into `S` via `usort`.
- `{conflict, A} ∪ {conflict, B}`: `usort(A ++ B)`.

Commutative, associative, idempotent — verified by PropEr (§5.3).

## GC

`gc_threshold(State) == hlc(State)` for every non-`undefined` state.
`gc_threshold(undefined) == undefined`.

## Encoding

```
undefined            -> <<0>>
{set, V, H}          -> <<1, H:64, VSize:32, V/binary>>
{conflict, States}   -> <<2, N:32, (<<H:64, VSize:32, V/binary>>)+>>
{revoked, H}         -> <<3, H:64>>
```

Events:

```
{set, H, V}          -> <<1, H:64, VSize:32, V/binary>>
{revoke, H}          -> <<2, H:64>>
{resolve, H, V}      -> <<3, H:64, VSize:32, V/binary>>
```
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
-type conflict_entry() :: {register_value(), bondy_oplog_hlc:hlc()}.

-type state() ::
        undefined
        | {set, register_value(), bondy_oplog_hlc:hlc()}
        | {conflict, [conflict_entry()]}
        | {revoked, bondy_oplog_hlc:hlc()}.

-type event() ::
        {set, bondy_oplog_hlc:hlc(), register_value()}
        | {revoke, bondy_oplog_hlc:hlc()}
        | {resolve, bondy_oplog_hlc:hlc(), register_value()}.

-export_type([state/0, event/0]).

%% =============================================================================
%% CALLBACKS
%% =============================================================================

-spec initial_value() -> state().

initial_value() ->
    undefined.


-spec apply_event(state(), event()) -> state().

%% --- from undefined ---------------------------------------------------------

apply_event(undefined, {set, H, V}) when is_binary(V) ->
    {set, V, H};

apply_event(undefined, {revoke, H}) ->
    {revoked, H};

apply_event(undefined, {resolve, H, V}) when is_binary(V) ->
    %% Admin-issued resolve on an empty cell degrades to a normal set.
    {set, V, H};

%% --- from {set, _, _} -------------------------------------------------------

apply_event({set, V, OldH} = S, {set, H, V2}) when is_binary(V2) ->
    if
        H < OldH ->
            S;
        H == OldH andalso V == V2 ->
            S;
        H == OldH ->
            %% Concurrent write at the same HLC with a different value —
            %% surface as a conflict (canonical sorted form).
            {conflict, lists:usort([{V, OldH}, {V2, H}])};
        H > OldH ->
            %% Strictly newer — causally ordered, LWW.
            {set, V2, H}
    end;

apply_event({set, _V, OldH}, {revoke, H}) when H >= OldH ->
    %% Revoke at >= state HLC: terminal. Tie goes to revoke (security-
    %% critical operation wins ties, like cleared in lww_register).
    {revoked, H};

apply_event({set, _, _} = S, {revoke, _}) ->
    S;

apply_event({set, _V, OldH}, {resolve, H, V2}) when H >= OldH, is_binary(V2) ->
    {set, V2, H};

apply_event({set, _, _} = S, {resolve, _, _}) ->
    S;

%% --- from {conflict, States} ------------------------------------------------

apply_event({conflict, States}, {set, H, V}) when is_binary(V) ->
    Entry = {V, H},
    case lists:member(Entry, States) of
        true ->
            {conflict, States};
        false ->
            %% Keep canonical sorted form so equal sets of entries
            %% produce equal states regardless of arrival order.
            {conflict, lists:usort([Entry | States])}
    end;

apply_event({conflict, States}, {revoke, H}) ->
    MaxH = max_conflict_hlc(States),
    case H >= MaxH of
        true  -> {revoked, H};
        false -> {conflict, States}
    end;

apply_event({conflict, States}, {resolve, H, V}) when is_binary(V) ->
    MaxH = max_conflict_hlc(States),
    case H >= MaxH of
        true  -> {set, V, H};
        false -> {conflict, States}
    end;

%% --- from {revoked, _} (terminal) ------------------------------------------

apply_event({revoked, OldH}, {set, H, V}) when is_binary(V) ->
    {revoked, erlang:max(OldH, H)};

apply_event({revoked, OldH}, {revoke, H}) ->
    {revoked, erlang:max(OldH, H)};

apply_event({revoked, OldH}, {resolve, H, V}) when is_binary(V) ->
    {revoked, erlang:max(OldH, H)}.


-spec merge_states(state(), state()) -> state().

merge_states(undefined, B) -> B;
merge_states(A, undefined) -> A;

%% revoke dominates from either side
merge_states({revoked, Hr}, B) ->
    {revoked, erlang:max(Hr, hlc(B))};
merge_states(A, {revoked, Hr}) ->
    {revoked, erlang:max(Hr, hlc(A))};

%% set vs set
merge_states({set, _, Ha} = A, {set, _, Hb}) when Ha > Hb -> A;
merge_states({set, _, Ha}, {set, _, Hb} = B) when Hb > Ha -> B;
merge_states({set, V, H} = A, {set, V, H}) -> A;
merge_states({set, Va, H}, {set, Vb, H}) ->
    %% Same HLC, distinct values — surface a conflict (canonical form).
    {conflict, lists:usort([{Va, H}, {Vb, H}])};

%% conflict vs conflict
merge_states({conflict, A}, {conflict, B}) ->
    {conflict, lists:usort(A ++ B)};

%% set vs conflict (and reverse)
merge_states({set, V, H}, {conflict, States}) ->
    {conflict, lists:usort([{V, H} | States])};
merge_states({conflict, States}, {set, V, H}) ->
    {conflict, lists:usort([{V, H} | States])}.


-spec hlc(state()) -> bondy_oplog_hlc:hlc().

hlc(undefined)         -> 0;
hlc({set, _, H})       -> H;
hlc({conflict, States}) -> max_conflict_hlc(States);
hlc({revoked, H})      -> H.


-spec gc_threshold(state()) -> bondy_oplog_hlc:hlc() | undefined.

gc_threshold(undefined)         -> undefined;
gc_threshold({set, _, H})       -> H;
gc_threshold({conflict, States}) -> max_conflict_hlc(States);
gc_threshold({revoked, H})      -> H.


-spec encode_state(state()) -> binary().

encode_state(undefined) ->
    <<0>>;

encode_state({set, V, H}) when is_binary(V), is_integer(H) ->
    VSize = byte_size(V),
    <<1, H:64/big-unsigned, VSize:32/big-unsigned, V/binary>>;

encode_state({conflict, States}) when is_list(States) ->
    N = length(States),
    Body = << <<H:64/big-unsigned,
                (byte_size(V)):32/big-unsigned,
                V/binary>>
              || {V, H} <- States >>,
    <<2, N:32/big-unsigned, Body/binary>>;

encode_state({revoked, H}) when is_integer(H) ->
    <<3, H:64/big-unsigned>>.


-spec decode_state(binary()) -> state().

decode_state(<<0>>) ->
    undefined;

decode_state(<<1, H:64/big-unsigned, VSize:32/big-unsigned, V:VSize/binary>>) ->
    {set, V, H};

decode_state(<<2, N:32/big-unsigned, Rest/binary>>) ->
    {conflict, decode_conflict_entries(N, Rest)};

decode_state(<<3, H:64/big-unsigned>>) ->
    {revoked, H}.


-spec encode_event(event()) -> binary().

encode_event({set, H, V}) when is_integer(H), is_binary(V) ->
    VSize = byte_size(V),
    <<1, H:64/big-unsigned, VSize:32/big-unsigned, V/binary>>;

encode_event({revoke, H}) when is_integer(H) ->
    <<2, H:64/big-unsigned>>;

encode_event({resolve, H, V}) when is_integer(H), is_binary(V) ->
    VSize = byte_size(V),
    <<3, H:64/big-unsigned, VSize:32/big-unsigned, V/binary>>.


-spec decode_event(binary()) -> event().

decode_event(<<1, H:64/big-unsigned, VSize:32/big-unsigned, V:VSize/binary>>) ->
    {set, H, V};

decode_event(<<2, H:64/big-unsigned>>) ->
    {revoke, H};

decode_event(<<3, H:64/big-unsigned, VSize:32/big-unsigned, V:VSize/binary>>) ->
    {resolve, H, V}.


%% =============================================================================
%% INTERNAL
%% =============================================================================

max_conflict_hlc(States) ->
    lists:max([H || {_, H} <- States]).

decode_conflict_entries(0, <<>>) ->
    [];
decode_conflict_entries(N, <<H:64/big-unsigned,
                             VSize:32/big-unsigned,
                             V:VSize/binary,
                             Rest/binary>>) when N > 0 ->
    [{V, H} | decode_conflict_entries(N - 1, Rest)].

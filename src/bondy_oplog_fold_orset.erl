%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_fold_orset).
-behaviour(bondy_oplog_fold).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Observed-Remove Set (OR-Set) fold.

A classical CRDT for set-shaped cells (membership lists, subscription
sets, capability sets) where concurrent add/remove operations must
converge without coordination. Each `add` is tagged with a unique
**dot** `{node_id, counter}`; a `remove` carries the list of dots it
observed at the originating replica. Concurrent add-while-remove keeps
the new dot (the remover didn't observe it), preserving the OR-Set's
defining property.

## State

```
#{
    live       := #{element() => ordsets:ordset(dot())},
    tombstones := ordsets:ordset(dot()),
    hlc        := hlc()
}
```

- `live` — for each currently-present element, the set of dots that
  added it. An element with no active dots is **dropped** from the
  map (canonical form: no empty entries).
- `tombstones` — global set of dots that have been observed-removed.
  Tombstones are sticky: once a dot is tombstoned, any later-arriving
  `add` for that dot is rejected (idempotent no-op).
- `hlc` — maximum HLC absorbed by the cell.

## Events

```
{add, hlc(), element(), dot()}
| {remove, hlc(), element(), [dot()]}
```

`dot() :: {node_id(), counter()}`. Both fields are application-supplied
binaries / non-negative integers; the fold treats them as opaque
identifiers.

## Why tombstones (deviation from doc §4.5)

The doc's pseudo-code drops removes targeting unseen elements
(`undefined -> State`). That's incorrect under arbitrary message
reordering: if a remove of `(e, dot1)` arrives at a replica before the
add of `(e, dot1)` from another replica, the doc's rule loses the
remove and the cell reanimates. Tombstones close this gap — the remove
records `dot1` as tombstoned regardless of whether the add has been
seen, and the later add is rejected on `is_element/2` against the
tombstone set.

This is the standard OR-Set tombstone discipline (Shapiro et al. 2011);
the fold's contract is fully commutative under any arrival order.

## Tombstones are global, not per-element

OR-Set dots are unique by construction: each dot identifies a single
`(element, add-event)` pair. Tombstones are therefore a **global** set,
not partitioned by element. On `apply_event(_, {remove, _, _, Dots})`,
the fold scrubs every live entry of those dots — even ones associated
with a different element than the remove names. This sanitises
malformed input (a remove naming the wrong element) and preserves the
invariant `live ∩ tombstones = ∅` end-to-end, which is required for
`merge_states/2` idempotency: without it, `merge(S, S)` could differ
from `S` after a tombstone-vs-live conflict.

## Idempotency and HLC monotonicity

- `apply_event` is idempotent on every transition. `ordsets` operations
  are idempotent for repeated adds/removals, and HLC is monotone via
  `erlang:max/2`.
- Unlike LWW/strict folds, OR-Set events are **never rejected on HLC**:
  causality between dots is captured by the dot identities themselves,
  not by HLC ordering. HLC only tracks the cell's "last modified at"
  for GC and projection.

## Merge

Symmetric union of components, with tombstones applied to the unified
live map (so a dot tombstoned in either replica wins out across the
merge). Commutative, associative, idempotent — verified by PropEr (§5.3).

## GC

`gc_threshold(State) == hlc(State)` for any state where events have
been absorbed; `undefined` for the literal initial value. The tombstone
set is part of the state representation and survives GC of older WAL
events.

## Encoding

```
state -> <<HLC:64,
           NumLive:32, <encoded live entries, sorted by element>,
           NumTomb:32, <encoded tombstone dots, ordset order>>>

live entry -> <<ElemSize:32, Elem/binary,
                NumDots:32, <encoded dots, ordset order>>>

dot -> <<NodeSize:16, Node/binary, Counter:64>>

events -> tag 1 = add; tag 2 = remove (with dot-list length prefix).
```

The encoding is canonical: live map entries are sorted by element,
dot sets are ordset (sorted), tombstones are ordset (sorted) — so two
`=:=` states produce byte-identical encodings.
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

-type element_v() :: binary().
-type node_id() :: binary().
-type counter() :: non_neg_integer().
-type dot() :: {node_id(), counter()}.
-type dot_set() :: ordsets:ordset(dot()).
-type live_map() :: #{element_v() => dot_set()}.

-type state() :: #{
    live := live_map(),
    tombstones := dot_set(),
    hlc := bondy_oplog_hlc:hlc()
}.

-type event() ::
    {add, bondy_oplog_hlc:hlc(), element_v(), dot()}
    | {remove, bondy_oplog_hlc:hlc(), element_v(), [dot()]}.

-export_type([state/0, event/0, dot/0]).

%% =============================================================================
%% CALLBACKS
%% =============================================================================

-spec initial_value() -> state().

initial_value() ->
    #{live => #{}, tombstones => [], hlc => 0}.

-spec apply_event(state(), event(), bondy_oplog_fold:meta()) ->
    bondy_oplog_fold:apply_result().

apply_event(
    #{live := L, tombstones := T, hlc := H0} = S,
    {add, H, Elem, Dot},
    _Meta
) when
    is_binary(Elem)
->
    H1 = erlang:max(H, H0),
    case ordsets:is_element(Dot, T) of
        true ->
            %% Dot was observed-removed; OR-Set semantics says a re-add
            %% of a tombstoned dot is a no-op. (To "re-add" the element,
            %% the application allocates a fresh dot.)
            {S#{hlc := H1}, none};
        false ->
            WasLive = maps:is_key(Elem, L),
            Dots0 = maps:get(Elem, L, []),
            Dots1 = ordsets:add_element(Dot, Dots0),
            NewS = S#{live := L#{Elem => Dots1}, hlc := H1},
            Delta =
                case WasLive of
                    true -> none;
                    false -> {add_elem, Elem}
                end,
            {NewS, Delta}
    end;
apply_event(
    #{live := L0, tombstones := T0, hlc := H0} = S,
    {remove, H, _Elem, ObservedDots},
    _Meta
) when
    is_list(ObservedDots)
->
    H1 = erlang:max(H, H0),
    DotsToTomb = ordsets:from_list(ObservedDots),
    T1 = ordsets:union(T0, DotsToTomb),
    %% Tombstones are global: dots are globally unique by the OR-Set's
    %% input contract, so a remove scrubs the tombstoned dots from
    %% every element's live set. This preserves the invariant
    %% `live ∩ tombstones = ∅` even when the remove names the wrong
    %% element (which is malformed input, but the fold sanitises rather
    %% than corrupting state).
    {L1, RemovedElems} = scrub_dots(L0, DotsToTomb),
    NewS = S#{live := L1, tombstones := T1, hlc := H1},
    Delta =
        case RemovedElems of
            [] -> none;
            Els -> {remove_elems, ordsets:from_list(Els)}
        end,
    {NewS, Delta}.

-spec to_value(state()) -> ordsets:ordset(element_v()).

to_value(#{live := L}) ->
    ordsets:from_list(maps:keys(L)).

-doc """
Combine an OR-Set value with an `apply_event/3` delta.

Deltas:

- `{add_elem, Elem}` — the event lifted `Elem` into membership.
- `{remove_elems, Elems}` — the event evicted every listed element
  (their last live dot was tombstoned).
""".
-spec apply_value_delta(
    ordsets:ordset(element_v()),
    {add_elem, element_v()}
    | {remove_elems, ordsets:ordset(element_v())}
) ->
    ordsets:ordset(element_v()).

apply_value_delta(OldValue, {add_elem, Elem}) ->
    ordsets:add_element(Elem, OldValue);
apply_value_delta(OldValue, {remove_elems, Elems}) ->
    ordsets:subtract(OldValue, Elems).

-spec merge_states(state(), state()) -> state().

merge_states(
    #{live := La, tombstones := Ta, hlc := Ha},
    #{live := Lb, tombstones := Tb, hlc := Hb}
) ->
    Tu = ordsets:union(Ta, Tb),
    Hu = erlang:max(Ha, Hb),
    Lu = merge_live(La, Lb, Tu),
    #{live => Lu, tombstones => Tu, hlc => Hu}.

-spec hlc(state()) -> bondy_oplog_hlc:hlc().

hlc(#{hlc := H}) -> H.

-spec gc_threshold(state()) -> bondy_oplog_hlc:hlc() | undefined.

gc_threshold(#{live := L, tombstones := [], hlc := 0}) when map_size(L) == 0 ->
    undefined;
gc_threshold(#{hlc := H}) ->
    H.

-spec encode_state(state()) -> binary().

encode_state(#{live := L, tombstones := T, hlc := H}) ->
    LiveEntries = lists:sort(maps:to_list(L)),
    NumLive = length(LiveEntries),
    LiveBin = iolist_to_binary([
        encode_live_entry(E, D)
     || {E, D} <- LiveEntries
    ]),
    NumTomb = length(T),
    TombBin = iolist_to_binary([encode_dot(D) || D <- T]),
    <<H:64/big-unsigned, NumLive:32/big-unsigned, LiveBin/binary,
        NumTomb:32/big-unsigned, TombBin/binary>>.

-spec decode_state(binary()) -> state().

decode_state(<<H:64/big-unsigned, NumLive:32/big-unsigned, Rest0/binary>>) ->
    {LiveEntries, Rest1} = decode_live_entries(NumLive, Rest0, []),
    <<NumTomb:32/big-unsigned, Rest2/binary>> = Rest1,
    {Tombs, <<>>} = decode_dots(NumTomb, Rest2, []),
    #{
        live => maps:from_list(LiveEntries),
        tombstones => Tombs,
        hlc => H
    }.

-spec encode_event(event()) -> binary().

encode_event({add, H, Elem, Dot}) when
    is_integer(H), is_binary(Elem)
->
    ElemSize = byte_size(Elem),
    DotBin = encode_dot(Dot),
    <<1, H:64/big-unsigned, ElemSize:32/big-unsigned, Elem/binary,
        DotBin/binary>>;
encode_event({remove, H, Elem, ObservedDots}) when
    is_integer(H), is_binary(Elem), is_list(ObservedDots)
->
    ElemSize = byte_size(Elem),
    NumDots = length(ObservedDots),
    DotsBin = iolist_to_binary([encode_dot(D) || D <- ObservedDots]),
    <<2, H:64/big-unsigned, ElemSize:32/big-unsigned, Elem/binary,
        NumDots:32/big-unsigned, DotsBin/binary>>.

-spec decode_event(binary()) -> event().

decode_event(
    <<1, H:64/big-unsigned, ElemSize:32/big-unsigned, Elem:ElemSize/binary,
        Rest/binary>>
) ->
    {Dot, <<>>} = decode_one_dot(Rest),
    {add, H, Elem, Dot};
decode_event(
    <<2, H:64/big-unsigned, ElemSize:32/big-unsigned, Elem:ElemSize/binary,
        NumDots:32/big-unsigned, Rest/binary>>
) ->
    {Dots, <<>>} = decode_dots(NumDots, Rest, []),
    {remove, H, Elem, Dots}.

%% =============================================================================
%% INTERNAL — scrub tombstoned dots from every live entry
%% =============================================================================

scrub_dots(Live, DotsToTomb) ->
    maps:fold(
        fun(K, Dots, {AccLive, AccRemoved}) ->
            Remaining = ordsets:subtract(Dots, DotsToTomb),
            case Remaining of
                [] -> {AccLive, [K | AccRemoved]};
                _ -> {AccLive#{K => Remaining}, AccRemoved}
            end
        end,
        {#{}, []},
        Live
    ).

%% =============================================================================
%% INTERNAL — merge
%% =============================================================================

merge_live(La, Lb, Tombs) ->
    Keys = lists:usort(maps:keys(La) ++ maps:keys(Lb)),
    lists:foldl(
        fun(K, Acc) ->
            DotsA = maps:get(K, La, []),
            DotsB = maps:get(K, Lb, []),
            U = ordsets:union(DotsA, DotsB),
            Live = ordsets:subtract(U, Tombs),
            case Live of
                [] -> Acc;
                _ -> Acc#{K => Live}
            end
        end,
        #{},
        Keys
    ).

%% =============================================================================
%% INTERNAL — encoding
%% =============================================================================

encode_live_entry(Elem, Dots) ->
    ElemSize = byte_size(Elem),
    NumDots = length(Dots),
    DotsBin = iolist_to_binary([encode_dot(D) || D <- Dots]),
    <<ElemSize:32/big-unsigned, Elem/binary, NumDots:32/big-unsigned,
        DotsBin/binary>>.

encode_dot({Node, Counter}) when is_binary(Node), is_integer(Counter) ->
    NodeSize = byte_size(Node),
    <<NodeSize:16/big-unsigned, Node/binary, Counter:64/big-unsigned>>.

decode_live_entries(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_live_entries(
    N,
    <<ElemSize:32/big-unsigned, Elem:ElemSize/binary, NumDots:32/big-unsigned,
        Rest0/binary>>,
    Acc
) when N > 0 ->
    {Dots, Rest1} = decode_dots(NumDots, Rest0, []),
    decode_live_entries(N - 1, Rest1, [{Elem, Dots} | Acc]).

decode_dots(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_dots(N, Bin, Acc) when N > 0 ->
    {Dot, Rest} = decode_one_dot(Bin),
    decode_dots(N - 1, Rest, [Dot | Acc]).

decode_one_dot(
    <<NodeSize:16/big-unsigned, Node:NodeSize/binary, Counter:64/big-unsigned,
        Rest/binary>>
) ->
    {{Node, Counter}, Rest}.

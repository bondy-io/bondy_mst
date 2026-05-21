%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_fold_g_set).
-behaviour(bondy_oplog_fold).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Grow-Only Set (G-Set) fold.

A monotone set with `add`-only semantics: elements can be added but
never removed. Concurrent adds converge trivially because
`ordsets:union/2` is commutative, associative, and idempotent.

For namespaces that need observed-remove semantics use
`bondy_oplog_fold_orset` instead.

## State

```
{Set :: ordsets:ordset(binary()), MaxHlc :: hlc()}
```

The paper-design representation is "just the ordset", but the
substrate's `hlc/1` callback needs a maximum HLC to encode into the
cell frame, so the fold tracks it inside the state. HLC is monotone
via `erlang:max/2`.

## Events

```
{add, Elem :: binary()}
```

The HLC is read from `Meta` (the WAL event key). The element payload
is opaque to the fold — applications provide already-serialised
binaries.

## Idempotency

`ordsets:add_element/2` is idempotent for repeated adds, so replaying
or duplicate delivery is naturally a no-op.

## Merge

`merge_states({Sa, Ha}, {Sb, Hb}) = {ordsets:union(Sa, Sb),
                                       max(Ha, Hb)}` — CAI.

## GC

`gc_threshold(State) == hlc(State)` once any event has been absorbed;
`undefined` for the literal initial value. The set itself is part of
the state and survives GC of older WAL events.

## `value_equals_state/0 -> true`

G-Set declares this optional callback as `true`. The substrate then
omits the value column from the cell frame and treats the state bytes
as the value bytes on HEAD reads — sets of thousands of elements no
longer pay a 2x storage tax for value-column duplication. The reader
decodes the state and projects via `to_value/1` to obtain the
user-facing ordset.

## Encoding

```
state  -> <<MaxHlc:64,
            NumElems:32,
            <encoded elements, ordset order>>>

element -> <<ElemSize:32, Elem/binary>>

event {add, Elem} -> <<1, ElemSize:32, Elem/binary>>
```

The encoding is canonical: elements are stored in ordset order so two
`=:=` states produce byte-identical encodings.
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

-type elem()  :: binary().
-type set_t() :: ordsets:ordset(elem()).
-type state() :: {set_t(), bondy_oplog_hlc:hlc()}.
-type event() :: {add, elem()}.

-export_type([state/0, event/0, elem/0]).

%% =============================================================================
%% CALLBACKS
%% =============================================================================

-spec initial_value() -> state().

initial_value() ->
    {[], 0}.


-spec apply_event(state(), event(), bondy_oplog_fold:meta()) ->
    bondy_oplog_fold:apply_result().

apply_event({Set, H0}, {add, Elem}, Meta)
        when is_binary(Elem), Meta =/= undefined ->
    H = bondy_oplog_event:key_hlc(Meta),
    NewState = {ordsets:add_element(Elem, Set), erlang:max(H0, H)},
    %% `value_equals_state/0 -> true`: substrate omits the value
    %% column, so we never emit a separate delta. The state bytes
    %% double as the value bytes.
    {NewState, none}.


-spec to_value(state()) -> set_t().

to_value({Set, _H}) -> Set.


-spec value_equals_state() -> true.

value_equals_state() -> true.


-spec merge_states(state(), state()) -> state().

merge_states({Sa, Ha}, {Sb, Hb}) ->
    {ordsets:union(Sa, Sb), erlang:max(Ha, Hb)}.


-spec hlc(state()) -> bondy_oplog_hlc:hlc().

hlc({_S, H}) -> H.


-spec gc_threshold(state()) -> bondy_oplog_hlc:hlc() | undefined.

gc_threshold({[], 0}) -> undefined;
gc_threshold({_S, H}) -> H.


-spec encode_state(state()) -> binary().

encode_state({Set, H}) when is_integer(H) ->
    NumElems = length(Set),
    ElemsBin = iolist_to_binary([encode_elem(E) || E <- Set]),
    <<H:64/big-unsigned,
      NumElems:32/big-unsigned,
      ElemsBin/binary>>.


-spec decode_state(binary()) -> state().

decode_state(<<H:64/big-unsigned,
               NumElems:32/big-unsigned, Rest0/binary>>) ->
    {Elems, <<>>} = decode_elems(NumElems, Rest0, []),
    {Elems, H}.


-spec encode_event(event()) -> binary().

encode_event({add, Elem}) when is_binary(Elem) ->
    ElemSize = byte_size(Elem),
    <<1, ElemSize:32/big-unsigned, Elem/binary>>.


-spec decode_event(binary()) -> event().

decode_event(<<1, ElemSize:32/big-unsigned, Elem:ElemSize/binary>>) ->
    {add, Elem}.


%% =============================================================================
%% INTERNAL
%% =============================================================================

encode_elem(Elem) when is_binary(Elem) ->
    ElemSize = byte_size(Elem),
    <<ElemSize:32/big-unsigned, Elem/binary>>.

decode_elems(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_elems(N, <<ElemSize:32/big-unsigned, Elem:ElemSize/binary, Rest/binary>>,
             Acc) when N > 0 ->
    decode_elems(N - 1, Rest, [Elem | Acc]).

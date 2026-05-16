%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_fold_presence_basic).
-behaviour(bondy_oplog_fold).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Basic presence fold — a three-state machine for namespaces where every
cell has a **unique writer by construction** (no concurrent writes to
the same key).

## State

```
empty
| {live, hlc(), binary()}
| {dead, hlc()}
```

- `empty` — never observed.
- `{live, H, P}` — currently present, last touched at HLC `H`, payload `P`.
- `{dead, H}` — terminal; was deleted at HLC `H`. No further state change.

## Events

```
{create, hlc(), binary()}
| {delete, hlc()}
```

## Idempotency and monotonicity

- `apply_event` is idempotent on every transition: applying the same
  event twice yields the same state as applying it once.
- `hlc/1` is non-decreasing under any event: every transition takes
  `max(old_state_hlc, event_hlc)`. This lets us preserve monotonicity
  even when events arrive out of causal order (e.g. a delete arrives
  before the create it supersedes).
- A delete arriving before a create on an `empty` cell produces a
  tombstone `{dead, H}` rather than a silent no-op. Without this, a
  later-arriving create with a smaller HLC would resurrect the cell
  and the fold would lose its commutativity property — the same set
  of events would converge to different states depending on arrival
  order.
- Once `dead`, the cell is terminal: no event resurrects it. Late
  events still bump the dead HLC so the cell's `last_modified_hlc`
  reflects everything we've seen.

## Conflicts

None. Keys are unique-by-construction; this fold has no conflict path.
Same-HLC creates with different payloads from independent writers are
outside the fold's input contract.

## GC

`gc_threshold(State) == hlc(State)` for terminal states (live, dead).
For `empty`, `gc_threshold = undefined`. Any event with `hlc <= threshold`
has already been absorbed into the state (or is older than what the
state observed) and is safe to drop from the WAL.

## Encoding

```
empty       -> <<0>>
{live, H, P} -> <<1, H:64/big-unsigned, PSize:32/big-unsigned, P/binary>>
{dead, H}    -> <<3, H:64/big-unsigned>>
```

Event encoding parallels state encoding with a leading tag.

The payload is **opaque** to this fold — application code provides
already-serialised binaries.
""").

-export([initial_value/0]).
-export([apply_event/2]).
-export([hlc/1]).
-export([gc_threshold/1]).
-export([encode_state/1]).
-export([decode_state/1]).
-export([encode_event/1]).
-export([decode_event/1]).

-type payload() :: binary().
-type state() ::
        empty
        | {live, bondy_oplog_hlc:hlc(), payload()}
        | {dead, bondy_oplog_hlc:hlc()}.

-type event() ::
        {create, bondy_oplog_hlc:hlc(), payload()}
        | {delete, bondy_oplog_hlc:hlc()}.

-export_type([state/0, event/0, payload/0]).

%% =============================================================================
%% CALLBACKS
%% =============================================================================

-spec initial_value() -> state().

initial_value() ->
    empty.


-spec apply_event(state(), event()) -> state().

apply_event(empty, {create, H, P}) ->
    {live, H, P};

apply_event(empty, {delete, H}) ->
    %% Delete arriving before its corresponding create — record a
    %% tombstone so a later-arriving create with smaller HLC cannot
    %% silently resurrect the cell.
    {dead, H};

apply_event({live, OldH, _OldP} = S, {create, H, _}) when H < OldH ->
    %% Older-HLC create on live state — out-of-order, rejected.
    S;

apply_event({live, _, _}, {create, H, P}) ->
    %% H >= OldH: idempotent absorb (same HLC) or supersede.
    {live, H, P};

apply_event({live, OldH, _}, {delete, H}) ->
    %% Delete moves the cell to terminal `dead`. Preserve HLC
    %% monotonicity even if the delete is causally older.
    {dead, erlang:max(OldH, H)};

apply_event({dead, OldH}, {create, H, _}) ->
    %% Terminal — do not resurrect. Bump the cell HLC to reflect that
    %% we have observed an event with this HLC.
    {dead, erlang:max(OldH, H)};

apply_event({dead, OldH}, {delete, H}) ->
    %% Terminal — bump HLC on duplicate or late-arriving delete.
    {dead, erlang:max(OldH, H)}.


-spec hlc(state()) -> bondy_oplog_hlc:hlc().

hlc(empty)         -> 0;
hlc({live, H, _})  -> H;
hlc({dead, H})     -> H.


-spec gc_threshold(state()) -> bondy_oplog_hlc:hlc() | undefined.

gc_threshold(empty)        -> undefined;
gc_threshold({live, H, _}) -> H;
gc_threshold({dead, H})    -> H.


-spec encode_state(state()) -> binary().

encode_state(empty) ->
    <<0>>;

encode_state({live, H, P}) when is_integer(H), is_binary(P) ->
    PSize = byte_size(P),
    <<1, H:64/big-unsigned, PSize:32/big-unsigned, P/binary>>;

encode_state({dead, H}) when is_integer(H) ->
    <<3, H:64/big-unsigned>>.


-spec decode_state(binary()) -> state().

decode_state(<<0>>) ->
    empty;

decode_state(<<1, H:64/big-unsigned, PSize:32/big-unsigned, P:PSize/binary>>) ->
    {live, H, P};

decode_state(<<3, H:64/big-unsigned>>) ->
    {dead, H}.


-spec encode_event(event()) -> binary().

encode_event({create, H, P}) when is_integer(H), is_binary(P) ->
    PSize = byte_size(P),
    <<1, H:64/big-unsigned, PSize:32/big-unsigned, P/binary>>;

encode_event({delete, H}) when is_integer(H) ->
    <<2, H:64/big-unsigned>>.


-spec decode_event(binary()) -> event().

decode_event(<<1, H:64/big-unsigned, PSize:32/big-unsigned, P:PSize/binary>>) ->
    {create, H, P};

decode_event(<<2, H:64/big-unsigned>>) ->
    {delete, H}.

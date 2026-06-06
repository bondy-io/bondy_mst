%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_projection_leveled).

-include("bondy_mst.hrl").
-include_lib("leveled/include/leveled.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
`bondy_oplog_projection_adapter` implementation backed by a leveled
Bookie running in **`head_only` mode** with a SubKey split.

This adapter is a **pure mapper**: it owns no Bookie process, no
supervision, no path layout. It receives an already-opened Bookie pid
via `open/4`'s `Opts` and translates the substrate's seven callbacks
into the corresponding `leveled_bookie` calls.

Bookie lifecycle (start, stop, supervision, path layout, refcounting)
is the caller's concern — the consumer-facing `bondy_db` layer above
the substrate is where those decisions live. The Bookie **must** be
opened with `{head_only, with_lookup}` for this adapter to function;
see `bondy_db_topology_*:default_book_opts/1` for the canonical opts.

## SubKey split

Each logical cell `(Bucket, Key)` is stored as **two** leveled HEAD
entries under the `?HEAD_TAG`, distinguished by SubKey:

| SubKey    | Payload (binary)                                              |
|-----------|---------------------------------------------------------------|
| `?SK_STATE` (`<<"s">>`) | `<<HlcLen:16, Hlc/binary, StateBytes/binary>>` |
| `?SK_VALUE` (`<<"v">>`) | `<<HlcLen:16, Hlc/binary, ValueBytes/binary>>` (HEAD wire format) |

Both subkeys carry the HLC so each can be decoded independently.
For folds that declare `value_equals_state/0 -> true` (currently
only G-Set) the value subkey stores a copy of `StateBytes`; the
duplication is a minor space cost in exchange for uniform read
semantics.

## Why head_only mode

Two material wins over the previous `?BONDY_FOLD_TAG` normal-mode
setup:

1. **Atomic batched writes via `book_mput/2`**. The applier's
   `apply_cell_batch/2` collects all per-event writes into a single
   `put_batch/2` call; the adapter then translates that into one
   `book_mput` ObjectSpec list (two specs per cell — `?SK_STATE` +
   `?SK_VALUE`) and ships it to leveled atomically. Previous setup
   required one `book_put` gen_server roundtrip per cell write.
2. **Ledger-only reads**. In `head_only` mode the journal carries
   no body — the entire value is in the LSM HEAD entry. `book_get`
   becomes equivalent to `book_headonly` (no journal hop), so the
   apply path's read of OldState drops from ~1.7 ms (journal seek)
   to <100 µs (ledger lookup). See PR-PS-15a measurement.

## Bucket is call-time

In line with the projection adapter behaviour, every data callback
takes `Bucket` as an argument and forwards it to `leveled_bookie`.
The handle is just the Bookie pid — one handle serves every Bucket
inside the shard.

## Handle shape

```erlang
#{bookie := pid()}
```

## Required `Opts` for `open/4`

| Key | Type | Meaning |
|---|---|---|
| `bookie` | `pid()` | The leveled Bookie this `(NS, Index, Shard)` writes to |

Anything else in `Opts` is ignored.

## Range bounds

Leveled's `book_keylist/5` range is **inclusive** on both ends; the
substrate contract is `[Low, High)` (half-open on the high side).
The fold function below excludes any composite key whose underlying
Key matches the High sentinel. Within each Key, only the
`?SK_VALUE` SubKey is enumerated for the range — the state subkey
is fetched on demand to reconstruct the full V2 frame.

## What this adapter does NOT do

- Open, stop, or supervise the Bookie.
- Path management, journal/ledger directory creation, recovery.
- Routing or topology decisions.
- Reuse the legacy `?BONDY_FOLD_TAG` extractor at
  `bondy_oplog_leveled_tag` — head_only mode bypasses extractors
  entirely (HEAD bytes are written directly via `book_mput`).
""").

-behaviour(bondy_oplog_projection_adapter).

-export([
    open/4,
    close/1,
    get/3,
    head/3,
    put_batch/2,
    range/5,
    delete/3,
    info/1
]).

-define(SK_STATE, <<"s">>).
-define(SK_VALUE, <<"v">>).

%% Lexicographically minimal/maximal SubKey sentinels for prefix scans
%% over a single Key (encompasses ?SK_STATE and ?SK_VALUE).
-define(SK_LOW, <<>>).
-define(SK_HIGH, <<255, 255, 255, 255>>).

-type handle() :: #{bookie := pid()}.

%% =============================================================================
%% API
%% =============================================================================

-spec open(
    Namespace :: atom(),
    Index :: atom(),
    Shard :: non_neg_integer(),
    Opts :: map()
) -> {ok, handle()} | {error, term()}.

open(_NS, _Index, _Shard, #{bookie := Pid} = _Opts) when is_pid(Pid) ->
    {ok, #{bookie => Pid}};
open(_NS, _Index, _Shard, Opts) when is_map(Opts) ->
    {error, {invalid_opts, Opts}}.

-spec close(handle()) -> ok.

close(#{bookie := _Pid}) ->
    ok.

-doc """
Full-cell read. Returns the V2 cell frame reconstructed from both
subkeys (state + value). Used by the applier's `apply_one_cell/11`
which needs both OldState and OldValueOpt.

Two `book_headonly/4` calls (ledger-only, no journal hop). On
not-found in either subkey returns `not_found` — the cell is treated
as absent (this is the same semantics as a key never having been
written; corruption that leaves only one subkey is logged elsewhere
and surfaces here as not_found).
""".
-spec get(handle(), Bucket :: binary(), Key :: binary()) ->
    {ok, Frame :: binary()} | not_found.

get(#{bookie := Pid}, Bucket, Key) when
    is_binary(Bucket), is_binary(Key)
->
    case read_state_subkey(Pid, Bucket, Key) of
        not_found ->
            not_found;
        {ok, Hlc, StateBytes} ->
            %% Value subkey is absent when the source frame had
            %% `HasValueColumn=0` (value_equals_state folds) — we
            %% deliberately don't write it on put_batch in that
            %% case. Reconstruct with the same flag the original
            %% encode/4 used.
            case read_value_subkey(Pid, Bucket, Key) of
                not_found ->
                    {ok,
                        bondy_oplog_cell_frame:encode(
                            Hlc, StateBytes, undefined, true
                        )};
                {ok, _Hlc, ValueBytes} ->
                    {ok,
                        bondy_oplog_cell_frame:encode(
                            Hlc, StateBytes, ValueBytes, false
                        )}
            end
    end.

-doc """
HEAD fast-path read. Returns the value subkey's payload as-is — it
**is** the HEAD wire format
(`<<HlcLen:16, HlcBin:HlcLen/binary, ValueBytes/binary>>`). One
`book_headonly/4` call.

This is the optional `head/3` callback on
`bondy_oplog_projection_adapter`; substrates that lack a native HEAD
mechanism can skip the export and let the caller fall back to
`get/3 + bondy_oplog_cell_frame:extract_head/1`.
""".
-spec head(handle(), Bucket :: binary(), Key :: binary()) ->
    {ok, HeadBytes :: binary()} | not_found.

head(#{bookie := Pid}, Bucket, Key) when
    is_binary(Bucket), is_binary(Key)
->
    case leveled_bookie:book_headonly(Pid, Bucket, Key, ?SK_VALUE) of
        {ok, HeadBytes} ->
            {ok, HeadBytes};
        not_found ->
            %% No value subkey → this cell was written by a
            %% value_equals_state fold (only state subkey exists).
            %% The state subkey payload IS the HEAD wire format
            %% (StateBytes doubles as ValueBytes for these folds).
            case leveled_bookie:book_headonly(Pid, Bucket, Key, ?SK_STATE) of
                {ok, HeadBytes} -> {ok, HeadBytes};
                not_found -> not_found
            end
    end.

-doc """
Batched cell write. Decodes each V2 frame into `{Hlc, State, Value}`,
builds two `book_mput` ObjectSpecs per entry (one for `?SK_STATE`,
one for `?SK_VALUE`), and ships them all to leveled in a single
atomic `book_mput/2` call.

The caller is expected to have already coalesced per-batch writes
(see `bondy_oplog_applier:apply_cell_batch/2`) so this function
typically receives N entries and issues ONE gen_server roundtrip.
""".
-spec put_batch(
    handle(),
    [{Bucket :: binary(), Key :: binary(), Frame :: binary()}]
) -> ok | {error, term()}.

put_batch(_Handle, []) ->
    ok;
put_batch(#{bookie := Pid}, Entries) when is_list(Entries) ->
    ObjectSpecs = build_object_specs(Entries, []),
    case leveled_bookie:book_mput(Pid, ObjectSpecs) of
        ok -> ok;
        pause -> ok
    end.

-doc """
Range read over the value subkeys. Returns up to `Limit`
`{Key, Frame}` pairs in the requested direction. Each `Frame` is the
reconstructed V2 frame (one extra `book_headonly` per result to fetch
the matching state subkey).

The high bound is **exclusive** (substrate contract); the
keylist fold below filters out the matching key.

`High` may be the atom `infinity` for an open-ended scan (every value
subkey `>= Low` in the bucket) — the form the secondary-index
primary-scan fallback (IDX-4) uses. It folds the whole bucket
(`book_keylist/4`) rather than a bounded `KeyRange`.
""".
-spec range(
    handle(),
    Bucket :: binary(),
    Low :: binary(),
    High :: binary() | infinity,
    Opts :: bondy_oplog_projection_adapter:range_opts()
) -> {ok, [{Key :: binary(), Frame :: binary()}]} | {error, term()}.

range(#{bookie := Pid}, Bucket, Low, High, Opts) when
    is_binary(Bucket),
    is_binary(Low),
    (is_binary(High) orelse High =:= infinity),
    is_map(Opts)
->
    Limit = maps:get(limit, Opts, 1000),
    Direction = maps:get(direction, Opts, asc),
    {async, Folder} =
        case High of
            infinity ->
                %% Whole-bucket fold from Low with no upper bound.
                FoldFun0 = make_value_keylist_fold_open(Limit, Low),
                leveled_bookie:book_keylist(
                    Pid, ?HEAD_TAG, Bucket, {FoldFun0, {0, []}}
                );
            _ ->
                %% Range over the {Key, SubKey} composite that brackets
                %% every value subkey between Low and High.
                KeyRange = {{Low, ?SK_VALUE}, {High, ?SK_VALUE}},
                FoldFun1 = make_value_keylist_fold(Limit, High),
                leveled_bookie:book_keylist(
                    Pid, ?HEAD_TAG, Bucket, KeyRange, {FoldFun1, {0, []}}
                )
        end,
    {_N, KeysRev} =
        try
            Folder()
        catch
            throw:{limit_reached, S} -> S
        end,
    KeysAsc = lists:reverse(KeysRev),
    %% Fetch the state subkey for each key found, reconstruct the V2
    %% frame. Returns [] if Reader fails on any key (treats as not_found).
    Pairs = [
        {K, F}
     || K <- KeysAsc,
        {ok, F} <- [get(#{bookie => Pid}, Bucket, K)]
    ],
    case Direction of
        asc -> {ok, Pairs};
        desc -> {ok, lists:reverse(Pairs)}
    end.

-doc """
Delete both subkeys for `(Bucket, Key)` atomically via `book_mput/2`
with `remove` ops.
""".
-spec delete(handle(), Bucket :: binary(), Key :: binary()) -> ok.

delete(#{bookie := Pid}, Bucket, Key) when
    is_binary(Bucket), is_binary(Key)
->
    ObjectSpecs = [
        {remove, Bucket, Key, ?SK_STATE, null},
        {remove, Bucket, Key, ?SK_VALUE, null}
    ],
    case leveled_bookie:book_mput(Pid, ObjectSpecs) of
        ok -> ok;
        pause -> ok
    end.

-spec info(handle()) -> #{atom() => term()}.

info(#{bookie := Pid}) ->
    #{
        backend => leveled,
        bookie => Pid,
        tag => ?HEAD_TAG,
        subkey_state => ?SK_STATE,
        subkey_value => ?SK_VALUE
    }.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% Read the state subkey, returning {ok, Hlc, StateBytes} | not_found.
read_state_subkey(Pid, Bucket, Key) ->
    case leveled_bookie:book_headonly(Pid, Bucket, Key, ?SK_STATE) of
        {ok, <<HlcLen:16/big-unsigned, Hlc:HlcLen/binary, StateBytes/binary>>} ->
            HlcInt = binary:decode_unsigned(Hlc, big),
            {ok, HlcInt, StateBytes};
        not_found ->
            not_found
    end.

%% Read the value subkey, returning {ok, Hlc, ValueBytes} | not_found.
read_value_subkey(Pid, Bucket, Key) ->
    case leveled_bookie:book_headonly(Pid, Bucket, Key, ?SK_VALUE) of
        {ok, <<HlcLen:16/big-unsigned, Hlc:HlcLen/binary, ValueBytes/binary>>} ->
            HlcInt = binary:decode_unsigned(Hlc, big),
            {ok, HlcInt, ValueBytes};
        not_found ->
            not_found
    end.

%% Turn an [{Bucket, Key, Frame}] list into a flat [ObjectSpec] list
%% suitable for book_mput.
%%
%% For frames with `HasValueColumn=1` we emit BOTH subkeys (state +
%% value). For frames with `HasValueColumn=0` (value_equals_state
%% folds) we emit ONLY the state subkey — the value subkey absence is
%% the signal on read that the cell was written by a
%% value_equals_state fold. See `get/3` and `head/3` for the
%% read-side handling.
build_object_specs([], Acc) ->
    lists:reverse(Acc);
build_object_specs([{Bucket, Key, Frame} | Rest], Acc) ->
    {Hlc, StateBytes, ValueBytesOpt} =
        bondy_oplog_cell_frame:decode_full(Frame),
    HlcBin = <<Hlc:64/big-unsigned>>,
    HlcLen = byte_size(HlcBin),
    StatePayload = <<HlcLen:16/big-unsigned, HlcBin/binary, StateBytes/binary>>,
    Acc1 = [{add, Bucket, Key, ?SK_STATE, StatePayload} | Acc],
    Acc2 =
        case ValueBytesOpt of
            undefined ->
                Acc1;
            ValueBytes ->
                ValuePayload =
                    <<HlcLen:16/big-unsigned, HlcBin/binary,
                        ValueBytes/binary>>,
                [{add, Bucket, Key, ?SK_VALUE, ValuePayload} | Acc1]
        end,
    build_object_specs(Rest, Acc2).

%% Fold fun for keylist over `{Key, ?SK_VALUE}` composite keys.
%% Accumulates Keys (deduped by the SubKey == ?SK_VALUE filter) up to
%% Limit; excludes any Key matching the High sentinel (half-open range).
make_value_keylist_fold(Limit, High) ->
    fun(_B, {K, SubKey}, {N, Items}) ->
        case SubKey of
            ?SK_VALUE when K =/= High ->
                N1 = N + 1,
                State = {N1, [K | Items]},
                case N1 >= Limit of
                    true -> throw({limit_reached, State});
                    false -> State
                end;
            _ ->
                {N, Items}
        end
    end.

%% Open-ended (`High =:= infinity`) variant: a whole-bucket fold keeps
%% only value subkeys whose key is `>= Low`, capped at `Limit`.
make_value_keylist_fold_open(Limit, Low) ->
    fun(_B, {K, SubKey}, {N, Items}) ->
        case SubKey of
            ?SK_VALUE when K >= Low ->
                N1 = N + 1,
                State = {N1, [K | Items]},
                case N1 >= Limit of
                    true -> throw({limit_reached, State});
                    false -> State
                end;
            _ ->
                {N, Items}
        end
    end.

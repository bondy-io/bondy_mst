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
Bookie.

This adapter is a **pure mapper**: it owns no Bookie process, no
supervision, no path layout. It receives an already-opened Bookie pid
via `open/4`'s `Opts` and translates the substrate's seven callbacks
into the corresponding `leveled_bookie` calls.

Bookie lifecycle (start, stop, supervision, path layout, refcounting)
is the caller's concern — the consumer-facing `bondy_db` layer above
the substrate is where those decisions live.

## Bucket is call-time

In line with the projection adapter behaviour, every data callback
takes `Bucket` as an argument and forwards it to `leveled_bookie`.
The handle is just the Bookie pid — one handle serves every Bucket
inside the shard. This matches leveled's native `(Bucket, Key)`
addressing, so new Buckets need no adapter ceremony.

## Handle shape

```erlang
#{bookie := pid()}
```

## Required `Opts` for `open/4`

| Key | Type | Meaning |
|---|---|---|
| `bookie` | `pid()` | The leveled Bookie this `(NS, Index, Shard)` writes to |

Anything else in `Opts` is ignored.

## Encoding choices

- **Tag** — `?BONDY_FOLD_TAG` (atom `o_fold`). Activates
  `bondy_oplog_leveled_tag`'s `extract_metadata/3` + `build_head/2`
  hooks so `book_head/4` returns the HEAD wire format
  (`<<HlcLen:16, Hlc/binary, ValueBytes/binary>>`) directly from the
  ledger without a full-object fetch. See
  `_design/catalogue_expansion_plan.md` §3.4.
- **Bucket** — passthrough; must be binary (leveled enforces this).
- **Key** — passthrough; must be binary.
- **Value** — passthrough.
- **Range bounds** — leveled's `book_objectfold/6` range is **inclusive**
  on both ends; the substrate contract is `[Low, High)` (half-open on
  the high side). The fold function below excludes `K =:= High` to
  bridge the two.

## What this adapter does NOT do

- Open, stop, or supervise the Bookie.
- Path management, journal/ledger directory creation, recovery.
- Routing or topology decisions.
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


-spec get(handle(), Bucket :: binary(), Key :: binary()) ->
    {ok, Frame :: binary()} | not_found.

get(#{bookie := Pid}, Bucket, Key)
        when is_binary(Bucket), is_binary(Key) ->
    case leveled_bookie:book_get(Pid, Bucket, Key, ?BONDY_FOLD_TAG) of
        {ok, Frame}     -> {ok, Frame};
        not_found       -> not_found
    end.


-doc """
HEAD fast-path read. Returns the HEAD wire format
(`<<HlcLen:16, Hlc/binary, ValueBytes/binary>>`) without fetching the
full V2 frame from the journal — the bytes are reconstructed in-ledger
by `bondy_oplog_leveled_tag:build_head/2` from the metadata that the
extractor stashed at write time.

This is the optional `head/3` callback on
`bondy_oplog_projection_adapter` — substrates that lack a native HEAD
mechanism can skip the export and let the caller fall back to
`get/3 + bondy_oplog_cell_frame:extract_head/1`.
""".
-spec head(handle(), Bucket :: binary(), Key :: binary()) ->
    {ok, HeadBytes :: binary()} | not_found.

head(#{bookie := Pid}, Bucket, Key)
        when is_binary(Bucket), is_binary(Key) ->
    case leveled_bookie:book_head(Pid, Bucket, Key, ?BONDY_FOLD_TAG) of
        {ok, HeadBytes} -> {ok, HeadBytes};
        not_found       -> not_found
    end.


-spec put_batch(
    handle(),
    [{Bucket :: binary(), Key :: binary(), Frame :: binary()}]
) -> ok | {error, term()}.

put_batch(#{bookie := Pid}, Entries) when is_list(Entries) ->
    do_put_batch(Pid, Entries).


-spec range(
    handle(),
    Bucket :: binary(),
    Low :: binary(),
    High :: binary(),
    Opts :: bondy_oplog_projection_adapter:range_opts()
) -> {ok, [{Key :: binary(), Frame :: binary()}]} | {error, term()}.

range(#{bookie := Pid}, Bucket, Low, High, Opts)
        when is_binary(Bucket), is_binary(Low), is_binary(High),
             is_map(Opts) ->
    Limit = maps:get(limit, Opts, 1000),
    Direction = maps:get(direction, Opts, asc),
    FoldFun = make_range_fold_fun(Limit, High),
    {async, Folder} = leveled_bookie:book_objectfold(
        Pid, ?BONDY_FOLD_TAG, Bucket, {Low, High}, {FoldFun, {0, []}}, true
    ),
    {_N, AccRev} = try Folder() catch
        throw:{limit_reached, State} -> State
    end,
    Asc = lists:reverse(AccRev),
    case Direction of
        asc  -> {ok, Asc};
        desc -> {ok, lists:reverse(Asc)}
    end.


-spec delete(handle(), Bucket :: binary(), Key :: binary()) -> ok.

delete(#{bookie := Pid}, Bucket, Key)
        when is_binary(Bucket), is_binary(Key) ->
    %% `leveled_bookie:book_delete/4` hardcodes `?STD_TAG`, so we go
    %% direct to `book_put/6` with the `delete` tombstone payload to
    %% target `?BONDY_FOLD_TAG`.
    case leveled_bookie:book_put(
        Pid, Bucket, Key, delete, [], ?BONDY_FOLD_TAG
    ) of
        ok      -> ok;
        pause   -> ok
    end.


-spec info(handle()) -> #{atom() => term()}.

info(#{bookie := Pid}) ->
    #{
        backend => leveled,
        bookie => Pid,
        tag => ?BONDY_FOLD_TAG
    }.


%% =============================================================================
%% PRIVATE
%% =============================================================================

do_put_batch(_Pid, []) ->
    ok;

do_put_batch(Pid, [{Bucket, Key, Frame} | Rest])
        when is_binary(Bucket), is_binary(Key), is_binary(Frame) ->
    case leveled_bookie:book_put(
        Pid, Bucket, Key, Frame, [], ?BONDY_FOLD_TAG
    ) of
        ok      -> do_put_batch(Pid, Rest);
        pause   -> do_put_batch(Pid, Rest)
    end.


make_range_fold_fun(Limit, High) ->
    fun(_B, K, V, {N, Items}) ->
        case K =:= High of
            true ->
                %% Half-open: substrate's contract is [Low, High); leveled
                %% gave us K =:= High because its range is inclusive.
                {N, Items};
            false ->
                N1 = N + 1,
                State = {N1, [{K, V} | Items]},
                case N1 >= Limit of
                    true  -> throw({limit_reached, State});
                    false -> State
                end
        end
    end.

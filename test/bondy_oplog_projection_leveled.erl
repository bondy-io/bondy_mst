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
Bookie (`MST_DB_DESIGN.md` §6, §18 item 10).

This adapter is a **pure mapper**: it owns no Bookie process, no
supervision, no path layout. It receives an already-opened Bookie pid
and a bucket binary via `open/4`'s `Opts` and translates the substrate's
seven callbacks into the corresponding `leveled_bookie` calls.

Bookie lifecycle (start, stop, supervision, path layout, refcounting)
is the caller's concern — the consumer-facing `bondy_db` layer above the
substrate is where those decisions live. The substrate only sees this
adapter through its behaviour, and the adapter only sees leveled through
its API; nothing here knows about realms, entity types, or shards.

## Handle shape

```erlang
#{
    bookie  := pid(),       %% already-running Bookie process
    bucket  := binary()     %% leveled bucket; consumer chooses what goes here
                            %% (e.g., the realm name for the bucket-per-realm
                            %% topology, or a fixed constant for per-(NS,
                            %% Index, Shard) Bookies)
}
```

## Required `Opts` for `open/4`

| Key | Type | Meaning |
|---|---|---|
| `bookie` | `pid()` | The leveled Bookie this `(NS, Index, Shard)` writes to |
| `bucket` | `binary()` | The leveled Bucket inside that Bookie |

Anything else in `Opts` is ignored.

## Encoding choices

- **Tag** — `?STD_TAG` (leveled's general-purpose object tag, atom `o`).
  Custom tags require codec extensions for the index-fold / head-fold
  paths we do not use; staying on the standard tag avoids that surface.
- **Key** — passthrough. The WAL codec already constrains substrate
  keys to fixed-length binaries, so no encoding step is required.
  A runtime `is_binary/1` guard catches accidents.
- **Value** — passthrough. `bondy_oplog_cell_frame:encode/2` is the
  canonical wire format; leveled stores the frame bytes verbatim.
- **Range bounds** — leveled's `book_objectfold/6` range is **inclusive**
  on both ends; the substrate contract is `[Low, High)` (half-open on
  the high side). The fold function below excludes `K =:= High` to
  bridge the two.

## Performance caveats

- `put_batch/2` issues sequential `book_put/5` calls, one per entry.
  Leveled has no batched-journal API for non-head-only stores
  (`book_mput/2` is restricted to head-only mode). Substrate batches
  already serialise per shard, so the journal-append cost is paid
  per entry inside one applier call. The Inker buffers writes and the
  sync strategy is controlled by the Bookie's `book_start/4` options;
  `put_batch/2` itself does not tune them.
- `range/4` runs an asynchronous fold with early-exit via `throw` once
  the per-call limit is reached. Snapshot is taken at fold-time
  (`SnapPreFold = true`).
- `book_put/5` may return `pause` under back-pressure; this adapter
  treats `pause` as success but does not back off — callers that need
  back-pressure handling should consult `info/1` or watch leveled's own
  telemetry.

## What this adapter does NOT do

- Open, stop, or supervise the Bookie.
- Path management, journal/ledger directory creation, recovery.
- Routing or topology decisions (which realm/entity type/shard goes to
  which Bookie). Those live in `bondy_db`.
- Index folds, head folds, secondary indexes. The substrate calls
  only `get/2`, `put_batch/2`, `range/4`, and `delete/2` on the hot
  path; the adapter implements those four plus the three lifecycle
  callbacks.
""").

-behaviour(bondy_oplog_projection_adapter).

-export([
    open/4,
    close/1,
    get/2,
    put_batch/2,
    range/4,
    delete/2,
    info/1
]).

-type handle() :: #{bookie := pid(), bucket := binary()}.

%% =============================================================================
%% API
%% =============================================================================

-spec open(
    Namespace :: atom(),
    Index :: atom(),
    Shard :: non_neg_integer(),
    Opts :: map()
) -> {ok, handle()} | {error, term()}.

open(_NS, _Index, _Shard, #{bookie := Pid, bucket := Bucket} = _Opts)
        when is_pid(Pid), is_binary(Bucket) ->
    {ok, #{bookie => Pid, bucket => Bucket}};

open(_NS, _Index, _Shard, Opts) when is_map(Opts) ->
    {error, {invalid_opts, Opts}}.


-spec close(handle()) -> ok.

close(#{bookie := _Pid}) ->
    %% The adapter does not own the Bookie; closing the handle is a
    %% no-op. Bookie shutdown is the caller's responsibility.
    ok.


-spec get(handle(), Key :: binary()) ->
    {ok, Frame :: binary()} | not_found.

get(#{bookie := Pid, bucket := Bucket}, Key) when is_binary(Key) ->
    case leveled_bookie:book_get(Pid, Bucket, Key, ?STD_TAG) of
        {ok, Frame}     -> {ok, Frame};
        not_found       -> not_found
    end.


-spec put_batch(handle(), [{Key :: binary(), Frame :: binary()}]) ->
    ok | {error, term()}.

put_batch(#{bookie := Pid, bucket := Bucket}, Entries) when is_list(Entries) ->
    do_put_batch(Pid, Bucket, Entries).


-spec range(
    handle(),
    Low :: binary(),
    High :: binary(),
    Opts :: bondy_oplog_projection_adapter:range_opts()
) -> {ok, [{Key :: binary(), Frame :: binary()}]} | {error, term()}.

range(#{bookie := Pid, bucket := Bucket}, Low, High, Opts)
        when is_binary(Low), is_binary(High), is_map(Opts) ->
    Limit = maps:get(limit, Opts, 1000),
    Direction = maps:get(direction, Opts, asc),
    FoldFun = make_range_fold_fun(Limit, High),
    {async, Folder} = leveled_bookie:book_objectfold(
        Pid, ?STD_TAG, Bucket, {Low, High}, {FoldFun, {0, []}}, true
    ),
    {_N, AccRev} = try Folder() catch
        throw:{limit_reached, State} -> State
    end,
    Asc = lists:reverse(AccRev),
    case Direction of
        asc  -> {ok, Asc};
        desc -> {ok, lists:reverse(Asc)}
    end.


-spec delete(handle(), Key :: binary()) -> ok.

delete(#{bookie := Pid, bucket := Bucket}, Key) when is_binary(Key) ->
    case leveled_bookie:book_delete(Pid, Bucket, Key, []) of
        ok      -> ok;
        pause   -> ok
    end.


-spec info(handle()) -> #{atom() => term()}.

info(#{bookie := Pid, bucket := Bucket}) ->
    #{
        backend => leveled,
        bookie => Pid,
        bucket => Bucket,
        tag => ?STD_TAG
    }.


%% =============================================================================
%% PRIVATE
%% =============================================================================

do_put_batch(_Pid, _Bucket, []) ->
    ok;

do_put_batch(Pid, Bucket, [{Key, Frame} | Rest])
        when is_binary(Key), is_binary(Frame) ->
    case leveled_bookie:book_put(Pid, Bucket, Key, Frame, []) of
        ok      -> do_put_batch(Pid, Bucket, Rest);
        pause   -> do_put_batch(Pid, Bucket, Rest)
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

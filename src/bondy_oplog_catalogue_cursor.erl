%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_catalogue_cursor).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("bondy_doc.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Node-shared cursor table for catalogue-snapshot bootstrap sessions.

The bootstrap protocol pulls a peer's catalogue projection in batches
(`_design/catalogue_expansion_plan.md` §4.12). Each session is paged on
the peer side by an opaque cursor; this module owns the cursor table.

## Lifecycle

1. **`mint/5`** — the peer-side responder mints a cursor at the start
   of a bootstrap session. The cursor captures the
   `(instance, ns, index, shard, bucket, watermark)` and an initial
   `last_key = undefined` (start from the lowest key in the bucket).
2. **`lookup/1`** — the responder resolves the cursor on each
   `get_catalogue_snapshot_next` request.
3. **`advance/2`** — after returning a batch the responder advances
   `last_key` to the last-returned key. Expiry deadline is bumped on
   every advance so an in-flight session never times out under its own
   activity.
4. **`discard/1`** — explicit cleanup when the responder hits end of
   keyspace; the cursor row is dropped immediately.

Expired cursors are lazily reaped on access (a `lookup/1` of an
expired row returns `expired` *and* deletes the row). A periodic sweep
also runs every `?GC_INTERVAL_MS` to bound table growth when sessions
abandon their cursors silently.

## Concurrency

The ETS table is `public` so `mint/5`, `lookup/1`, `advance/2`, and
`discard/1` are direct ETS ops — no gen_server roundtrip per batch.
This keeps multiple bootstrap sessions on the same peer fully parallel.
The owning gen_server only creates the table on init and runs the
periodic GC sweep.

## Failure mode

Cursors live only in memory. If the peer crashes mid-bootstrap the
initiator's next request finds `not_found` and must retry from
`get_catalogue_snapshot_init`. This is the correct semantics — the
snapshot is a moving target across peer restarts.
""").

%% Public ETS table; rows are direct-write from the responder so
%% multiple in-flight sessions do not serialise on the gen_server.
-define(TABLE, bondy_oplog_catalogue_cursor_tab).

%% Cursor lifetime budget per individual request. Refreshed on every
%% `advance/2`. A bootstrap session that pulls a batch every < 60s
%% never expires; a session that stalls is reclaimed.
-define(DEFAULT_TTL_MS, 60_000).

%% Periodic full-table sweep. Cheap — `ets:select_delete/2` with a
%% match-spec, no copying.
-define(GC_INTERVAL_MS, 30_000).

-record(cursor, {
    cursor :: cursor(),
    instance_id :: instance_id(),
    ns :: atom(),
    index :: atom(),
    shard :: non_neg_integer(),
    bucket :: binary(),
    last_key :: undefined | binary(),
    watermark :: non_neg_integer(),
    expires_at :: integer()
}).

-record(state, {
    ttl_ms :: non_neg_integer()
}).

-type cursor() :: binary().
-type cursor_state() :: #{
    instance_id := instance_id(),
    ns := atom(),
    index := atom(),
    shard := non_neg_integer(),
    bucket := binary(),
    last_key := undefined | binary(),
    watermark := non_neg_integer()
}.

-export_type([cursor/0]).
-export_type([cursor_state/0]).

%% Lifecycle
-export([start_link/0]).
-export([start_link/1]).
-export([child_spec/0]).
-export([child_spec/1]).

%% Cursor API (direct ETS, no gen_server roundtrip)
-export([mint/6]).
-export([lookup/1]).
-export([advance/2]).
-export([discard/1]).
-export([info/0]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

%% =============================================================================
%% LIFECYCLE
%% =============================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.

start_link() ->
    start_link(#{}).

-spec start_link(map()) -> {ok, pid()} | {error, term()}.

start_link(Opts) when is_map(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

-spec child_spec() -> supervisor:child_spec().

child_spec() ->
    child_spec(#{}).

-spec child_spec(map()) -> supervisor:child_spec().

child_spec(Opts) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Opts]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

%% =============================================================================
%% CURSOR API
%% =============================================================================

?DOC("""
Mints a new cursor for a catalogue-snapshot session against the given
shard and bucket. Returns the opaque cursor token, which the caller
returns to the peer in subsequent `get_catalogue_snapshot_next`
requests.
""").
-spec mint(
    instance_id(),
    NS :: atom(),
    Index :: atom(),
    Shard :: non_neg_integer(),
    Bucket :: binary(),
    Watermark :: non_neg_integer()
) -> cursor().

mint(InstanceId, NS, Index, Shard, Bucket, Watermark) when
    is_binary(InstanceId),
    is_atom(NS),
    is_atom(Index),
    is_integer(Shard),
    Shard >= 0,
    is_binary(Bucket),
    is_integer(Watermark),
    Watermark >= 0
->
    Cursor = crypto:strong_rand_bytes(16),
    Row = #cursor{
        cursor = Cursor,
        instance_id = InstanceId,
        ns = NS,
        index = Index,
        shard = Shard,
        bucket = Bucket,
        last_key = undefined,
        watermark = Watermark,
        expires_at = erlang:monotonic_time(millisecond) + ttl_ms()
    },
    true = ets:insert(?TABLE, Row),
    Cursor.

?DOC("""
Resolves a cursor. Expired rows are eagerly deleted and reported as
`expired`; unknown cursors return `not_found`. A successful lookup
returns the cursor's current state as a map.
""").
-spec lookup(cursor()) -> {ok, cursor_state()} | expired | not_found.

lookup(Cursor) when is_binary(Cursor) ->
    case ets:lookup(?TABLE, Cursor) of
        [] ->
            not_found;
        [#cursor{expires_at = ExpiresAt} = Row] ->
            Now = erlang:monotonic_time(millisecond),
            case Now >= ExpiresAt of
                true ->
                    true = ets:delete(?TABLE, Cursor),
                    expired;
                false ->
                    {ok, row_to_map(Row)}
            end
    end.

?DOC("""
Advances `last_key` for the cursor and refreshes its expiry deadline.
Returns `not_found` if the cursor was reaped or never existed.
""").
-spec advance(cursor(), NewLastKey :: binary()) -> ok | not_found.

advance(Cursor, NewLastKey) when
    is_binary(Cursor), is_binary(NewLastKey)
->
    Now = erlang:monotonic_time(millisecond),
    NewExpiresAt = Now + ttl_ms(),
    Updates = [
        {#cursor.last_key, NewLastKey},
        {#cursor.expires_at, NewExpiresAt}
    ],
    try ets:update_element(?TABLE, Cursor, Updates) of
        true -> ok;
        false -> not_found
    catch
        error:badarg -> not_found
    end.

?DOC("""
Drops the cursor immediately. Idempotent.
""").
-spec discard(cursor()) -> ok.

discard(Cursor) when is_binary(Cursor) ->
    true = ets:delete(?TABLE, Cursor),
    ok.

?DOC("""
Returns operational diagnostics. Cheap.
""").
-spec info() -> map().

info() ->
    #{
        table_size => ets:info(?TABLE, size),
        memory_words => ets:info(?TABLE, memory),
        ttl_ms => ttl_ms()
    }.

%% =============================================================================
%% gen_server CALLBACKS
%% =============================================================================

init(Opts) ->
    process_flag(trap_exit, true),
    %% Streams catalogue-snapshot pages in bursts during bootstrap/sync;
    %% off_heap mailbox so a page burst backlog isn't re-scanned by the GC.
    process_flag(message_queue_data, off_heap),
    _Tab = ets:new(?TABLE, [
        named_table,
        set,
        public,
        {keypos, #cursor.cursor},
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    schedule_gc(),
    {ok, #state{ttl_ms = maps:get(ttl_ms, Opts, ?DEFAULT_TTL_MS)}}.

handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(gc_tick, State) ->
    _ = sweep_expired(),
    schedule_gc(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
row_to_map(#cursor{
    instance_id = Id,
    ns = NS,
    index = Index,
    shard = Shard,
    bucket = Bucket,
    last_key = LastKey,
    watermark = Watermark
}) ->
    #{
        instance_id => Id,
        ns => NS,
        index => Index,
        shard => Shard,
        bucket => Bucket,
        last_key => LastKey,
        watermark => Watermark
    }.

%% @private
ttl_ms() ->
    application:get_env(bondy_mst, catalogue_cursor_ttl_ms, ?DEFAULT_TTL_MS).

%% @private
schedule_gc() ->
    erlang:send_after(?GC_INTERVAL_MS, self(), gc_tick).

%% @private
sweep_expired() ->
    Now = erlang:monotonic_time(millisecond),
    MatchSpec = [
        {
            #cursor{expires_at = '$1', _ = '_'},
            [{'<', '$1', {const, Now}}],
            [true]
        }
    ],
    ets:select_delete(?TABLE, MatchSpec).

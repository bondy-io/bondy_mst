%% =============================================================================
%% In-memory `bondy_oplog_projection_adapter` for tests.
%%
%% Backs the projection with a single `ordered_set` ETS table per
%% (NS, Index, Shard). Bucket is part of the ETS key: the row tuple is
%% `{{Bucket, Key}, Frame}`, so an `ordered_set` scan keeps a single
%% bucket's rows contiguous in `(Bucket, Key)` lexicographic order.
%% Suitable only for tests; production consumers should provide a
%% persistent adapter (Leveled, RocksDB, etc.).
%% =============================================================================

-module(bondy_oplog_projection_ets).

-behaviour(bondy_oplog_projection_adapter).

-export([
    open/4,
    close/1,
    get/3,
    put_batch/2,
    range/5,
    delete/3,
    info/1
]).

open(_NS, _Index, _Shard, _Opts) ->
    Tab = ets:new(?MODULE, [
        ordered_set,
        public,
        {read_concurrency, true}
    ]),
    {ok, Tab}.

close(Tab) ->
    true = ets:delete(Tab),
    ok.

get(Tab, Bucket, Key) ->
    case ets:lookup(Tab, {Bucket, Key}) of
        [{_, Frame}] -> {ok, Frame};
        [] -> not_found
    end.

put_batch(Tab, Entries) ->
    Rows = [{{B, K}, F} || {B, K, F} <- Entries],
    true = ets:insert(Tab, Rows),
    ok.

range(Tab, Bucket, Low, High, Opts) ->
    Limit = maps:get(limit, Opts, 1000),
    Direction = maps:get(direction, Opts, asc),
    %% Rows are keyed by `{Bucket, Key}`. To scan a single bucket's
    %% `[Low, High)` we constrain the composite key to that bucket.
    MS = [
        {
            {{'$1', '$2'}, '$3'},
            [
                {'=:=', '$1', {const, Bucket}},
                {'>=', '$2', {const, Low}},
                {'<', '$2', {const, High}}
            ],
            [{{'$2', '$3'}}]
        }
    ],
    Result =
        case ets:select(Tab, MS, Limit) of
            '$end_of_table' -> [];
            {Found, _Cont} -> Found
        end,
    Ordered =
        case Direction of
            asc -> Result;
            desc -> lists:reverse(Result)
        end,
    {ok, Ordered}.

delete(Tab, Bucket, Key) ->
    true = ets:delete(Tab, {Bucket, Key}),
    ok.

info(Tab) ->
    #{
        size => ets:info(Tab, size),
        memory => ets:info(Tab, memory)
    }.

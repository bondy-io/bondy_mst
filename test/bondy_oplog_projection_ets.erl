%% =============================================================================
%% In-memory `bondy_oplog_projection_adapter` for tests.
%%
%% Backs the projection with a single `ordered_set` ETS table per
%% (NS, Index, Shard). Suitable only for tests; production consumers
%% should provide a persistent adapter (Leveled, RocksDB, etc.).
%% =============================================================================

-module(bondy_oplog_projection_ets).

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

get(Tab, Key) ->
    case ets:lookup(Tab, Key) of
        [{_, Frame}] -> {ok, Frame};
        [] -> not_found
    end.

put_batch(Tab, Entries) ->
    true = ets:insert(Tab, Entries),
    ok.

range(Tab, Low, High, Opts) ->
    Limit = maps:get(limit, Opts, 1000),
    Direction = maps:get(direction, Opts, asc),
    MS = [{
        {'$1', '$2'},
        [
            {'>=', '$1', {const, Low}},
            {'<',  '$1', {const, High}}
        ],
        [{{'$1', '$2'}}]
    }],
    Result = case ets:select(Tab, MS, Limit) of
        '$end_of_table' -> [];
        {Found, _Cont} -> Found
    end,
    Ordered = case Direction of
        asc -> Result;
        desc -> lists:reverse(Result)
    end,
    {ok, Ordered}.

delete(Tab, Key) ->
    true = ets:delete(Tab, Key),
    ok.

info(Tab) ->
    #{
        size => ets:info(Tab, size),
        memory => ets:info(Tab, memory)
    }.

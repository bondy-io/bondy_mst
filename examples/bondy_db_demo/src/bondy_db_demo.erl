%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_db_demo).

-include_lib("kernel/include/logger.hrl").

-moduledoc """
REPL-friendly facade over the `demo` DB opened by
`bondy_db_demo_cluster`.

Boot a node (`bin/nodeN console`) and the three sharded tables
(`users`, `counters`, `tags`) are already open and replicating. Then,
from the shell:

```erlang
%% Write on node1
bondy_db_demo:put(<<"acme">>, <<"alice">>, <<"alice@acme.io">>).
bondy_db_demo:incr(<<"acme">>, <<"hits">>, 3).
bondy_db_demo:add(<<"acme">>, <<"colours">>, <<"red">>).

%% Read back here, or on node2 / node3 a moment later (sync is
%% periodic — call sync_now/0 to push immediately):
bondy_db_demo:get(users,    <<"acme">>, <<"alice">>).
bondy_db_demo:get(counters, <<"acme">>, <<"hits">>).
bondy_db_demo:get(tags,     <<"acme">>, <<"colours">>).
```

`get/3` returns `{ok, Value, Hlc}` | `not_found` | `{error, _}`, where
`Value` is the fold's user-facing value (the register value, the
counter integer, or the set as an ordset).
""".

%% Cluster introspection
-export([info/0]).
-export([peers/0]).
-export([tables/0]).
-export([table/1]).
-export([sync_now/0]).

%% Generic read/write
-export([get/3]).
-export([write/4]).

%% Per-table convenience writers (assume the default table names)
-export([put/3]).
-export([incr/3]).
-export([add/3]).

%% Sync dispatch (installed by bondy_db_demo_cluster)
-export([dispatch/2]).

-define(PT(K), {bondy_db_demo, K}).

%% =============================================================================
%% CLUSTER INTROSPECTION
%% =============================================================================

-doc "A snapshot of this node's demo wiring.".
-spec info() -> map().
info() ->
    #{
        node   => node(),
        db     => persistent_term:get(?PT(db_name), undefined),
        tables => tables(),
        peers  => peers(),
        scheduler => bondy_oplog_sync_scheduler:info()
    }.

-doc "The configured peer nodes (the cluster minus self).".
-spec peers() -> [node()].
peers() ->
    persistent_term:get(?PT(peers), []).

-doc "The open table names.".
-spec tables() -> [atom()].
tables() ->
    persistent_term:get(?PT(table_names), []).

-doc "The `bondy_db` table handle for `Name`.".
-spec table(atom()) -> map().
table(Name) when is_atom(Name) ->
    persistent_term:get(?PT({table, Name})).

-doc """
Force a sync tick now instead of waiting for the periodic timer.
Pushes this node's pending writes to every peer immediately.
""".
-spec sync_now() -> ok.
sync_now() ->
    bondy_oplog_sync_scheduler:trigger().

%% =============================================================================
%% READ / WRITE
%% =============================================================================

-doc """
Read `(Realm, Key)` from `Table`. Returns the fold's user-facing value.
""".
-spec get(atom(), binary(), binary()) ->
    {ok, term(), bondy_oplog_hlc:hlc()} | not_found | {error, term()}.
get(Table, Realm, Key)
        when is_atom(Table), is_binary(Realm), is_binary(Key) ->
    bondy_db:read(table(Table), Realm, Key).

-doc """
Write `Value` to `(Realm, Key)` in `Table`, choosing the event shape
from the table's fold module:

- `lww_register` — set the register to `Value` at a fresh HLC.
- `pn_counter`   — increment by `Value` (an integer; may be negative).
- `g_set`        — add the element `Value` to the set.

Other fold modules return `{error, {unsupported_fold_for_demo, Fold}}`
(use `bondy_db:apply/4` directly for those).
""".
-spec write(atom(), binary(), binary(), term()) -> ok | {error, term()}.
write(TableName, Realm, Key, Value)
        when is_atom(TableName), is_binary(Realm), is_binary(Key) ->
    T = table(TableName),
    case maps:get(fold_module, T) of
        lww_register ->
            bondy_db:apply(T, Realm, Key, {set, bondy_db:tick(T), Value});
        pn_counter when is_integer(Value) ->
            bondy_db:counter_inc(T, Realm, Key, Value);
        g_set ->
            bondy_db:apply(T, Realm, Key, {add, Value});
        Fold ->
            {error, {unsupported_fold_for_demo, Fold}}
    end.

-doc "Set the `users` register at `(Realm, Key)` to `Value`.".
-spec put(binary(), binary(), binary()) -> ok | {error, term()}.
put(Realm, Key, Value) ->
    write(users, Realm, Key, Value).

-doc "Increment the `counters` PN-Counter at `(Realm, Key)` by `Delta`.".
-spec incr(binary(), binary(), integer()) -> ok | {error, term()}.
incr(Realm, Key, Delta) when is_integer(Delta) ->
    write(counters, Realm, Key, Delta).

-doc "Add element `Elem` to the `tags` G-Set at `(Realm, Key)`.".
-spec add(binary(), binary(), binary()) -> ok | {error, term()}.
add(Realm, Key, Elem) when is_binary(Elem) ->
    write(tags, Realm, Key, Elem).

%% =============================================================================
%% SYNC DISPATCH
%% =============================================================================

-doc """
Sync dispatch for the closed disterl cluster. Spawns one async
pull-direction sync session per peer per tick over the disterl
transport. Sessions run in their own processes and report
success/failure via `bondy_oplog_peer_state`; the scheduler does not
wait for completion. An unreachable peer surfaces as a session error
that is absorbed by the session process — never a crash here.
""".
-spec dispatch(binary(), [node()]) -> ok.
dispatch(InstanceId, Peers) ->
    lists:foreach(
        fun(Peer) ->
            _ = bondy_oplog_sync_session:start(
                InstanceId, Peer,
                #{
                    transport      => bondy_oplog_transport_disterl,
                    transport_opts => #{timeout => 5_000}
                }
            )
        end,
        Peers
    ).

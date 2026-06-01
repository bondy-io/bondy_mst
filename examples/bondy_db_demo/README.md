# bondy_db 3-node REPL demo

Three relx releases — `node1`, `node2`, `node3` — that form a closed
disterl cluster and demonstrate `bondy_db` replicating CRDT-backed
tables across nodes.

Each release boots the in-tree example app `bondy_db_demo`, which on
startup:

1. opens DB `demo` (the `shared_shards` topology, `shard_count = 8`),
2. opens **three sharded tables**, each backed by a different CRDT fold:

   | Table      | Fold           | Semantics                              |
   |------------|----------------|----------------------------------------|
   | `users`    | `lww_register` | last-writer-wins single value          |
   | `counters` | `pn_counter`   | converging integer counter             |
   | `tags`     | `g_set`        | grow-only set (union-converging)       |

3. wires the sync scheduler (built-in static peer source + a disterl
   dispatch) so writes on one node replicate to the others every
   `sync_interval_ms` (250 ms).

Per-node identity and tuning come from `config/nodeN/sys.config` and
`config/nodeN/vm.args`. All three `sys.config` files are identical; the
node identity lives in `vm.args` (`-name nodeN@127.0.0.1`, shared
cookie `bondy_db_demo`). Each node keeps its data under
`/tmp/bondy_db_demo/<node>/`.

## Build

From the repository root, build each release into its own profile:

```bash
rebar3 as node1 release -n node1
rebar3 as node2 release -n node2
rebar3 as node3 release -n node3
```

> The `-n nodeN` flag is required: the default `bondy_mst` release and
> the `nodeN` release are both visible in the merged relx config, so
> the release name must be given explicitly.

## Run — one terminal per node

```bash
_build/node1/rel/node1/bin/node1 console
_build/node2/rel/node2/bin/node2 console
_build/node3/rel/node3/bin/node3 console
```

Each node logs `bondy_db_demo cluster ready` once its tables are open.
Nodes may start in any order — an unreachable peer is simply retried on
the next sync tick.

## Drive it from the REPLs

The `bondy_db_demo` module is the REPL facade. On **node1**:

```erlang
%% lww_register — set a value
bondy_db_demo:put(<<"acme">>, <<"alice">>, <<"alice@acme.io">>).

%% pn_counter — increment (Delta may be negative)
bondy_db_demo:incr(<<"acme">>, <<"hits">>, 3).

%% g_set — add an element
bondy_db_demo:add(<<"acme">>, <<"colours">>, <<"red">>).
```

On **node2** (a moment later, or after `bondy_db_demo:sync_now()`):

```erlang
bondy_db_demo:get(users,    <<"acme">>, <<"alice">>).
%% => {ok, <<"alice@acme.io">>, _Hlc}

bondy_db_demo:get(counters, <<"acme">>, <<"hits">>).
%% => {ok, 3, _Hlc}

bondy_db_demo:get(tags,     <<"acme">>, <<"colours">>).
%% => {ok, [<<"red">>], _Hlc}
```

Concurrent writes converge by CRDT merge — increment on every node and
the counter settles on the sum; add on every node and the set settles
on the union:

```erlang
%% node1                              %% node2
bondy_db_demo:incr(<<"acme">>, <<"hits">>, 10).   bondy_db_demo:incr(<<"acme">>, <<"hits">>, 7).
%% both nodes converge to {ok, 17, _}
```

### API

| Function | Meaning |
|---|---|
| `put(Realm, Key, Value)`       | set the `users` register |
| `incr(Realm, Key, Delta)`      | increment the `counters` PN-Counter |
| `add(Realm, Key, Elem)`        | add to the `tags` G-Set |
| `write(Table, Realm, Key, V)`  | generic write; event shape chosen from the table's fold |
| `get(Table, Realm, Key)`       | read → `{ok, Value, Hlc}` \| `not_found` \| `{error, _}` |
| `sync_now()`                   | force a sync tick now (don't wait for the timer) |
| `peers()` / `tables()` / `info()` | cluster introspection |

`Realm`, `Key`, and string values are binaries.

## Configure

Edit `config/nodeN/sys.config` (`bondy_db_demo` block) to change the DB
name, the `tables` list (`{Name, FoldModule}` pairs), `shard_count`,
the `data_dir`, or the `nodes` membership list. To run on more than one
host, set real hostnames/IPs in both `nodes` (sys.config) and `-name`
(vm.args), and keep the cookie identical.

## Clean up

```bash
_build/node1/rel/node1/bin/node1 stop   # (or Ctrl-C twice in console)
rm -rf /tmp/bondy_db_demo
```

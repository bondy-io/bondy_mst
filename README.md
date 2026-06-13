# bondy_mst

An Erlang/OTP library for building **coordination-free, eventually-consistent
replicated data stores** on top of Merkle Search Trees.

A *CRDT* (Conflict-free Replicated Data Type) is a data type whose
operations can be applied in any order on any replica and still converge
to the same result. CRDTs sidestep consensus protocols (Raft, Paxos) by
trading strong consistency for *Strong Eventual Consistency* — replicas
that have delivered the same operations end up in the same state, with
no coordination required during writes.

The library is **three cooperating packages behind one facade**, used
together or independently:

1. **`bondy_db`** — the **consumer-facing database** and the recommended
   entry point for most applications. It gives you typed *tables* over a
   native **CRDT catalogue** (registers, counters, g/2P/add-wins/remove-wins
   sets, multi-value register, add-wins map, enable/disable-wins flags), a
   fast read path (cache + overlay + LSM projection, with bounded staleness
   if you ask for it), a table-oriented write API (`apply/4`,
   `counter_inc/4`, `apply_batch/4`, `map_update/4`), pluggable durability
   (durable Leveled vs ephemeral in-memory), shard topologies, and optional
   secondary indexes. See [The `bondy_db` facade](#the-bondy_db-facade).
2. **`bondy_oplog`** — the **write + replication framework** beneath
   `bondy_db`. It appends events durably to a **local** WAL keyed by
   `{HLC, Origin, Seq}` (the source of truth, used for crash recovery) and
   installs them into the MST. Replication is coordination-free
   anti-entropy: peers converge by exchanging only the differing **MST
   pages** — the WAL is never shipped. Stable prefixes of the log fold into
   snapshots through a consumer-defined `interpret_cog/2`. Use it directly
   when you want the low-level event-log API rather than tables. The Concurrent
   Operation Group (COG) abstraction and the operation-log architecture are
   taken from Preston McCrary's 2022 paper [*Canteen: A Partially-Ordered
   Log Abstraction for the Emerging CRDT
   Datastore*](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2022/EECS-2022-160.html)
   (UC Berkeley); we replace Canteen's hash-chained DAG with the
   Auvolat & Taïani MST, adapting COG truncation to a tree-shaped log. The
   framework is agnostic to event payload semantics, transport (Distributed
   Erlang, Partisan, gRPC, ...), durability (in-memory, file, RocksDB, ...),
   and trust model (closed cluster, Byzantine-tolerant).
3. **`bondy_mst`** — the **Merkle Search Tree** primitive used as the
   replication structure, based on Alex Auvolat & François Taïani's 2019
   paper [*Merkle Search Trees: Efficient State-Based CRDTs in Open
   Networks*](https://inria.hal.science/hal-02303490/document). A balanced,
   content-addressed search tree whose shape is determined deterministically
   by item hashes, so two replicas with the same set of items have the same
   root hash — two peers compare root hashes and exchange only the differing
   pages. Usable standalone for anti-entropy and integrity verification.

**Where to start:** application developers should use the **`bondy_db`**
facade — see the [facade section](#the-bondy_db-facade) below, [chapter
03](doc_extras/architecture/03_bondy_db.md), the [app developer's
tour](doc_extras/architecture/07_app_developers_tour.md), and the
cheatsheet. The bulk of *this* README documents the **`bondy_oplog`**
framework layer (the event-log API, replication, compaction, validators)
that `bondy_db` is built on; reach for it when you need the low-level API.

If "MST", "COG", and "CRDT" don't already mean something specific to
you, read the [Background](#background-msts-and-cogs) section before
the Quick Start. The rest of this README assumes those concepts.

---

## Table of Contents

- [When to use this library](#when-to-use-this-library)
- [Architecture (tutorial)](#architecture-tutorial)
- [The `bondy_db` facade](#the-bondy_db-facade)
- [Background: MSTs and COGs](#background-msts-and-cogs)
- [Quick start](#quick-start)
- [Concepts](#concepts)
- [Defining a CRDT](#defining-a-crdt)
- [Lifecycle](#lifecycle)
- [Writing events](#writing-events)
  - [Concurrency and the lock-free fast path](#concurrency-and-the-lock-free-fast-path)
  - [Performance tips](#performance-tips)
- [Reading and querying](#reading-and-querying)
- [Replication](#replication)
- [Compaction and snapshots](#compaction-and-snapshots)
  - [Manual prefix truncation (advanced — lossy)](#manual-prefix-truncation-advanced--lossy)
  - [Retention advice](#retention-advice)
- [Persistence](#persistence)
- [Validators and Byzantine tolerance](#validators-and-byzantine-tolerance)
- [Operations](#operations)
- [Configuration reference](#configuration-reference)
- [Behaviour reference](#behaviour-reference)
- [The MST primitive](#the-mst-primitive)
- [Installation](#installation)
- [Jepsen](#jepsen)

---

## When to use this library

Use it when you need:

- **Eventually consistent state** replicated across many replicas without
  consensus (no Raft, no Paxos, no leader election).
- A clean separation between **what an event means** (your CRDT
  `interpret_cog`) and **how events are propagated** (the framework).
- **Self-verifying anti-entropy** — replicas converge by exchanging
  Merkle-tree pages, never the full state.
- **Truncatable history** — once a prefix of events is stable across the
  cluster it is folded into a snapshot and dropped from memory, so storage
  cost is bounded by the live (unstable) tail.

Don't use it if you need strong consistency, linearizable writes, or
read-your-writes across replicas. Those need consensus; this library
trades them away for availability and partition tolerance.

---

## Architecture (tutorial)

For the chapter-style, mermaid-illustrated walkthrough of how
`bondy_db` (read side), `bondy_mst` (Merkle Search Tree + page store),
and `bondy_oplog` (write side + sync) fit together, read the docs
under `doc_extras/architecture/`:

| # | Doc | Topic |
|---|---|---|
| 00 | [Overview](doc_extras/architecture/00_overview.md) | The three packages and one end-to-end write + read. |
| 01 | [bondy_oplog](doc_extras/architecture/01_bondy_oplog.md) | Instances, WAL, sync sessions, eager-push vs. anti-entropy. |
| 02 | [bondy_mst](doc_extras/architecture/02_bondy_mst.md) | The Merkle Search Tree, the pack-store backend, AE protocol. |
| 03 | [bondy_db](doc_extras/architecture/03_bondy_db.md) | The consumer-facing facade: tables, the CRDT catalogue, the read path (cache + overlay + projection, freshness fence), and the table write API. |
| 04 | [Applier](doc_extras/architecture/04_applier.md) | The reconciler loop that ties writes, the MST, and the projection together. |
| 05 | [The CRDT model](doc_extras/architecture/05_crdt_model.md) | The pure operation-based CRDT contract: `interpret_cog`, `apply_op`, causal tiers, the native catalogue. |
| 06 | [Compaction & bootstrap](doc_extras/architecture/06_compaction_and_bootstrap.md) | Why the oplog is bounded; how new replicas join. |
| 07 | [App developer's tour](doc_extras/architecture/07_app_developers_tour.md) | Mapping a domain onto tables, CRDTs and topologies. |
| 08 | [Backup & restore](doc_extras/architecture/08_backup_and_restore.md) | Operator runbook for `bondy_mst_admin`. |

The same docs ship in the ex_doc output (see `make docs`). These
chapters are the authoritative architecture reference; module docs
carry the implementation-level contracts.

---

## The `bondy_db` facade

Most applications never touch `bondy_oplog` or `bondy_mst` directly —
they use **`bondy_db`**, a table-oriented database over the native CRDT
catalogue. You open a DB and typed tables, write CRDT operations to
`(Realm, Key)` cells, and read the materialised value back; replication,
durability, compaction, and the MST are handled underneath.

```erlang
{ok, _} = application:ensure_all_started(bondy_mst),

%% A DB groups tables under a shard topology.
{ok, Db} = bondy_db:open(my_db, #{
    topology => bondy_db_topology_shared_shards
}),

%% A table picks a CRDT from the catalogue by its `fold_module` type
%% label (required), plus shard count, indexes, durability, ...
{ok, Users}    = bondy_db:open_table(Db, users,    #{
    fold_module => lww_register, shard_count => 8
}),
{ok, Counters} = bondy_db:open_table(Db, counters, #{
    fold_module => pn_counter
}),

%% Write CRDT ops to (Realm, Key) cells. The op shape is CRDT-specific.
ok = bondy_db:apply(Users, <<"acme">>, <<"alice">>, {set, Hlc, <<"Alice">>}),
ok = bondy_db:counter_inc(Counters, <<"acme">>, <<"visits">>, +1),

%% Read the materialised value (cache + overlay + projection, HLC-merged).
{ok, <<"Alice">>, _Hlc} = bondy_db:read(Users, <<"acme">>, <<"alice">>).
```

What the facade adds on top of the framework:

- **A native CRDT catalogue** — `lww_register`, `max`/`min_register`,
  `g`/`pn_counter`, `g_set`, `two_p_set`, `aw_set`, `rw_set`,
  `mv_register`, `aw_map`, `ew_flag`, `dw_flag` — selected per table by its
  short type label `fold_module` (required); pass a fully-qualified
  `crdt_module` to override with a custom module. See [The CRDT
  model](doc_extras/architecture/05_crdt_model.md).
- **Pluggable durability per table** — durable (Leveled LSM projection +
  pack-store MST) or `durability => ephemeral` (in-memory ETS projection +
  in-memory WAL, optionally `fused`) for hot, rebuildable state.
- **Batched map/set writes** — `apply_batch/4` and `map_update/4` pack
  many field commands into one atomic event (one WAL/MST entry).
- **Secondary indexes** — declare `indexes => [...]` on a table and query
  with `index_get/5` / `index_range/6` (with a `max_lag` freshness fence).
- **Shard topologies** — `single_bookie`, `per_entity`, `shared_shards`,
  and an in-memory topology, mapping tables onto shards.

The one-page [cheatsheet](doc_extras/cheatsheet.cheatmd) has the full
API at a glance; [chapter 03](doc_extras/architecture/03_bondy_db.md) and
the [app developer's tour](doc_extras/architecture/07_app_developers_tour.md)
are the narrative reference. The rest of this README covers the
`bondy_oplog` framework that sits beneath this facade.

---

## Background: MSTs and COGs

This library combines two ideas from the literature:

- **Merkle Search Trees** (Auvolat & Taïani, 2019) — for efficient
  anti-entropy by Merkle-hash comparison.
- **Concurrent Operation Groups** (McCrary, *Canteen*, 2022) — for
  collapsing stable prefixes of an operation log into snapshots, so
  storage cost is bounded by churn rather than history.

We adapt both: Canteen's COG abstraction is layered over an MST
instead of Canteen's hash-chained DAG — the MST gives the same
deterministic, content-addressed event ordering while adding
efficient set-reconciliation anti-entropy, which a hash-chained DAG
cannot offer. See
[chapter 06](doc_extras/architecture/06_compaction_and_bootstrap.md)
for how the COG/compaction machinery rides the MST.

### Merkle Search Trees (MSTs)

The MST construction is from Auvolat & Taïani's 2019 paper
[*Merkle Search Trees: Efficient State-Based CRDTs in Open
Networks*](https://inria.hal.science/hal-02303490/document) (Inria
HAL-02303490). It is a balanced search tree in which the *shape* of
the tree is determined by the *content* of the items, not by their
insertion order. The construction uses item hashes to deterministically
pick which layer each item lives on:

- Compute `hash(item)` and read it in some base `B`.
- Count the leading zero "digits" — that's the item's layer number.
- Items at layer `L` are pivots between pages of layer `L−1`. Internal
  pages reference their children by hash (the *Merkle* part).

The result has two important properties:

1. **Two replicas holding the same set of items produce the same tree.**
   No matter the insertion order, the layer assignments are identical
   and the page boundaries fall in the same places. Same items → same
   tree → same root hash.

2. **Every page is content-addressed.** Its hash is computed over the
   page's contents *including the hashes of its children*. Two pages
   with the same hash are bit-identical and have identical subtrees
   beneath them.

These two properties combine into the killer feature: **anti-entropy
by hash comparison**. Replica A asks replica B for its root hash; if
they match, they're done. If not, A walks down B's tree by asking for
pages, but only fetches subtree pages whose hashes A doesn't already
have. The amount of data exchanged is proportional to the *difference*
between the two trees, not to the *full state*.

For 100,000 events with 10 events different across two replicas, a
naive full-state exchange transfers 100,000 events; an MST exchange
transfers 10 events plus a logarithmic number of internal pages. This
is why MSTs are the right substrate for replicated event logs.

### Events sort by Hybrid Logical Clock

Every event in this library has a globally unique key:

```
{HLC, Origin, Seq}
```

A Hybrid Logical Clock combines wall-clock time with a logical counter
so that:

- Within a single replica, HLCs are strictly monotonic.
- Across replicas, HLCs roughly track wall-clock time but never go
  backwards even when peers' clocks drift, jump, or arrive in the
  future.
- The full `{HLC, Origin, Seq}` triple gives a *deterministic total
  order* across all replicas: there is exactly one canonical order in
  which events are folded into snapshots, and every replica computes
  the same order.

The total order is what `interpret_cog/2` consumes: events arrive in
this order regardless of when they were inserted into local storage.

### Concurrent Operation Groups (COGs)

The COG abstraction comes from Preston McCrary's 2022 master's thesis
[*Canteen: A Partially-Ordered Log Abstraction for the Emerging CRDT
Datastore*](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2022/EECS-2022-160.html)
(UC Berkeley, EECS-2022-160). Canteen organises operations into a
hash-chained DAG and exposes COGs as a partially-ordered log
abstraction; CRDT semantics layer on top.

This library adopts the COG abstraction but uses an MST as the
underlying log substrate instead of Canteen's DAG — the MST's
content-addressed, history-independent structure provides the same
deterministic ordering plus efficient anti-entropy.

A **Concurrent Operation Group** is a maximal contiguous batch of
events that is *stable* — no event with a key inside the batch's range
can possibly arrive in the future. Once stable, a batch can be folded
into a snapshot atomically through a consumer-defined `interpret_cog/2`
function, and the events themselves can be dropped from the live log.

Stability is established by **peer confirmation**: every replica
periodically reports its current root hash to its peers via sync. When
every fresh peer's confirmed root includes events up to time `T`, the
prefix `(-∞, T]` is stable everywhere — by construction, no event with
HLC ≤ T can still arrive. The library tracks per-peer roots in the
`bondy_oplog_peer_state` registry and computes the
**stability frontier** `T` on each compaction cycle.

The COG abstraction is what makes the log size bounded. Without it,
the event log would grow forever; with it, the live log holds only
events past the latest stable frontier, plus a compact snapshot of
everything older. Storage cost becomes a function of *churn*, not of
*history*.

Crucially, **`interpret_cog/2` is consumer-defined**. The framework
provides the COG abstraction (when is a batch stable?); the consumer
provides the semantics (what does the batch *mean*?). For a counter,
`interpret_cog/2` sums the increments. For a key-value store with
last-writer-wins, it picks the latest write per key. For a set, it
unions. Different CRDTs, same framework.

The determinism of `interpret_cog/2` is the anchor of correctness:
every replica that folds the same stable COG produces the same
snapshot. That's the foundation of *Strong Eventual Consistency* —
replicas that have delivered the same events converge on the same
state, no coordination required.

### The replication loop, end to end

Putting it together:

1. A replica receives an op (local) or an event (remote). It signs +
   inserts into its MST.
2. Periodically, sync sessions exchange MST root hashes between peers
   and then exchange the differing pages. Both replicas converge on
   the same tree.
3. After each sync, the peer's confirmed root is recorded in
   `peer_state`.
4. Periodically, the GC scheduler computes the stability frontier:
   across all fresh peers, what's the largest event key reachable from
   *every* peer's confirmed root?
5. That frontier is a stable COG boundary. Events between the previous
   watermark and the frontier are folded through `interpret_cog/2`
   into a new snapshot. The MST is truncated up to the frontier.
6. Reads project the live MST + snapshot through `query/2`.

Coordination-free at every step. The only synchronization between
replicas is the periodic sync session — and even those are
pull-direction and idempotent.

---

## Quick start

A counter CRDT replicated across two replicas in a single VM, in 60 lines.

### 1. Define the CRDT

```erlang
-module(my_counter).
-behaviour(bondy_oplog_crdt).

-export([causal_tier/0, init/0, interpret_cog/2, query/2]).

causal_tier() -> tier_0.
init()        -> 0.

interpret_cog(Events, State) ->
    lists:foldl(
        fun(E, Acc) ->
            case bondy_oplog_event:op(E) of
                {inc, N} -> Acc + N;
                _        -> Acc
            end
        end,
        State,
        Events
    ).

query(value, State) -> State.
```

> This is a minimal **event-log** CRDT — the `bondy_oplog` API uses only
> `causal_tier/0`, `init/0`, `interpret_cog/2`, and `query/2`. A CRDT that
> backs a `bondy_db` *table* (or durable storage) additionally implements the
> projection-seam callbacks `to_value/1`, `hlc/1`, `encode_state/1`,
> `decode_state/1`; see the native catalogue modules and
> [Defining a CRDT](#defining-a-crdt).

### 2. Start the application and two replicas

```erlang
{ok, _} = application:ensure_all_started(bondy_mst),

%% Replica A
{ok, _} = bondy_oplog:start_instance(<<"a">>, #{
    crdt_module => my_counter
}),

%% Replica B
{ok, _} = bondy_oplog:start_instance(<<"b">>, #{
    crdt_module => my_counter
}).
```

### 3. Append events

```erlang
_ = bondy_oplog:append(<<"a">>, {inc, 5}),
_ = bondy_oplog:append(<<"b">>, {inc, 3}).
```

### 4. Synchronise

```erlang
%% In-VM transport: peer ids are local instance ids.
{ok, _} = bondy_oplog:sync(<<"a">>, <<"b">>),  %% pull B → A
{ok, _} = bondy_oplog:sync(<<"b">>, <<"a">>).  %% pull A → B
```

### 5. Query derived state

```erlang
8 = bondy_oplog:query(<<"a">>, value),
8 = bondy_oplog:query(<<"b">>, value).
```

Both replicas now hold the same root hash and the same projected counter
value. No coordination was required.

---

## Concepts

A short reference glossary. See [Background](#background-msts-and-cogs)
for the longer explanations.

| Term | Meaning |
|---|---|
| **Instance** | One independent replicated value: one MST, one event log, one snapshot, one storage backend. A node hosts many. Identified by a binary `instance_id()` chosen by the consumer. |
| **Origin** | Opaque binary identity of a replica. Two distinct replicas must never share an Origin. Defaults to a per-VM 128-bit random id. For Byzantine deployments, use `sha256(public_key)`. |
| **Event key** | Globally unique `{HLC, Origin, Seq}` triple. `HLC` is the Hybrid Logical Clock; `Seq` is a per-Origin monotonic counter. The triple sorts lexicographically, giving every replica the same total order on events. |
| **Operation** | The CRDT-level intent attached to an event. Opaque to the framework, interpreted by your `interpret_cog/2`. Examples: `{inc, 5}`, `{put, key, value}`, `{add, item}`. |
| **Snapshot** | The folded CRDT state at a compaction frontier — the output of `interpret_cog/2` over the stable prefix. Each instance keeps exactly one (the most recent). |
| **Watermark** | The highest event key included in the latest snapshot. Events `=< watermark` are no longer in the live MST; events `> watermark` are. |
| **COG** | *Concurrent Operation Group* — a contiguous batch of events that has become stable across the cluster. The unit of compaction. |
| **Sync session** | A single pull-direction anti-entropy round: this replica fetches the peer's root hash and the pages it's missing. Two sessions (in opposite directions) converge both replicas. |
| **Stability frontier** | The largest event key reachable from *every* fresh peer's confirmed root. Computed on each compaction cycle; events up to this key are eligible for folding into the snapshot. |

---

## Defining a CRDT

Implement `bondy_oplog_crdt`:

| Callback | Required | Purpose |
|---|---|---|
| `causal_tier/0` | yes | Return `tier_0`, `tier_1`, or `tier_2` — selects the causal metadata the substrate provisions (tier_0 = scalar HLC; tier_2 = per-cell causal context). See [The CRDT model](doc_extras/architecture/05_crdt_model.md). |
| `init/0` | yes | Bottom state — what the CRDT looks like when no events have ever been applied. |
| `interpret_cog/2` | yes | `(Events, State) -> NewState`. Given a batch of events in key order, return the updated state. **Must be deterministic** — same inputs ⇒ same output on every replica. This is the foundation of convergence. |
| `query/2` | yes | `(Query, State) -> Result`. Project the state for client queries. Pure. |
| `to_value/1` | yes | `(State) -> Value`. The materialised value for a cell — what `bondy_db:read/3` returns. |
| `hlc/1` | yes | `(State) -> hlc()`. The state's current HLC (drives projection + merge). |
| `encode_state/1`, `decode_state/1` | yes | Serialise the folded state to/from a binary (compaction checkpoint + projection storage). |
| `gc_threshold/1` | optional | `(State) -> hlc() \| undefined`. Frontier below which causal metadata may be reaped. |
| `value_equals_state/0` | optional | `-> boolean()`. `true` when the projection value *is* the state (skips a re-encode). |
| `order_independent/0` | optional | `-> boolean()`. `true` for commutative types (enables the eager `apply_op/3` path on `bondy_oplog_crdt_commutative`). |
| `context_of/1`, `reap_origins/2` | optional (tier_2) | Per-cell causal-context accessor + dead-origin reaping. |

The first four callbacks (`causal_tier/0`, `init/0`, `interpret_cog/2`,
`query/2`) are all the **event-log** API (`bondy_oplog`) needs — that is what
the Quick Start `my_counter` implements. The projection-seam callbacks
(`to_value/1`, `hlc/1`, `encode_state/1`, `decode_state/1`) are additionally
required when the CRDT backs a **`bondy_db` table** or durable storage; the
native catalogue modules implement all of them.

### Determinism is non-negotiable

`interpret_cog/2` must be a pure deterministic function. Replicas that
received the same events will produce different snapshots otherwise, and the
system will silently diverge. No timestamps from `os:system_time/1`, no
`rand`, no environment lookups, no message passing.

---

## Lifecycle

```erlang
%% Start with defaults (in-memory, trust validator, ETS compaction checkpoint).
{ok, SupPid} = bondy_oplog:start_instance(InstanceId).

%% Or with options:
{ok, SupPid} = bondy_oplog:start_instance(InstanceId, Opts).

%% Stop:
ok = bondy_oplog:stop_instance(InstanceId).

%% List running:
Ids = bondy_oplog:list_instances().

%% Discover on disk (when using a durable storage backend):
Ids = bondy_oplog:discover_instances(<<"/var/lib/bondy_mst">>).
```

`start_instance/2` is idempotent — calling it for an already-running
instance returns the existing supervisor pid.

The library does **not** enforce a lifecycle policy. Lazy loading, LRU
eviction, cold-tier offload, per-tenant quota — these are the consumer's
concerns. Call `start_instance` and `stop_instance` from your own code.

---

## Writing events

### Local append

```erlang
%% Op only (default meta = undefined):
Key = bondy_oplog:append(InstanceId, Op).

%% Op + meta:
Key = bondy_oplog:append(InstanceId, Op, Meta).

%% Atomic batch — all-or-nothing:
Keys = bondy_oplog:append_many(InstanceId, [
    {Op1, Meta1},
    {Op2, Meta2},
    {Op3, undefined}
]).
```

Local appends:
1. Mint a fresh `{HLC, Origin, Seq}` event key.
2. Sign the event through the configured validator.
3. Insert it into the MST.
4. Publish the new state to the read-snapshot registry.

### Remote receipt

When you receive an event from a peer (out-of-band, e.g. a pubsub gossip
layer), inject it via:

```erlang
ok = bondy_oplog:append_remote(InstanceId, Event).
```

The framework verifies the event through the validator, detects equivocation
(two valid signatures at the same `{HLC, Origin, Seq}` with different
payloads — see [Validators](#validators-and-byzantine-tolerance)), and
inserts. Below-watermark events are dropped silently. Idempotent re-receives
are no-ops.

### Backpressure

Optional working-set cap:

```erlang
{ok, _} = bondy_oplog:start_instance(Id, #{
    max_working_set => 10_000  %% reject appends past this many live events
}).
```

`append/2` returns `{error, working_set_full}` when the cap is reached.
`append_many/2` admits atomically — either all events fit, or none.

### Concurrency and the lock-free fast path

`bondy_oplog:append/2,3` and `append_many/2` are **lock-free** when
the configured validator advertises `is_stateless/0 -> true` — the
default `bondy_oplog_validator_trust` does. The caller process
builds the event(s), signs them in-process, calls the WAL gen_server
directly, and stages the overlay row(s) inline. The instance
gen_server is not on the hot write path.

Concretely, what this means at runtime:

| Path                          | Hops | Bottleneck                          |
|-------------------------------|------|-------------------------------------|
| `append/2,3` (stateless)      | 1    | WAL gen_server (file + fsync)       |
| `append/2,3` (stateful)       | 2    | Instance gen_server, then the WAL   |
| `append_many/2` (stateless)   | 1    | WAL gen_server (one batch frame)    |
| `append_many/2` (stateful)    | 2    | Instance gen_server, then the WAL   |
| `append_remote/2`             | 1    | Applier (out-of-band)               |

The WAL gen_server is still a serialisation point — every appender
queues for the WAL's frame-ordered write. In `batched` fsync mode
that's well over a million events/s on commodity hardware; in
`per_write` mode it's bounded by the device fsync rate (a few
thousand/s).

**Reads are also lock-free.** `get/2`, `fold_range/5`,
`first_key/1`, `latest_key/1`, `size/1`, and `root_hash/1` go
straight to the registry-published MST handle and the overlay ETS
table — no gen_server hop, no waiting behind writes.

**Stateful validators** (`bondy_oplog_validator_crypto` or any
caller-supplied validator that doesn't export
`is_stateless/0 -> true`) route through the instance gen_server so
the validator's per-event state mutations are serialised correctly.
This is automatic — same `append/2,3` and `append_many/2` API.

**The `bench/README.md`** has measured numbers and a longer
discussion of the concurrency model.

### Performance tips

The install pipeline applies events from the WAL into the MST inside
the per-instance gen_server. That gen_server is the only writer to the
MST handle; all reads bypass it via the registry-published snapshot.
A handful of choices in how you call the API change throughput by an
order of magnitude:

**Prefer `append_many/2` over a loop of `append/2` when you have more
than one event to write.** A 100-event batch is installed via a single
`bondy_mst:put_batch/2` call that builds a small in-process MST from
the batch and merges it into the live tree in one traversal. Compared
to 100 individual appends, this collapses ~600 page rebuilds and hashes
down to ~15, and amortises the per-cast publish, overlay-evict, HLC
update, and telemetry emit. Measured: ~3× lower per-event install cost
and ~3× higher sustained throughput on the bench's `append_many` scenarios.

**Keep the validator stateless if you can.** The default
`bondy_oplog_validator_trust` advertises `is_stateless/0 -> true` and
gets the lock-free fast path: the calling process builds and signs
events in-process, calls the WAL gen_server directly, and stages
overlay rows without routing through the instance gen_server. Stateful
validators (anything that doesn't export `is_stateless/0 -> true`) fall
back to the gen_server path so the validator's per-event state
mutations stay serialised. Both paths share the same `append/2,3`
and `append_many/2` API — the routing is automatic.

**Tune the overlay cap if you have bursty writers.** The default
backpressure caps are 10,000 events / 5 MB. When workers push faster
than the applier can drain into the MST, `append/2,3` and
`append_many/2` return `{error, backpressure}` once either cap is
crossed. If your workload is bursty and you have memory to spare,
increase the caps:

```erlang
{ok, _} = bondy_oplog:start_instance(Id, #{
    max_overlay_events => 100_000,
    max_overlay_bytes  => 50 * 1024 * 1024
}).
```

A larger cap absorbs more in-flight events but extends the worst-case
wait for `await_apply/1,2` (used by callers that need read-your-writes
beyond the overlay's window) — the applier still has to drain it. Pick
a value matched to your bursts; for steady-state workloads the default
is usually fine.

**Use `await_apply/1,2` sparingly.** It blocks until the overlay is
empty — useful for tests and for read-your-writes barriers, but doing
it in a tight loop forces every caller to wait for the applier to
catch up. Reads via `get/2`, `fold_range/5`, `first_key/1`,
`latest_key/1`, and `size/1` already see the overlay so they observe
freshly appended events without an `await_apply`.

**WAL fsync mode.** `per_write` is the default and gives durability
on every successful return at the cost of one fsync per WAL frame
(~5k frames/sec on commodity SSDs). For higher append throughput when
you can tolerate a small window of post-ack data loss on a hard crash,
configure `fsync_mode => batched` at `start_instance/2`, and at
explicit barriers call `bondy_oplog_wal:await_durable/3` on the
per-instance writer pid (look it up via
`bondy_oplog_registry:wal_pid/1`). See `bondy_oplog_wal`'s
moduledoc and the WAL options table in
[Configuration reference](#configuration-reference) below.

---

## Reading and querying

All reads with binary `instance_id()` arguments are **lock-free**: a single
ETS lookup pulls the published MST handle/snapshot/watermark, and the rest
runs in the calling process. They do not queue behind ongoing writes.

### Event-level reads

```erlang
%% Single event by key:
{ok, Event} = bondy_oplog:get(Id, Key).

%% Range scan (inclusive):
Events = bondy_oplog:range(Id, From, To).

%% Streaming fold:
Acc = bondy_oplog:fold_range(Id, From, To, Fun, Acc0).

%% Bookends:
{ok, FirstKey}  = bondy_oplog:first_key(Id).
{ok, LatestKey} = bondy_oplog:latest_key(Id).

%% Diagnostics:
N        = bondy_oplog:size(Id).
Origin   = bondy_oplog:origin(Id).
Hash     = bondy_oplog:root_hash(Id).
DiagMap  = bondy_oplog:info(Id).
```

### CRDT queries

```erlang
%% Hot query: snapshot + live events folded through interpret_cog/2.
%% Reflects the most recent local view, including unstable events.
Result = bondy_oplog:query(Id, MyQuery).

%% Stable query: snapshot only, no live replay. Cheaper and identical
%% across replicas that have compacted to the same watermark.
Result = bondy_oplog:query_stable(Id, MyQuery).
```

Query semantics are entirely defined by your CRDT module's `query/2`.

### Per-cell projection

When the instance is configured with a CRDT module (see [The CRDT
model](doc_extras/architecture/05_crdt_model.md)), the substrate
also maintains a per-instance materialised projection fed by the
applier:

```erlang
%% Drains the applier first, then returns the current fold projection.
{ok, State}            = bondy_oplog:projection(Id).
{error, no_fold_configured} = bondy_oplog:projection(OtherId).
```

For full cell-level reads (cache + overlay + projection merge with
HLC), use the `bondy_db` / `bondy_oplog_core` read facade documented in
[`doc_extras/architecture/03_bondy_db.md`](doc_extras/architecture/03_bondy_db.md).

---

## Replication

### Sync

```erlang
%% Synchronous pull from Peer into Instance.
{ok, FinalRoot} = bondy_oplog:sync(Instance, Peer).

%% Async — returns a pid that completes in the background.
{ok, Pid} = bondy_oplog:sync_async(Instance, Peer).

%% Bootstrap: install the peer's snapshot first, then sync the live tail.
%% Use for fresh or far-behind replicas joining a long-running cluster.
{ok, _Root} = bondy_oplog:bootstrap(Instance, Peer).
```

Sync is **single-direction** — calling `sync(A, B)` brings A up to date with
B, but not the reverse. For full convergence either call sync in both
directions, or rely on the default sync scheduler running on every replica.

### Default sync scheduler

The library ships a periodic scheduler that, on every tick, asks each
running instance's configured peer source for peers and dispatches a sync
session to each. Configure via app env:

```erlang
%% sys.config
[
    {bondy_mst, [
        {sync_scheduler, true},
        {sync_interval_ms, 500},
        {peer_source, bondy_oplog_peer_source_static},
        {peer_source_opts, #{peers => [Peer1, Peer2, Peer3]}}
    ]}
].
```

Two built-in peer sources:

- `bondy_oplog_peer_source_static` — reads `peers` from opts.
- `bondy_oplog_peer_source_sample` — random subset of a `pool`.

For domain-specific topologies, implement
`bondy_oplog_peer_source` yourself.

### Transports

The transport plugs in the actual network. Two ship with the library:

- `bondy_oplog_transport_inline` — in-VM, for tests. `peer_id` is
  a local instance id.
- `bondy_oplog_transport_disterl` — Distributed Erlang. `peer_id`
  is a node atom (e.g. `'b@host'`).

A Partisan transport is not shipped (it would force Partisan as a hard
dependency); implement `bondy_oplog_transport` yourself with
`partisan_gen_server:call/3` in place of `gen_server:call/3`.

```erlang
{ok, _} = bondy_oplog:sync(Instance, 'b@host', #{
    transport      => bondy_oplog_transport_disterl,
    transport_opts => #{timeout => 10000}
}).
```

For disterl, every node must have the `bondy_mst` application running so
the per-node `bondy_oplog_responder` is registered.

---

## Compaction and snapshots

Compaction folds the *stable prefix* of events into a CRDT snapshot and
truncates them from the live MST.

### Stability frontier

The framework computes the largest event key K such that every event with
key `=< K` is reachable from every fresh peer's confirmed root. Peer roots
are recorded by sync sessions in `bondy_oplog_peer_state` (a
node-shared ETS); peers are considered fresh if their `last_seen` is within
`peer_timeout_ms` (default 30 s, app-env tunable).

If no peers are fresh, no events are compacted — the log grows, but
correctness is preserved.

### Manual compaction

```erlang
case bondy_oplog:compact(Id) of
    {ok, no_change} ->
        ok;
    {ok, {compacted, Watermark, EventCount}} ->
        ok;
    {error, no_crdt_module} ->
        %% Instance was started without a `crdt_module` opt.
        ok
end.
```

### Manual prefix truncation (advanced — lossy)

`bondy_oplog:truncate_prefix(Id, Watermark)` removes every event with
key `=< Watermark` from the live MST and advances
`current_watermark/1` to `Watermark` (monotonically). Subsequent peer
events with HLC `=< Watermark` are rejected by the receive-side
filter.

```erlang
Removed = bondy_oplog:truncate_prefix(Id, Watermark).
```

**This call is lossy.** Unlike `compact/1`, it does **not** write a
snapshot at the new watermark. Events between the previous snapshot's
watermark and `Watermark` are unrecoverable by a bootstrapping peer —
that peer would receive the older snapshot and then be rejected for
every event in the gap. Use this only when out-of-band coordination
has confirmed cluster-wide that the dropped events are safe to lose.
For coordinated retention with a recoverable snapshot, use `compact/1`.

### Default GC scheduler

```erlang
%% sys.config
[
    {bondy_mst, [
        {gc_scheduler, true},
        {gc_interval_ms, 1000}   %% default
    ]}
].
```

On each tick the scheduler invokes `compact/1` for every running instance.
Errors are logged and absorbed; one bad instance does not affect others.

### Inspecting compaction state

```erlang
Watermark               = bondy_oplog:current_watermark(Id).
{ok, Watermark, State}  = bondy_oplog:compaction_checkpoint(Id).
%%   or
not_found               = bondy_oplog:compaction_checkpoint(Id).
```

### Retention advice

`bondy_oplog:retention_advice(InstanceId)` returns a recommended
retention action for an instance based on its current write/segment
pressure, snapshot existence, outstanding scrubber alerts, and any
in-flight bootstrap consumers the caller knows about. The call is
advisory; it does not change state.

```erlang
{ok, #{recommended_action := Action,    %% compact | truncate_prefix | none
       rationale          := Rationale, %% binary; human-readable
       inputs             := Inputs}}
    = bondy_oplog:retention_advice(Id).

%% Cluster-supplied bootstrap-consumer count (the library does not
%% track active bootstrap sessions as durable state):
{ok, _Advice} = bondy_oplog:retention_advice(
    Id, #{bootstrap_consumers => 2}
).
```

Decision table:

| Scrubber alerts | Pressure  | Snapshot | Bootstrap consumers | Recommendation     |
|-----------------|-----------|----------|---------------------|--------------------|
| Outstanding     | any       | any      | any                 | `none`             |
| Clean           | low       | any      | any                 | `none`             |
| Clean           | non-low   | yes      | any                 | `compact`          |
| Clean           | non-low   | no       | > 0                 | `none`             |
| Clean           | non-low   | no       | 0                   | `truncate_prefix`  |

"Pressure" is the maximum of `bytes_total / max_total_wal_size` and
`live_segments_count / max_live_segments`; "low" is < 50 %.

Rationale by recommendation:

- **`none` (alerts outstanding)** — re-derive the affected segments
  (operator action: `bondy_oplog_wal:clear_segment_alert/2` once the
  segment has been replaced) or boot the instance with
  `recovery_mode => rescan` before reasoning about retention.
- **`none` (low pressure)** — nothing to do.
- **`none` (bootstrap, no snapshot)** — `truncate_prefix` would
  orphan the bootstrapping peers, and `compact` has nothing to fold.
  Wait for bootstrap to finish or take a snapshot first.
- **`compact`** — non-lossy; folds the prefix up to the snapshot
  watermark into the snapshot store, freeing WAL segments below it.
  Bootstrap consumers receive the snapshot and the events above it.
- **`truncate_prefix`** — lossy; drops events `=< Watermark`
  without a snapshot. Use only when out-of-band coordination has
  confirmed cluster-wide that the dropped events are safe to lose.

---

## Persistence

The library separates two storage concerns: the **MST backend** (page-level
key-value store) and the **snapshot store** (single-row durable state).

### MST backends

Configure via the `backend` opt at start_instance time:

| Backend | Module | Notes |
|---|---|---|
| `map` | `bondy_mst_map_store` | Pure functional map. Slow but simple; tests only. |
| `ets` | `bondy_mst_ets_store` | Default. Per-instance anonymous ETS. Read-concurrent. |
| `bondy_mst_pack_store` | `bondy_mst_pack_store` | Durable packfile-based store (git-style sorted-hash packs + fanout/bloom index). Production backend; selected by the **full module atom** (`map`/`ets` are the only shorthand atoms). See [`doc_extras/architecture/02_bondy_mst.md`](doc_extras/architecture/02_bondy_mst.md). |
| Custom | (any module) | Implement `bondy_mst_store` behaviour. Pass the module atom as `backend`. |

```erlang
%% In-RAM (default):
{ok, _} = bondy_oplog:start_instance(Id, #{
    backend         => ets,
    backend_options => #{name => <<"my-mst">>}
}).

%% Durable packfile backend (one directory per instance):
{ok, _} = bondy_oplog:start_instance(Id, #{
    backend         => bondy_mst_pack_store,
    storage_path    => <<"/var/lib/bondy_mst">>,
    path_layout     => sharded
}).
```

For other durable backends (RocksDB, custom KVs, …) implement
`bondy_mst_store` yourself; the framework treats it as opaque.

### Compaction checkpoints

Configure via the `compaction_checkpoint` opt (default is
context-sensitive: file-backed when `storage_path` is set, ETS
otherwise):

| Module | Durability |
|---|---|
| `bondy_oplog_compaction_checkpoint_ets` | In-memory. |
| `bondy_oplog_compaction_checkpoint_file` | File-backed. tmp + datasync + rename + fsync-dir on every write. |

```erlang
{ok, _} = bondy_oplog:start_instance(Id, #{
    crdt_module                => my_counter,
    compaction_checkpoint      => bondy_oplog_compaction_checkpoint_file,
    compaction_checkpoint_opts => #{path => <<"/var/lib/bondy_mst/checkpoints">>}
}).
```

A corrupted checkpoint surfaces as `{error, {corrupted, _}}` and the
instance refuses to start.
Crash-safe: a partial write produced by a VM crash leaves the previous
good file in place.

### HLC seeding on restart

When opening with a durable backend:

1. The HLC is seeded from the highest event key in the MST (or the
   snapshot's watermark, if the MST is empty).
2. The Seq counter is seeded from the highest local-Origin Seq in the MST.

Subsequent local appends are guaranteed to sort above any pre-restart event,
so the watermark filter never drops your own writes.

---

## Validators and Byzantine tolerance

A **validator** signs local events and verifies remote events. The framework
ships two:

### `bondy_oplog_validator_trust` (default)

No-op. Suitable for closed trusted clusters where every Origin is
friendly. Exports `is_stateless/0 -> true`, so `append/2,3` takes
the lock-free fast path
([Concurrency and the lock-free fast path](#concurrency-and-the-lock-free-fast-path)).

### `bondy_oplog_validator_crypto`

Ed25519 per-event signing with per-Origin hash chain. Suitable for
Byzantine-tolerant deployments. The per-Origin chain tail
(`last_hash`) mutates on every local sign, so the validator is
**stateful** — `append/2,3` routes through the instance gen_server
when this validator is configured.

```erlang
{Pub, Priv} = crypto:generate_key(eddsa, ed25519),
Origin      = crypto:hash(sha256, Pub),

{ok, _} = bondy_oplog:start_instance(Id, #{
    origin         => Origin,
    validator      => bondy_oplog_validator_crypto,
    validator_opts => #{
        keypair      => {Pub, Priv},
        peer_pubkeys => #{
            Origin     => Pub,
            PeerOrigin => PeerPub
        }
    },
    crdt_module    => my_counter
}).
```

### Opting a custom validator into the fast path

Consumer-supplied validators may export the optional
`is_stateless/0 -> boolean()` callback. Return `true` only when
`sign_event/2` is a pure function of its arguments — i.e. it
returns the same `{SignedEvent, State}` for the same `{Event,
State}` and never mutates any external state. Validators that
advertise `is_stateless/0 -> true` are eligible for the lock-free
`append/2,3` path which signs in the caller's process using a
cached, immutable validator state. The default (callback absent)
is `false` — signing is routed through the instance gen_server.

### Equivocation handling

If a malicious Origin signs two events at the same `{HLC, Origin, Seq}` with
different payloads (equivocation), and both reach an honest replica:

1. The pre-merge equivocation check detects the divergence.
2. The validator's `detect_equivocation/2` is invoked to produce a proof.
3. The proof is recorded in `bondy_oplog_quarantine` (node-shared
   ETS) keyed by `{instance_id, event_key}`.
4. The incoming event is rejected with `{error, equivocation_detected}`;
   the originally-stored event is preserved.

The instance gen_server **does not crash** — the strict-uniqueness merger
is bypassed for this case, so a malicious peer cannot DoS a replica.

### Banning origins

The library exposes the *mechanism*; **policy is the consumer's**. Inspect
quarantine, decide a threshold, ban manually:

```erlang
%% List proven equivocations:
Rows = bondy_oplog_quarantine:list_all().

%% Or per-instance:
Rows = bondy_oplog_quarantine:list_for_instance(Id).

%% Ban an origin (node-shared, applies across all instances):
ok = bondy_oplog_origin_bans:ban(Origin, {equivocation, Proof}).

%% Subsequent append_remote calls for events from this origin return
%% {error, banned_origin} without invoking the validator.

%% Lift the ban:
ok = bondy_oplog_origin_bans:unban(Origin).

%% Inspect:
ok       = bondy_oplog_origin_bans:is_banned(Origin),
BanList  = bondy_oplog_origin_bans:list().
```

Bans are node-shared because Origin identifies the *replica*, not a
per-instance role: an Origin malicious for one instance is malicious for all.

---

## Operations

### Node-shared registry tables

The library owns several node-shared ETS tables, all owned by the
`bondy_oplog_sup` supervisor tree:

| Module | Holds |
|---|---|
| `bondy_oplog_registry` | per-instance read snapshot: `pid`, `origin`, `mst handle`, `watermark`, `snapshot`, `crdt_module`, `live_size`. Hot-path read substrate. |
| `bondy_oplog_peer_state` | per-`(peer, instance)`: most recent confirmed root hash, `last_sync`, `last_seen`. Drives the stability frontier. |
| `bondy_oplog_quarantine` | detected equivocations: `{instance_id, event_key} → {E1, E2, Proof}`. |
| `bondy_oplog_origin_bans` | banned origins: `Origin → {Reason, Proof, BannedAt}`. |

All four expose a `list*`/`info` accessor for ops dashboards (e.g.
`bondy_oplog_registry:list/0`, `bondy_oplog_quarantine:list_all/0`).

### Telemetry events

The library emits the following telemetry events:

```
[bondy_oplog, instance, append]                       %% local append
[bondy_oplog, instance, apply_event, ok]              %% event installed (local or accepted remote)
[bondy_oplog, instance, append_remote, filtered]      %% below watermark
[bondy_oplog, instance, append_remote, banned]        %% origin banned
[bondy_oplog, instance, append_remote, equivocation]  %% divergence detected
[bondy_oplog, instance, backpressure]                 %% working-set cap hit
[bondy_oplog, instance, overlay, backpressure_drop]   %% overlay cap hit
[bondy_oplog, instance, mst_install]                  %% events installed into the MST
[bondy_oplog, instance, write_latency]                %% per-instance write→readable latency (see ch.04)
[bondy_oplog, sync, ok | error]                       %% sync session outcome
[bondy_oplog, compaction, ok]                         %% compaction cycle
[bondy_oplog, scheduler, sync, tick]
[bondy_oplog, scheduler, gc, tick]
[bondy_oplog, applier, ...]                           %% per-batch stage timings (batch_verify/fold/cell_apply/publish/install_cast, applied, …)
```

This is the main set; the substrate also emits `[bondy_oplog, sync_scheduler, …]`
(bootstrap dispatch), `[bondy_oplog, secondary_writer, …]`, and `[bondy_mst,
page_store, …]` families. Each event carries `instance_id` (and other context)
in metadata. Attach a handler via `telemetry:attach/4` (or a prefix handler on
`[bondy_oplog]` / `[bondy_mst]`) for metrics, alerting, or debug logging.

### Disabling the schedulers

For consumers that want to drive sync and compaction themselves, disable the
defaults:

```erlang
%% sys.config
[
    {bondy_mst, [
        {sync_scheduler, false},
        {gc_scheduler, false}
    ]}
].
```

The scheduler gen_servers still start (cheap), but their tick timers stay
quiescent. Trigger manually via `bondy_oplog:sync/2,3` and
`bondy_oplog:compact/1`.

---

## Configuration reference

### `start_instance/2` opts

| Key | Default | Meaning |
|---|---|---|
| `origin` | `bondy_oplog_origin:default()` | Per-replica binary id. Override for stable cross-restart identity. |
| `backend` | `ets` | `map` \| `ets` \| custom module implementing `bondy_mst_store`. |
| `backend_options` | `#{}` | Backend-specific options. |
| `storage_path` | `undefined` | Base dir for durable backends; combined with `path_layout`. |
| `path_layout` | `sharded` | `flat` (small fixed sets) or `sharded` (millions of instances). See `bondy_oplog_path`. |
| `hash_algorithm` | `sha256` | MST page hashing. |
| `validator` | `bondy_oplog_validator_trust` | Event signer/verifier. |
| `validator_opts` | `#{}` | Opts passed to the validator's `init/2`. |
| `fold_module` | `undefined` | **Legacy alias** for `crdt_module`. An atom shorthand with a native twin (`lww_register`, `g_counter`, `pn_counter`, `g_set`, `max_register`, `min_register`, `index_entry`) resolves to its byte-identical CRDT. Shorthands with no twin (`presence_basic`, `strict_register`, `orset`, `ttl_presence`, `map_of_fields`) were retired and now error. See [The CRDT model](doc_extras/architecture/05_crdt_model.md). |
| `fold_opts` | `#{}` | Opaque options threaded through with the legacy label. |
| `crdt_module` | `undefined` | Required for `compact/1` and `query/2`. |
| `compaction_checkpoint` | context-sensitive | `_file` when `storage_path` is set, `_ets` otherwise. |
| `compaction_checkpoint_opts` | `#{}` | E.g. `#{path => <<"...">>}` for `_file`. |
| `max_working_set` | `infinity` | Cap on live events; `append` returns `working_set_full` past it. |
| `max_overlay_events` | `10_000` | Overlay backpressure cap (events). `append*` returns `{error, backpressure}` past it. |
| `max_overlay_bytes` | `5 * 1024 * 1024` | Overlay backpressure cap (bytes). `append*` returns `{error, backpressure}` past it. |
| `overlay_throttle` | `drop` | Behaviour on overlay-cap breach. Only `drop` is currently supported. |
| `hlc_seed` | `0` | Initial HLC value. Auto-seeded from MST/snapshot at init when applicable. |
| `seq_seed` | `0` | Initial Seq value. Auto-seeded from MST at init. |
| `applier` | `#{}` | Per-instance applier tuning. Recognised keys: `commit_every` (default `64`), `poll_interval_ms` (default `5`). See [The applier](doc_extras/architecture/04_applier.md). |
| `wal_backend` | `disk` | `disk` (segment files) or `mem` (in-memory ETS WAL, fused-only) — the ephemeral/fast path. |
| `fused` | `false` | When `true`, the instance drains its own WAL and installs inline (no separate applier hop) — the ephemeral high-throughput mode. |
| `install_coalesce_max` | `16` | Max install batches the instance coalesces per cycle. |

### WAL options (instance-level)

The same opts map accepts WAL-writer configuration. Defaults are
production-safe; tune only when you have a workload reason. See
`bondy_oplog_wal` moduledoc for the full list.

| Key | Default | Meaning |
|---|---|---|
| `wal_dir` | derived from `storage_path` or `/tmp/bondy_oplog_wal/<os_pid>/` | Base directory under which the WAL writer creates `<InstanceId>/`. |
| `fsync_mode` | `per_write` | `per_write` (every successful `append` is durable) or `batched` (durability synchronised via `bondy_oplog_wal:await_durable/3`). |
| `max_segment_bytes` | 64 MiB | Rotate the head segment past this size. |
| `max_batch_bytes` | 4 MiB | Cap on the body bytes of a single batch frame. |
| `idx_interval_bytes` | 64 KiB | Sparse `.qidx` granularity. |
| `batched_fsync_interval` / `batched_fsync_bytes` | — | Fsync trigger thresholds when `fsync_mode = batched`. |
| `min_live_segments` | 2 | Floor on the number of live segments retained. |
| `retention_sweep_interval` | 300000 ms (5 min) | How often retention runs. |
| `max_total_wal_size` / `max_live_segments` | — | Soft caps the retention sweep honours. |
| `recovery_mode` | `strict` | `strict` (refuse to advance past a corrupt frame; needs operator action) or `rescan` (best-effort drop of corrupt frames during head-segment recovery). |
| `body_compression` | `disabled` | Per-frame body compression (`zlib` / `lz4`). |
| `body_encryption` | `disabled` | Per-frame body encryption envelope; key resolution module-supplied. |

### App env

| Key | Default | Meaning |
|---|---|---|
| `sync_scheduler` | `true` | Enable default sync scheduler. |
| `sync_interval_ms` | `500` | Tick interval. |
| `peer_source` | `bondy_oplog_peer_source_static` | Default peer-discovery module. |
| `peer_source_opts` | `#{}` | Default peer-source options. |
| `sync_dispatch` | spawn-per-peer | Override the per-tick dispatch fn. |
| `gc_scheduler` | `true` | Enable default GC scheduler. |
| `gc_interval_ms` | `1000` | Tick interval. |
| `gc_trigger` | `compact/1` | Override the per-tick trigger fn. |
| `peer_timeout_ms` | `30000` | Peer staleness threshold for compaction. |

---

## Behaviour reference

| Behaviour | Purpose |
|---|---|
| `bondy_oplog_crdt` | Consumer-defined operation-based CRDT semantics (`interpret_cog/2` + `query/2` + the projection seam). The full pure op-based catalogue ships natively (registers, counters, g/2P/add-wins/remove-wins sets, multi-value register, add-wins map, enable/disable-wins flags) — see [The CRDT model](doc_extras/architecture/05_crdt_model.md). |
| `bondy_oplog_crdt_commutative` | The eager single-operation step (`apply_op/3·4`) + a generic sort-and-fold `interpret_cog` for commutative CRDTs. |
| `bondy_oplog_validator` | Sign local events; verify remote events; detect equivocation. |
| `bondy_oplog_peer_source` | Per-instance peer discovery. |
| `bondy_oplog_transport` | Network transport for sync sessions. |
| `bondy_oplog_compaction_checkpoint` | Durable storage of compaction checkpoints. |
| `bondy_mst_store` | MST page-level storage backend. |
| `bondy_oplog_projection_adapter` | Pluggable materialised-cell store under `bondy_oplog_core` (the canonical implementation is `bondy_db_projection_leveled`). |
| `bondy_oplog_cache_adapter` | Pluggable read cache under `bondy_oplog_core` (ETS reference impl: `bondy_oplog_cache_ets`). |
| `bondy_db_topology` | How `bondy_db` tables map onto shards. Four ship: `single_bookie`, `per_entity`, `shared_shards`, and `memory` (in-memory ETS projection, the ephemeral-table substrate). |

Each behaviour is documented in its source module.

---

## The MST primitive

For consumers that want the underlying Merkle Search Tree directly without
the replication layer, use `bondy_mst`:

```erlang
T0 = bondy_mst:new(#{
    store          => bondy_mst_ets_store,
    store_opts     => #{name => <<"mytree">>},
    hash_algorithm => sha256
}),
T1 = bondy_mst:put(T0, <<"k1">>, <<"v1">>),
T2 = bondy_mst:put(T1, <<"k2">>, <<"v2">>),
<<"v1">> = bondy_mst:get(T2, <<"k1">>),
RootHash = bondy_mst:root(T2).
```

For bulk inserts, prefer `put_batch/2`:

```erlang
T = bondy_mst:put_batch(T0, [
    {<<"k1">>, <<"v1">>},
    {<<"k2">>, <<"v2">>},
    {<<"k3">>, <<"v3">>}
]).
```

`put_batch/2` builds a small volatile in-process MST from the input
pairs and merges it into the receiver in a single tree traversal —
one spine rebuild for the whole batch instead of one per entry. For
batches larger than a few items it is several times faster than the
equivalent `lists:foldl(fun put/3, T, Items)`; the receiver's
`comparator`, `merger`, and `hash_algorithm` are used, and collisions
with existing keys invoke the configured merger exactly as `put/3`
would. For `N=1` it falls through to `put/3` with no overhead.

API surface includes `put/3`, `put_batch/2`, `get/2,3`, `delete/2`,
`merge/2,3`, `missing_set/2`, `fold/3,4`, `first/1`, `last/1`,
`to_list/1`, `diff_to_list/2`, `gc/1,2`, etc.

This is the building block. The replication layer is built on top.

---

## Installation

### Requirements

- Erlang/OTP 27+ (the codebase uses triple-quoted docstrings and the
  `-doc` attribute).

### rebar.config

```erlang
{deps, [
    {bondy_mst, {
        git, "https://github.com/bondy-io/bondy_mst.git", {branch, "main"}
    }}
]}.
```

### Application start

The library is an OTP application. Its supervision tree starts on
`application:ensure_all_started(bondy_mst, permanent)` and brings up the
node-shared registries, the responder, and the schedulers.

---

## Jepsen

A 3-node Jepsen integration exercises 1 namespace × 10 tables ×
16 leveled-backed shards per table across Distributed Erlang. It is
split into two pieces, both under `jepsen/`:

- `jepsen/bondy_mst_jepsen/` — Erlang OTP wrapper (HTTP shim,
  disterl-cluster wiring, smoke test). A **sibling rebar3 project**
  that depends on this library via a `_checkouts/bondy_mst` symlink
  to the repo root, so the library's own build never pulls in
  Cowboy / jsx.
- `jepsen/jepsen.bondymst/` — Jepsen test driver (Clojure / Leiningen).

See [`jepsen/jepsen.bondymst/README.md`](jepsen/jepsen.bondymst/README.md)
for the full run flow:

```sh
make rel-jepsen     # build the Linux release in a one-shot Docker
make jepsen-up      # 1 control + 3 nodes via docker-compose
docker exec -it jepsen-control bash
cd /root/jepsen.bondymst
lein run test --nodes n1,n2,n3 \
  --ssh-private-key /root/shared/jepsen-bot \
  --workload set --crdt-module aw_set \
  --nemesis random-partition-halves \
  --time-limit 60 --concurrency 10 --rate 10
```

---

## Credits

This library builds directly on two pieces of academic work, and would
not exist without them:

- **Alex Auvolat** and **François Taïani** (Univ. Rennes, Inria, IRISA,
  CNRS), *"Merkle Search Trees: Efficient State-Based CRDTs in Open
  Networks"*, **SRDS 2019**.
  [Inria HAL-02303490](https://inria.hal.science/hal-02303490/document)
  · [Reference Elixir
  prototype](https://gitlab.inria.fr/aauvolat/mst_exp).
  This is the source of the MST construction used by the `bondy_mst`
  primitive.

- **Preston McCrary** (UC Berkeley), *"Canteen: A Partially-Ordered Log
  Abstraction for the Emerging CRDT Datastore"*, Master's thesis,
  **2022**.
  [EECS-2022-160](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2022/EECS-2022-160.html).
  This is the source of the COG abstraction, the operation-log
  framing, and the Byzantine-fault-tolerance approach via
  hash-chaining used by `bondy_oplog`. We replace Canteen's DAG with
  an MST as the underlying log substrate, trading the DAG's explicit
  causal edges for the MST's deterministic key order plus efficient
  set-reconciliation anti-entropy.

Any errors in this library's interpretation or adaptation of the above
work are ours, not theirs.

---

## License

Apache License 2.0.

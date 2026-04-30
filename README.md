# bondy_mst

An Erlang/OTP library for building **coordination-free, eventually-consistent
replicated data stores** on top of Merkle Search Trees.

A *CRDT* (Conflict-free Replicated Data Type) is a data type whose
operations can be applied in any order on any replica and still converge
to the same result. CRDTs sidestep consensus protocols (Raft, Paxos) by
trading strong consistency for *Strong Eventual Consistency* — replicas
that have delivered the same operations end up in the same state, with
no coordination required during writes.

The library has two layers, used independently or together:

1. **`bondy_mst`** — a Merkle Search Tree (MST) primitive (Auvolat &
   Taïani, 2019). A balanced, content-addressed search tree where the
   structural shape is determined deterministically by item hashes, so
   two replicas with the same set of items have the same root hash.
   Useful as a building block for anti-entropy and integrity
   verification.
2. **`bondy_oplog`** — a coordination-free CRDT replication
   *framework* built on top. Each replicated value is an **instance**:
   an append-only event log keyed by `{HLC, Origin, Seq}`, stored as an
   MST. Replicas synchronise by exchanging missing MST pages; stable
   prefixes collapse into snapshots through a consumer-defined
   `interpret_cog/2` function. The framework is agnostic to event
   payload semantics, transport (Distributed Erlang, Partisan, gRPC,
   ...), durability (in-memory, file, RocksDB, ...), and trust model
   (closed cluster, Byzantine-tolerant).

The replication layer is the modern API and the focus of this README.
The MST primitive is reused under the hood.

If "MST", "COG", and "CRDT" don't already mean something specific to
you, read the [Background](#background-msts-and-cogs) section before
the Quick Start. The rest of this README assumes those concepts.

---

## Table of Contents

- [When to use this library](#when-to-use-this-library)
- [Background: MSTs and COGs](#background-msts-and-cogs)
- [Quick start](#quick-start)
- [Concepts](#concepts)
- [Defining a CRDT](#defining-a-crdt)
- [Lifecycle](#lifecycle)
- [Writing events](#writing-events)
- [Reading and querying](#reading-and-querying)
- [Replication](#replication)
- [Compaction and snapshots](#compaction-and-snapshots)
- [Persistence](#persistence)
- [Validators and Byzantine tolerance](#validators-and-byzantine-tolerance)
- [Operations](#operations)
- [Configuration reference](#configuration-reference)
- [Behaviour reference](#behaviour-reference)
- [The MST primitive](#the-mst-primitive)
- [Installation](#installation)

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

## Background: MSTs and COGs

This library combines two ideas: **Merkle Search Trees** for efficient
anti-entropy, and **Concurrent Operation Groups** for bounded log size.
Understanding both makes the rest of the document much clearer.

### Merkle Search Trees (MSTs)

A Merkle Search Tree is a balanced search tree in which the *shape* of
the tree is determined by the *content* of the items, not by their
insertion order. The construction (see Auvolat & Taïani's
[2019 paper](https://inria.hal.science/hal-02303490/document)) uses
item hashes to deterministically pick which layer each item lives on:

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
| `causal_tier/0` | yes | Return `tier_0`, `tier_1`, or `tier_2` (informational; see _design/2_mst_causal_clocks.md). |
| `init/0` | yes | Bottom state — what the CRDT looks like when no events have ever been applied. |
| `interpret_cog/2` | yes | `(Events, State) -> NewState`. Given a batch of events in key order, return the updated state. **Must be deterministic** — same inputs ⇒ same output on every replica. This is the foundation of convergence. |
| `query/2` | yes | `(Query, State) -> Result`. Project the state for client queries. Pure. |
| `state_to_ops/2` | optional | For PUT-style APIs that diff states into op lists. |
| `merge_values/3` | optional | For CRDT-valued events that need value-level merging (vs. the strict-uniqueness default). |

### Determinism is non-negotiable

`interpret_cog/2` must be a pure deterministic function. Replicas that
received the same events will produce different snapshots otherwise, and the
system will silently diverge. No timestamps from `os:system_time/1`, no
`rand`, no environment lookups, no message passing.

---

## Lifecycle

```erlang
%% Start with defaults (in-memory, trust validator, ETS snapshot store).
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
{ok, Watermark, State}  = bondy_oplog:snapshot(Id).
%%   or
not_found               = bondy_oplog:snapshot(Id).
```

---

## Persistence

The library separates two storage concerns: the **MST backend** (page-level
key-value store) and the **snapshot store** (single-row durable state).

### MST backends

Configure via the `backend` opt at start_instance time:

| Backend | Module | Notes |
|---|---|---|
| `map` | `bondy_mst_map_store` | Pure functional map. Slow but simple. |
| `ets` | `bondy_mst_ets_store` | Default. Per-instance anonymous ETS. Read-concurrent. |
| Custom | (any) | Implement `bondy_mst_store` behaviour. Pass the module atom as `backend`. |

```erlang
{ok, _} = bondy_oplog:start_instance(Id, #{
    backend         => ets,
    backend_options => #{persistent => true}
}).
```

For durable backends (RocksDB, leveled, ...) implement `bondy_mst_store`
yourself; the framework treats it as opaque.

### Snapshot stores

Configure via the `snapshot_store` opt:

| Module | Durability |
|---|---|
| `bondy_oplog_snapshot_store_ets` | In-memory. Default. |
| `bondy_oplog_snapshot_store_file` | File-backed. Atomic rename on every write. |

```erlang
{ok, _} = bondy_oplog:start_instance(Id, #{
    crdt_module         => my_counter,
    snapshot_store      => bondy_oplog_snapshot_store_file,
    snapshot_store_opts => #{path => <<"/var/lib/bondy_mst/snapshots">>}
}).
```

The file-backed store uses `file:write_file → file:rename` (POSIX-atomic).
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

No-op. Suitable for closed trusted clusters where every Origin is friendly.

### `bondy_oplog_validator_crypto`

Ed25519 per-event signing with per-Origin hash chain. Suitable for
Byzantine-tolerant deployments.

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

All four expose `info/0` or `list/0` for ops dashboards.

### Telemetry events

The library emits the following telemetry events:

```
[bondy_oplog, instance, append]                       %% local append
[bondy_oplog, instance, append_remote, ok]            %% accepted remote
[bondy_oplog, instance, append_remote, filtered]      %% below watermark
[bondy_oplog, instance, append_remote, banned]        %% origin banned
[bondy_oplog, instance, append_remote, equivocation]  %% divergence detected
[bondy_oplog, instance, backpressure]                 %% working set cap hit
[bondy_oplog, sync, ok]                               %% sync session success
[bondy_oplog, sync, error]                            %% sync session failed
[bondy_oplog, compaction, ok]                         %% compaction cycle
[bondy_oplog, scheduler, sync, tick]
[bondy_oplog, scheduler, gc, tick]
```

Each carries `instance_id` (and other context) in metadata. Hook a handler
via `telemetry:attach/4` for metrics, alerting, or debug logging.

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
| `storage_path` | `undefined` | Base dir for durable backends; combined with `path_strategy`. |
| `path_strategy` | `bondy_oplog_path_sharded` | `_flat` (small fixed sets) or `_sharded` (millions of instances). |
| `hash_algorithm` | `sha256` | MST page hashing. |
| `validator` | `bondy_oplog_validator_trust` | Event signer/verifier. |
| `validator_opts` | `#{}` | Opts passed to the validator's `init/2`. |
| `merge_strategy` | `bondy_oplog_merge_strict_uniqueness` | Resolves rare merge collisions. |
| `crdt_module` | `undefined` | Required for `compact/1` and `query/2`. |
| `snapshot_store` | `bondy_oplog_snapshot_store_ets` | In-memory or `_file`. |
| `snapshot_store_opts` | `#{}` | E.g. `#{path => <<"...">>}` for `_file`. |
| `max_working_set` | `infinity` | Cap on live events; `append` returns `working_set_full` past it. |
| `hlc_seed` | `0` | Initial HLC value. Auto-seeded from MST/snapshot at init when applicable. |
| `seq_seed` | `0` | Initial Seq value. Auto-seeded from MST at init. |

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
| `bondy_oplog_crdt` | Consumer-defined CRDT semantics. |
| `bondy_oplog_validator` | Sign local events; verify remote events; detect equivocation. |
| `bondy_oplog_merge_strategy` | Resolve rare value collisions at the same event key. |
| `bondy_oplog_peer_source` | Per-instance peer discovery. |
| `bondy_oplog_transport` | Network transport for sync sessions. |
| `bondy_oplog_snapshot_store` | Durable storage of CRDT snapshots. |
| `bondy_oplog_path_strategy` | On-disk layout for durable backends. |
| `bondy_mst_store` | MST page-level storage backend. |

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

API surface includes `put/3`, `get/2,3`, `delete/2`, `merge/2,3`,
`missing_set/2`, `fold/3,4`, `first/1,2`, `last/1,2`, `to_list/1`,
`diff_to_list/2`, `gc/1,2`, etc.

This is the building block. The replication layer is built on top.

---

## Installation

### Requirements

- Erlang/OTP 26+

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

## License

Apache License 2.0.

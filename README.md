# bondy_mst

An OTP application that implements a Merkle Search Tree (MST), a probabilistic 
data structure optimised for efficient storage and retrieval of key-value pairs
proposed and demonstrated by Alex Auvolat and François Taïani in ther paper
[Merkle Search Trees: Efficient State-Based CRDTs in
Open Networks](https://inria.hal.science/hal-02303490/document).

The implementation is based the original [Elixir prototype](https://gitlab.inria.fr/aauvolat/mst_exp) but extended with many new features:

* Support for multiple backends (see [Backends](#backends) section)
* Configurable automatic garbage collection
* 

## MSTs
An MST is a balanced search tree from a set of items and encodes it as a Merkle Tree. MSTs can be used to build causally consistent stores that ensure eventual delivery of data to all connected replicas.

MSTs have the following characteristics:
- Deterministic contruction - the MST algorithm produces a single possible
representation for a given set of items, regardless of insertion order
- Self-balancing
- Sorted - Keys are sorted lexicographically
- Efficient key-value insertion and retrieval
- Merkle-based verification for integrity checks

## Backends
Backends implement the `bondy_mst_store` behaviour.

The following table shows the existing backends and their capabilities:

|Module|Type|Read Concurrency|Transactions|GC|
|---|---|---|---|---|
|`bondy_mst_map_store`|Dedicated `map()`|No|No|Epoch-based|
|`bondy_mst_ets_store`|Dedicated `ets` table|Yes|No|Epoch-based|
|`bondy_mst_leveled_store`|Named tree on shared `leveled` instance|No|No|Epoch-based|
|`bondy_mst_rocksdb_store`|Named tree on shared `rocksdb` instance|Yes|Yes (Optimistic)|Epoch-based|

Note: `bondy_mst_rocksdb_store` which is packaged in a separate library to avoid the rocksdb dependency on this application. This implementation allows for concurrent reads and uses RocksDB transactions when writing.

## CRDTs
The application provides an implementation of a CRDT that can behave as a Grow-only Set, CRDT Map or KV Store via the `bondy_mst_crdt` module. This provides the core logic required to implement a CRDT `gen_server` (or `gen_statem`), offering callbacks to fine-tune how the merging works. It takes care of the rest including, merges and anti-entropy sync exchanges. It makes no assumption as to the underlying networking you want to use, so it can be used with Distributed Erlang, [Partisan](https://partisan.dev/) or any other alternative.

## TODOs
[] Support `delete`
[] Range queries
[] Support configurable page serialisation e.g. JSON, CBOR for cross platform MSTs


# Requirements
* Erlang/OTP 26+

# Getting started

The latest version of the library is available at its `main` branch. All development, including new features and bug fixes, take place on the main branch using forking and pull requests as described in contribution guidelines.

Add the library as dependency to `rebar.config`

```erlang
{deps, [
    {bondy_mst,{
        git, "https://github.com/bondy-io/bondy_mst.git", {branch, "main"}
    }}
]}
```


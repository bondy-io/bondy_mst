# bondy_mst

An OTP application that implements a Merkle Search Tree as described in the 2019 paper [Merkle Search Trees: Efficient State-Based CRDTs in Open Networks](https://inria.hal.science/hal-02303490) by Alex Auvolat and François Taïani.

The implementation extends the original [Elixir prototype](https://gitlab.inria.fr/aauvolat/mst_exp) to support multiple backends, with some backends offering read concurrency through persistent data structures (path copying and epoch-based pruning). 

The library also provides `bondy_mst_grove` that is the core logic required to implement a sychronised group of MST replicas across a cluster. This module implements a the logic for anti-entropy exchanges and makes no assumption as to the underlying networking so it can be used with Distributed Erlang, [Partisan](https://partisan.dev/) or any other alternative.

## Backends
Backends implement the `bondy_mst_store` behaviour which offers support for transactions.

The following table shows the existing backends and their capabilities:

|Module|Type|Read Concurrency|Transactions|GC|
|---|---|---|---|---|
|`bondy_mst_map_store`|Dedicated `map()`|No|No|Epoch-based|
|`bondy_mst_ets_store`|Dedicated `ets` table|Yes|No|Epoch-based|
|`bondy_mst_rocksdb_store`|Named tree on shared `rocksdb` instance|Yes|Yes (Optimistic)|Epoch-based|
|`bondy_mst_leveled_store`|Named tree on shared `leveled` instance|No|No|Epoch-based|

# Requirements
* Erlang/OTP 26+

# Getting started

The latest version of the library is available at its main branch. All development, including new features and bug fixes, take place on the main branch using forking and pull requests as described in contribution guidelines.

Add the library as dependency to rebar.config

```erlang
{deps, [
    {bondy_mst,{
        git, "https://github.com/bondy-io/bondy_mst.git", {branch, "main"}
    }}
]}
```


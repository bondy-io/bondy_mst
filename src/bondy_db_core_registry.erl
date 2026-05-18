%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_db_core_registry).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Node-shared registry of per-`(namespace, index, shard)` triples for
`bondy_db_core` (`MST_DB_DESIGN.md` §3, §5, §6).

Each shard publishes one entry containing the handles `bondy_db_core`
needs to satisfy a read:

| Field | Source |
|---|---|
| `shard_count` | namespace configuration |
| `cache_adapter` + `cache_handle` | owner's cache adapter init |
| `projection_adapter` + `projection_handle` | owner's projection open |
| `overlay` | owner's `bondy_oplog_db_overlay:new/0` |
| `fold_module` | namespace's fold strategy |

The table is a single `public set` ETS owned by this gen_server. Reads
go directly to ETS (`lookup/3` is the hot path; no roundtrip).
`register/4` and `unregister/3` are `gen_server:call/2` so the server
can monitor the registering process and tear the row down if the owner
dies. The hot read path remains lock-free.

## Restart semantics

The ETS table is owned by this gen_server; if the gen_server dies the
table dies with it. On supervisor restart, `init/1` creates a fresh
empty table — **all in-memory monitor state is lost and previously
registered shards are orphaned** (their atomics refs still exist, but
the registry has no row pointing to them). Subsequent `lookup/3` calls
will return `not_found` until owners re-register.

There is currently no recovery protocol: owners are not signalled when
the registry restarts. Operators should either set the supervisor's
`intensity` so the registry effectively never restarts, or wire the
applier to periodically validate its own registrations and re-register
on `not_found`. The substrate does not police this.

## Owner DOWN cleanup

When an owner dies, the registry deletes the ETS row and removes the
monitor. It does **not** call `close/1` on the cache, projection, or
overlay adapters — those handles were created by (and may be tied to
the lifecycle of) the owner process. For ETS-based adapters this is
correct: ETS reclaims tables owned by the dead process. Adapters that
own external resources (file handles, sub-processes, connection
pools) MUST set up their own owner-monitoring inside the adapter — the
substrate guarantees registry-row cleanup only.

## Why a separate registry from `bondy_oplog_registry`

`bondy_oplog_registry` is per-instance (effectively per-namespace —
the existing substrate uses `instance_id` as the namespace). The
MST_DB read API needs a richer key: `(namespace, index, shard)` —
indexes (primary and secondaries) are a new dimension introduced in
`MST_DB_DESIGN.md` and not present in `bondy_oplog_instance`. Keeping
the registries separate avoids retrofitting `bondy_oplog_registry`'s
record with index/shard fields that would be `undefined` for the
99% of consumers that have not opted into the read-side projection.

## Why ETS, not persistent_term

`persistent_term:put/2` triggers a global GC scan on every process on
the node. With many shards doing many config refreshes (e.g., a
secondary's lag bound changing), that's a non-starter. ETS `insert` is
constant-time, no global side effects, and `read_concurrency: true`
keeps reads parallel.
""").

-define(TABLE, bondy_db_core_registry_tab).

-record(entry, {
    key                :: shard_key(),
    shard_count        :: pos_integer(),
    cache_adapter      :: module(),
    cache_handle       :: term(),
    projection_adapter :: module(),
    projection_handle  :: term(),
    overlay            :: bondy_oplog_db_overlay:tid() | undefined,
    fold_module        :: bondy_oplog_fold:strategy(),
    %% Per-shard freshness counter, written by the applier on each
    %% projection commit (or by anti-entropy on each successful round).
    %% Stored as `monotonic_time(millisecond)`; read wait-free by
    %% `ensure_fresh/2` (`MST_DB_DESIGN.md` §11).
    ae_atomics         :: atomics:atomics_ref(),
    %% Per-namespace policy (§15). `ap` (default) places no constraint
    %% on reads; `cp` rejects `eventual`-consistency batch reads to
    %% prevent unfenced staleness. Owners pass this on `register/4`;
    %% the substrate trusts the value to be consistent across shards
    %% of the same namespace (consumer responsibility).
    consistency_class  :: ap | cp
}).

-record(state, {
    %% MonitorRef -> shard_key()
    mon_to_key = #{} :: #{reference() := shard_key()},
    %% shard_key() -> MonitorRef
    key_to_mon = #{} :: #{shard_key() := reference()},
    %% Fresh `make_ref()` per gen_server start. Exposed via
    %% `current_epoch/0` and broadcast on `bondy_db_core_events`
    %% under topic `bondy_db_core_registry_started`. Owners cache the
    %% epoch and treat a change as "registry was restarted; re-register".
    epoch :: reference()
}).

-type shard_key()   :: {atom(), atom(), non_neg_integer()}.
-type shard_entry() :: #entry{}.
-type config()      :: #{
    shard_count := pos_integer(),
    cache_adapter := module(),
    cache_handle := term(),
    projection_adapter := module(),
    projection_handle := term(),
    fold_module := bondy_oplog_fold:strategy(),
    overlay => bondy_oplog_db_overlay:tid(),
    %% Optional. If absent, the registry allocates a single-counter
    %% atomics ref on register. Owners that want shared accounting
    %% (e.g., across a hot/cold reload) can pass their own ref.
    ae_atomics => atomics:atomics_ref(),
    %% Optional. Pid the registry will monitor; when this process exits
    %% the registration is torn down automatically. Defaults to the
    %% calling process.
    owner => pid(),
    %% Optional. Per-namespace consistency policy (`MST_DB_DESIGN.md`
    %% §15). Defaults to `ap`. See `read_batch/2` for the enforcement
    %% rule.
    consistency_class => ap | cp
}.

-export_type([shard_entry/0, config/0]).

-export([child_spec/0]).
-export([start_link/0]).

-export([register/4]).
-export([unregister/3]).
-export([lookup/3]).
-export([shard_count/2]).
-export([list/0]).

%% Restart-recovery protocol (`MST_DB_DESIGN.md` §11.1, §18 item 11).
-export([current_epoch/0]).

%% Freshness (`MST_DB_DESIGN.md` §11).
-export([bump_ae/3]).
-export([bump_ae/4]).
-export([bump_ae_targets/1]).
-export([bump_ae_targets/2]).
-export([last_ae_at/3]).
-export([shards_for/1]).
-export([namespaces/0]).

%% Field accessors (so callers do not need the header).
-export([entry_key/1]).
-export([entry_cache_adapter/1]).
-export([entry_cache_handle/1]).
-export([entry_projection_adapter/1]).
-export([entry_projection_handle/1]).
-export([entry_overlay/1]).
-export([entry_fold_module/1]).
-export([entry_shard_count/1]).
-export([entry_ae_atomics/1]).
-export([entry_consistency_class/1]).

%% Namespace-level consistency_class lookup (`MST_DB_DESIGN.md` §15).
-export([consistency_class/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% =============================================================================
%% API
%% =============================================================================

child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-spec register(
    Namespace :: atom(),
    Index :: atom(),
    Shard :: non_neg_integer(),
    Config :: config()
) -> ok | {error, {missing_required_field, atom()}}.

register(NS, Index, Shard, Config)
        when is_atom(NS), is_atom(Index), is_integer(Shard), Shard >= 0,
             is_map(Config) ->
    %% Validate required keys here, before the gen_server call. A bad
    %% config crashing inside the gen_server would wipe the monitor
    %% bookkeeping for every other registration on the node — a
    %% single misconfigured call is not allowed to take the substrate
    %% down with it.
    case validate_config(Config) of
        ok ->
            Owner = maps:get(owner, Config, self()),
            gen_server:call(?MODULE, {register, NS, Index, Shard, Owner, Config});
        {error, _} = Err ->
            Err
    end.


-spec unregister(atom(), atom(), non_neg_integer()) -> ok.

unregister(NS, Index, Shard) ->
    gen_server:call(?MODULE, {unregister, {NS, Index, Shard}}).


-doc("""
Return the current epoch reference. A new epoch is allocated on each
gen_server start and broadcast on
`bondy_db_core_events:notify(bondy_db_core_registry_started, Epoch)`.
Owners cache the epoch they last saw and treat any change as
"registry was restarted; re-register every shard I own".
""").
-spec current_epoch() -> reference().

current_epoch() ->
    gen_server:call(?MODULE, current_epoch).


-spec lookup(atom(), atom(), non_neg_integer()) ->
    {ok, shard_entry()} | not_found.

lookup(NS, Index, Shard) ->
    case ets:lookup(?TABLE, {NS, Index, Shard}) of
        [#entry{} = E] -> {ok, E};
        [] -> not_found
    end.


-spec shard_count(atom(), atom()) -> {ok, pos_integer()} | not_found.

shard_count(NS, Index) ->
    MS = [{
        #entry{
            key = {NS, Index, '_'},
            shard_count = '$1',
            _ = '_'
        },
        [],
        ['$1']
    }],
    case ets:select(?TABLE, MS, 1) of
        {[Count], _} -> {ok, Count};
        '$end_of_table' -> not_found
    end.


-spec list() -> [shard_entry()].

list() ->
    ets:select(?TABLE, [{'_', [], ['$_']}]).


-doc("""
Record on the shard's atomics counter that the shard has just had a
fresh round of applier activity (or anti-entropy convergence). Wait-free.

Uses `erlang:monotonic_time(millisecond)` as the bump timestamp. For
applier loops that bump several shards in one logical step and want
to reuse the same "now" across them, see `bump_ae/4`.
""").
-spec bump_ae(atom(), atom(), non_neg_integer()) -> ok | not_found.

bump_ae(NS, Index, Shard) ->
    bump_ae(NS, Index, Shard, erlang:monotonic_time(millisecond)).


-doc("""
Like `bump_ae/3` but caller supplies the monotonic millisecond
timestamp so the same "now" can be reused across a batch of shards.
""").
-spec bump_ae(atom(), atom(), non_neg_integer(), integer()) ->
    ok | not_found.

bump_ae(NS, Index, Shard, Now) when is_integer(Now) ->
    case lookup(NS, Index, Shard) of
        {ok, #entry{ae_atomics = Ref}} ->
            atomics:put(Ref, 1, Now),
            ok;
        not_found ->
            not_found
    end.


-doc("""
Bump every shard in `Targets` with a single shared
`erlang:monotonic_time(millisecond)` so the batch observes the same
"now". Returns `{Bumped, NotFound}` counts for telemetry. An empty
list is a strict no-op and returns `{0, 0}`.
""").
-spec bump_ae_targets([shard_key()]) ->
    {non_neg_integer(), non_neg_integer()}.

bump_ae_targets([]) ->
    {0, 0};
bump_ae_targets(Targets) when is_list(Targets) ->
    bump_ae_targets(Targets, erlang:monotonic_time(millisecond)).


-doc("""
Like `bump_ae_targets/1` but caller supplies the monotonic
millisecond timestamp so the same "now" can be reused across multiple
target lists (e.g., when both the applier and an AE round complete in
the same logical tick).
""").
-spec bump_ae_targets([shard_key()], integer()) ->
    {non_neg_integer(), non_neg_integer()}.

bump_ae_targets([], _Now) ->
    {0, 0};
bump_ae_targets(Targets, Now) when is_list(Targets), is_integer(Now) ->
    lists:foldl(
        fun({NS, Index, Shard}, {B, NF}) ->
            case bump_ae(NS, Index, Shard, Now) of
                ok        -> {B + 1, NF};
                not_found -> {B, NF + 1}
            end
        end,
        {0, 0},
        Targets
    ).


-doc("""
Return the monotonic millisecond timestamp of the shard's last AE bump.
Wait-free.

A shard that has never been bumped reads `-(1 bsl 62)` (an
"infinitely stale" sentinel chosen so `Now - sentinel` is a very large
positive number regardless of the node's `monotonic_time` offset).
The sentinel ensures un-bumped shards reliably fail any finite
`max_lag` check until the applier or AE has driven the counter
forward at least once.
""").
-spec last_ae_at(atom(), atom(), non_neg_integer()) ->
    integer() | not_found.

last_ae_at(NS, Index, Shard) ->
    case lookup(NS, Index, Shard) of
        {ok, #entry{ae_atomics = Ref}} ->
            atomics:get(Ref, 1);
        not_found ->
            not_found
    end.


-doc("""
Return all entries registered for the namespace. Used by callers that
need the atomics ref directly to avoid the second `lookup/3`.
""").
-spec shards_for(atom()) -> [shard_entry()].

shards_for(NS) when is_atom(NS) ->
    MS = [{
        #entry{
            key = {NS, '_', '_'},
            _ = '_'
        },
        [],
        ['$_']
    }],
    ets:select(?TABLE, MS).


-doc("""
List of all distinct namespaces registered. Used by callers that want
to apply a freshness check over "every namespace this node knows about"
without spelling them out.
""").
-spec namespaces() -> [atom()].

namespaces() ->
    MS = [{
        #entry{
            key = {'$1', '_', '_'},
            _ = '_'
        },
        [],
        ['$1']
    }],
    lists:usort(ets:select(?TABLE, MS)).


%% =============================================================================
%% Accessors
%% =============================================================================

entry_key(#entry{key = V}) -> V.
entry_cache_adapter(#entry{cache_adapter = V}) -> V.
entry_cache_handle(#entry{cache_handle = V}) -> V.
entry_projection_adapter(#entry{projection_adapter = V}) -> V.
entry_projection_handle(#entry{projection_handle = V}) -> V.
entry_overlay(#entry{overlay = V}) -> V.
entry_fold_module(#entry{fold_module = V}) -> V.
entry_shard_count(#entry{shard_count = V}) -> V.
entry_ae_atomics(#entry{ae_atomics = V}) -> V.
entry_consistency_class(#entry{consistency_class = V}) -> V.


-doc("""
Return the consistency class declared for the namespace. Reads it from
any registered shard of the namespace (the substrate trusts the value
to be consistent across shards — see `register/4`). Returns `ap` for an
unknown namespace, matching the default.
""").
-spec consistency_class(atom()) -> ap | cp.

consistency_class(NS) when is_atom(NS) ->
    MS = [{
        #entry{
            key = {NS, '_', '_'},
            consistency_class = '$1',
            _ = '_'
        },
        [],
        ['$1']
    }],
    case ets:select(?TABLE, MS, 1) of
        {[Class], _} -> Class;
        '$end_of_table' -> ap
    end.


%% =============================================================================
%% gen_server callbacks
%% =============================================================================

init([]) ->
    _ = ets:new(?TABLE, [
        set,
        public,
        named_table,
        {keypos, #entry.key},
        {read_concurrency, true}
    ]),
    Epoch = erlang:make_ref(),
    %% Broadcast asynchronously after init returns so subscribers wake
    %% up *after* the registry is in `ready` state. Synchronous notify
    %% from inside init would still work because the subscribers are
    %% other processes, but doing the work inline keeps init fast.
    self() ! {broadcast_started, Epoch},
    {ok, #state{epoch = Epoch}}.

handle_call({register, NS, Index, Shard, Owner, Config}, _From, State0) ->
    Key = {NS, Index, Shard},
    %% If a previous registration exists for this key, demonitor it
    %% before installing the new owner.
    State1 = drop_monitor_for_key(Key, State0),
    Mon = erlang:monitor(process, Owner),
    Ae = case maps:find(ae_atomics, Config) of
        {ok, ExistingRef} ->
            ExistingRef;
        error ->
            NewRef = atomics:new(1, [{signed, true}]),
            %% Initialise to a "very stale" sentinel so that on a node
            %% where `monotonic_time(millisecond)` is large-negative
            %% (the default offset), `Now - sentinel` is always huge,
            %% i.e. an un-bumped shard fails any finite freshness
            %% check. -(1 bsl 62) leaves plenty of headroom above the
            %% signed-int64 floor for subtraction not to wrap.
            ok = atomics:put(NewRef, 1, -(1 bsl 62)),
            NewRef
    end,
    Entry = #entry{
        key = Key,
        shard_count = maps:get(shard_count, Config),
        cache_adapter = maps:get(cache_adapter, Config),
        cache_handle = maps:get(cache_handle, Config),
        projection_adapter = maps:get(projection_adapter, Config),
        projection_handle = maps:get(projection_handle, Config),
        overlay = maps:get(overlay, Config, undefined),
        fold_module = maps:get(fold_module, Config),
        ae_atomics = Ae,
        consistency_class = maps:get(consistency_class, Config, ap)
    },
    true = ets:insert(?TABLE, Entry),
    State2 = State1#state{
        mon_to_key = maps:put(Mon, Key, State1#state.mon_to_key),
        key_to_mon = maps:put(Key, Mon, State1#state.key_to_mon)
    },
    {reply, ok, State2};

handle_call({unregister, Key}, _From, State0) ->
    State1 = drop_monitor_for_key(Key, State0),
    true = ets:delete(?TABLE, Key),
    {reply, ok, State1};

handle_call(current_epoch, _From, #state{epoch = E} = State) ->
    {reply, E, State};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast(_, State) -> {noreply, State}.

handle_info({broadcast_started, Epoch}, State) ->
    %% `bondy_db_core_events` is started before this module in
    %% `bondy_oplog_sup`, so the notify is safe at init time. If the
    %% events module is down, swallow the error — it is a diagnostic
    %% gap, not a substrate-correctness issue.
    catch bondy_db_core_events:notify(
        bondy_db_core_registry_started,
        Epoch
    ),
    {noreply, State};
handle_info({'DOWN', Mon, process, _Pid, _Reason}, State0) ->
    case maps:take(Mon, State0#state.mon_to_key) of
        {Key, MonToKey1} ->
            true = ets:delete(?TABLE, Key),
            State1 = State0#state{
                mon_to_key = MonToKey1,
                key_to_mon = maps:remove(Key, State0#state.key_to_mon)
            },
            {noreply, State1};
        error ->
            {noreply, State0}
    end;
handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) -> ok.
code_change(_, State, _) -> {ok, State}.


%% =============================================================================
%% Internal
%% =============================================================================

-define(REQUIRED_FIELDS, [
    shard_count, cache_adapter, cache_handle,
    projection_adapter, projection_handle, fold_module
]).

validate_config(Config) ->
    case [K || K <- ?REQUIRED_FIELDS, not maps:is_key(K, Config)] of
        []      -> validate_consistency_class(Config);
        [K | _] -> {error, {missing_required_field, K}}
    end.


validate_consistency_class(Config) ->
    case maps:find(consistency_class, Config) of
        {ok, V} when V =:= ap; V =:= cp -> ok;
        {ok, Bad} -> {error, {invalid_consistency_class, Bad}};
        error -> ok
    end.


drop_monitor_for_key(Key, State) ->
    case maps:take(Key, State#state.key_to_mon) of
        {OldMon, KeyToMon1} ->
            true = erlang:demonitor(OldMon, [flush]),
            State#state{
                mon_to_key = maps:remove(OldMon, State#state.mon_to_key),
                key_to_mon = KeyToMon1
            };
        error ->
            State
    end.

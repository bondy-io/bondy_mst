%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_metrics).

-behaviour(gen_server).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Counter / gauge primitive backed by the BIF `counters` and `atomics`
modules.

Inspired by [`shortishly/metrics`](https://github.com/shortishly/metrics);
implements the small subset we need with the same single-map API. The
hot path (increment, set, read) is wait-free: a single ETS read to
locate the counter reference, then a `counters:add/3` or
`counters:put/3` call against the BIF-backed array. There is no ETS
write contention on counter mutation — the reference is allocated once
on first-touch and never moves.

## Storage

| Layer | Concern |
|---|---|
| ETS `bondy_metrics_tab` | `{ {Name, Label} => #{type, ref} }` — first-touch registry |
| `counters:new(1, [write_concurrency])` | per-metric atomic value |

Allocating a counter races safely: the loser of an `insert_new` race
re-reads and uses the winner's reference, dropping the unused
`counters` array on the floor (it is small and unrooted; the GC reaps
it).

## Types

- **counter** — monotonically increasing; `counter/1` accepts an
  optional `delta` (default `1`) and adds it.
- **gauge** — arbitrary up/down value; `gauge/1` accepts a `value`
  and writes it directly via `counters:put/3`. There is no delta form
  because gauges are stateful observations, not increments.

Type is fixed at first-touch: re-using a name across types returns
`{error, {wrong_type, _}}`.

## Labels

Optional `label` map for the second dimension of the metric key. Empty
label (default) is used when the metric is not partitioned. Two metrics
with the same `name` but different labels are independent counters and
the storage cost scales with the cross-product of distinct labels.

## Read APIs

- `value/1` — one (Name, Label) pair.
- `with_name/1` — every metric matching a name (across all labels);
  cheap because the registry walks one match-spec.
- `all/0` — every metric on the node; intended for exposition.

## Application lifecycle

A gen_server owns the registry table. The hot-path primitives are
public-table operations so they bypass the gen_server entirely. The
gen_server handles `delete/1` (which both removes the row and drops
the reference) and any future management API.

A restart wipes the table; on first-touch every counter is re-allocated
from zero. Counters are gauge-style observations of running totals, not
durable accounting — if a consumer needs survival across restart they
need a separate persistence layer.
""").

-define(SERVER, ?MODULE).
-define(TAB, bondy_metrics_tab).
-define(POS, 1).

-record(state, {}).

-type name() :: atom().
-type label() :: map().
-type type() :: counter | gauge.
-type spec() :: #{
    name := name(),
    label => label(),
    delta => integer(),
    value => integer()
}.
-type entry() :: #{type := type(), ref := counters:counters_ref()}.

-export_type([name/0, label/0, type/0, spec/0]).

-export([child_spec/0]).
-export([start_link/0]).

%% Counter / gauge mutation
-export([counter/1]).
-export([gauge/1]).

%% Reads
-export([value/1]).
-export([with_name/1]).
-export([all/0]).
-export([info/1]).

%% Management
-export([delete/1]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

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

-spec start_link() -> {ok, pid()} | {error, term()}.

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

?DOC("""
Add to a counter. Default `delta` is `1`. Allocates on first touch.

Returns `ok` on success or `{error, {wrong_type, _}}` if the name is
already in use by a metric of a different type.
""").
-spec counter(spec()) -> ok | {error, term()}.

counter(#{name := Name} = M) ->
    Label = maps:get(label, M, #{}),
    Delta = maps:get(delta, M, 1),
    operate(
        {Name, Label},
        counter,
        fun(Ref) -> counters:add(Ref, ?POS, Delta) end
    ).

?DOC("""
Write an absolute value to a gauge. Allocates on first touch.

Returns `ok` on success or `{error, {wrong_type, _}}` if the name is
already in use by a metric of a different type.
""").
-spec gauge(spec()) -> ok | {error, term()}.

gauge(#{name := Name, value := V} = M) when is_integer(V) ->
    Label = maps:get(label, M, #{}),
    operate(
        {Name, Label},
        gauge,
        fun(Ref) -> counters:put(Ref, ?POS, V) end
    ).

?DOC("""
Read the current value of one (Name, Label) pair. Returns `undefined`
when the metric does not exist.
""").
-spec value(#{name := name(), label => label()}) ->
    integer() | undefined.

value(#{name := Name} = M) ->
    Label = maps:get(label, M, #{}),
    case lookup_entry({Name, Label}) of
        {ok, #{ref := Ref}} -> counters:get(Ref, ?POS);
        not_found -> undefined
    end.

?DOC("""
Return `[{Label, Value}]` for every metric registered under `Name`.
""").
-spec with_name(name()) -> [{label(), integer()}].

with_name(Name) when is_atom(Name) ->
    MS = [{{{Name, '$1'}, '$2'}, [], [{{'$1', '$2'}}]}],
    [{L, read(Entry)} || {L, Entry} <- ets:select(?TAB, MS)].

?DOC("""
Return every metric on the node, intended for exposition. Each row is
`#{name, label, type, value}`.
""").
-spec all() ->
    [
        #{
            name := name(),
            label := label(),
            type := type(),
            value := integer()
        }
    ].

all() ->
    [
        #{name => N, label => L, type => T, value => counters:get(R, ?POS)}
     || {{N, L}, #{type := T, ref := R}} <- ets:tab2list(?TAB)
    ].

?DOC("""
Metadata for one metric without reading the value. Useful when callers
want to inspect type without paying for the counters read.
""").
-spec info(#{name := name(), label => label()}) ->
    {ok, entry()} | not_found.

info(#{name := Name} = M) ->
    lookup_entry({Name, maps:get(label, M, #{})}).

?DOC("""
Drop a metric. The row is removed from the registry and the underlying
counters reference is GC'd. Subsequent `value/1` on the same name
returns `undefined` until the next write re-allocates.
""").
-spec delete(#{name := name(), label => label()}) -> ok.

delete(#{name := Name} = M) ->
    Label = maps:get(label, M, #{}),
    gen_server:call(?SERVER, {delete, {Name, Label}}).

%% =============================================================================
%% gen_server callbacks
%% =============================================================================

init([]) ->
    _ = ets:new(?TAB, [
        set,
        public,
        named_table,
        {keypos, 1},
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    {ok, #state{}}.

handle_call({delete, Key}, _From, State) ->
    true = ets:delete(?TAB, Key),
    {reply, ok, State};
handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% Look up the entry; allocate on first-touch with insert_new. On race
%% (insert_new returns false) we re-lookup so the loser uses the winner's
%% reference and the abandoned counters array is GC'd.
operate(Key, Type, Op) ->
    case lookup_entry(Key) of
        {ok, #{type := Type, ref := Ref}} ->
            Op(Ref);
        {ok, #{type := Other}} ->
            {error, {wrong_type, Other}};
        not_found ->
            Ref0 = counters:new(1, [write_concurrency]),
            Entry = #{type => Type, ref => Ref0},
            case ets:insert_new(?TAB, {Key, Entry}) of
                true ->
                    Op(Ref0);
                false ->
                    %% lost the race; retry with the winner's ref
                    operate(Key, Type, Op)
            end
    end.

lookup_entry(Key) ->
    case ets:lookup(?TAB, Key) of
        [{_, Entry}] -> {ok, Entry};
        [] -> not_found
    end.

read(#{ref := Ref}) -> counters:get(Ref, ?POS).

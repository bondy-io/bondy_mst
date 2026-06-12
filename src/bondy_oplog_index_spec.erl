%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_index_spec).

-include("bondy_doc.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Declarative secondary-index spec: validation, term extraction, and
column projection.

A spec is **pure declarative data** — no funs — so every node computes
the same index entries from the same primary value. It addresses the
indexed column(s) by a path into the (decoded) projection value, and
optionally denormalises further columns alongside the index pointer.

## Spec shape

```erlang
#{
    name      := atom(),                 %% index name, e.g. by_status
    extract   := path(),                 %% path to the indexed column
    normalize => none | downcase,        %% term normaliser (default none)
    projects  => [path()],               %% denormalised columns (default [])
    max_lag   => non_neg_integer()       %% read-side freshness bound, ms
               | infinity                %%   (default infinity)
}
```

`path()` is a list of map keys walked into the value (`[]` = the whole
value; `[status]` = top-level `status`; `[user, status]` = nested). A
bare key is not accepted — always a list — to keep "single field" and
"nested path" unambiguous.

## `terms/2` (value -> `[Term]`)

Navigates `extract` into `Value` and normalises the leaf:

- A **missing** path step or an `undefined` leaf yields `[]` (no index
  entry for this value).
- A **list/ordset** leaf yields one term per element (a multi-valued
  index — e.g. indexing a `g_set` value or a tags field).
- Any other **scalar** leaf yields a singleton `[Term]`.

`normalize => downcase` lowercases binary terms (via
`string:lowercase/1`) and passes non-binaries through unchanged.

## `project/2` (value -> columns binary)

For an empty `projects` (pointer-only index) returns `<<>>`. Otherwise
builds a map `#{Path => Value}` over the present projected paths and
encodes it with `term_to_binary/2` `[deterministic]`, so the bytes are
canonical and identical across replicas — required because the index
entry fold declares `value_equals_state/0 -> true` (its state bytes are
the value bytes, which must converge byte-for-byte). `decode_projection/1`
inverts it.
""").

-export([validate/1]).
-export([name/1]).
-export([max_lag/1]).
-export([max_inflight/1]).
-export([coalesce_ms/1]).
-export([projects/1]).
-export([terms/2]).
-export([normalize_term/2]).
-export([project/2]).
-export([decode_projection/1]).

-type path() :: [atom() | binary() | integer()].
-type normalizer() :: none | downcase.
-type spec() :: #{
    name := atom(),
    extract := path(),
    normalize => normalizer(),
    projects => [path()],
    max_lag => non_neg_integer() | infinity,
    max_inflight => pos_integer(),
    coalesce_ms => non_neg_integer()
}.

-export_type([spec/0, path/0, normalizer/0]).

-define(MISSING, '$missing').
%% Default per-secondary-shard in-flight back-pressure cap (IDX-4). Kept
%% in sync with `bondy_oplog_applier`'s fallback. Large by design: the cap
%% is a safety valve for a pathologically hot shard, not a steady-state
%% throttle, and MUST exceed a shard's live-entry working set.
-define(DEFAULT_MAX_INFLIGHT, 100000).

%% =============================================================================
%% API
%% =============================================================================

-doc """
Validate a spec. Returns `ok` or `{error, {Reason, Detail}}` with a
nested-tuple reason: `{missing_key, name | extract}`,
`{invalid_name, _}`, `{invalid_extract, _}`, `{invalid_normalize, _}`,
`{invalid_projects, _}`, or `{not_a_spec, _}`.
""".
-spec validate(term()) -> ok | {error, {atom(), term()}}.

validate(Spec) when is_map(Spec) ->
    Steps = [
        fun check_name/1,
        fun check_extract/1,
        fun check_normalize/1,
        fun check_projects/1,
        fun check_max_inflight/1,
        fun check_coalesce_ms/1
    ],
    run_checks(Steps, Spec);
validate(Other) ->
    {error, {not_a_spec, Other}}.

-doc "The index name.".
-spec name(spec()) -> atom().

name(#{name := Name}) -> Name.

-doc "The read-side freshness bound in ms, or `infinity`.".
-spec max_lag(spec()) -> non_neg_integer() | infinity.

max_lag(Spec) -> maps:get(max_lag, Spec, infinity).

-doc """
The per-secondary-shard in-flight back-pressure cap (IDX-4): the maximum
number of dispatched-but-not-yet-flushed index ops the writer's backlog
may hold before a batch is dropped and the shard scheduled for rebuild.
Defaults to a large value (`100000`).
""".
-spec max_inflight(spec()) -> pos_integer().

max_inflight(Spec) -> maps:get(max_inflight, Spec, ?DEFAULT_MAX_INFLIGHT).

-doc """
The secondary writer's flush-coalescing window in ms (IDX-3). `undefined`
defers to the writer's own default. Exposed on the spec mainly so tests
can disable auto-flush (a large value) to drive the back-pressure path
deterministically.
""".
-spec coalesce_ms(spec()) -> non_neg_integer() | undefined.

coalesce_ms(Spec) -> maps:get(coalesce_ms, Spec, undefined).

-doc "The denormalised column paths (`[]` for a pointer-only index).".
-spec projects(spec()) -> [path()].

projects(Spec) -> maps:get(projects, Spec, []).

-doc """
Extract the (possibly empty, possibly multi-valued) list of normalised
index terms for a value.
""".
-spec terms(spec(), term()) -> [bondy_oplog_index_key:term_value()].

terms(#{extract := Path} = Spec, Value) ->
    Norm = maps:get(normalize, Spec, none),
    case navigate(Path, Value) of
        ?MISSING ->
            [];
        undefined ->
            [];
        Leaf when is_list(Leaf) ->
            [normalize(Norm, E) || E <- Leaf, E =/= undefined];
        Leaf ->
            [normalize(Norm, Leaf)]
    end.

-doc """
Apply the spec's normaliser to a single query term, so a lookup term is
encoded the same way the stored terms were by `terms/2`. Use this on the
caller-supplied term in `index_get`/`index_range` before encoding bounds.
""".
-spec normalize_term(spec(), bondy_oplog_index_key:term_value()) ->
    bondy_oplog_index_key:term_value().

normalize_term(Spec, Term) ->
    normalize(maps:get(normalize, Spec, none), Term).

-doc """
Build the denormalised columns binary for a value. `<<>>` for a
pointer-only index.

Projected column values must be `[safe]`-decodable on every node that
reads the index — binaries, numbers, lists, maps, and atoms already
present on the reading node. `decode_projection/1` uses
`binary_to_term(_, [safe])`, so projecting a column whose value is an
atom not loaded on a fresh replica would fail that decode. In practice
projected columns are binaries/numbers; the constraint only bites
exotic atom-valued projections.
""".
-spec project(spec(), term()) -> binary().

project(Spec, Value) ->
    case projects(Spec) of
        [] ->
            <<>>;
        Paths ->
            Cols = lists:foldl(
                fun(P, Acc) ->
                    case navigate(P, Value) of
                        ?MISSING -> Acc;
                        undefined -> Acc;
                        V -> Acc#{P => V}
                    end
                end,
                #{},
                Paths
            ),
            term_to_binary(Cols, [deterministic])
    end.

-doc "Decode a columns binary produced by `project/2`.".
-spec decode_projection(binary()) -> map().

decode_projection(<<>>) -> #{};
decode_projection(Bin) when is_binary(Bin) -> binary_to_term(Bin, [safe]).

%% =============================================================================
%% INTERNAL — validation
%% =============================================================================

run_checks([], _Spec) ->
    ok;
run_checks([Check | Rest], Spec) ->
    case Check(Spec) of
        ok -> run_checks(Rest, Spec);
        {error, _} = Err -> Err
    end.

check_name(#{name := Name}) when is_atom(Name) -> ok;
check_name(#{name := Name}) -> {error, {invalid_name, Name}};
check_name(_) -> {error, {missing_key, name}}.

check_extract(#{extract := Path}) ->
    case is_path(Path) of
        true -> ok;
        false -> {error, {invalid_extract, Path}}
    end;
check_extract(_) ->
    {error, {missing_key, extract}}.

check_normalize(Spec) ->
    case maps:get(normalize, Spec, none) of
        none -> ok;
        downcase -> ok;
        Other -> {error, {invalid_normalize, Other}}
    end.

check_projects(Spec) ->
    Paths = maps:get(projects, Spec, []),
    case is_list(Paths) andalso lists:all(fun is_path/1, Paths) of
        true -> ok;
        false -> {error, {invalid_projects, Paths}}
    end.

check_max_inflight(Spec) ->
    case maps:get(max_inflight, Spec, ?DEFAULT_MAX_INFLIGHT) of
        N when is_integer(N), N > 0 -> ok;
        Bad -> {error, {invalid_max_inflight, Bad}}
    end.

check_coalesce_ms(Spec) ->
    case maps:get(coalesce_ms, Spec, undefined) of
        undefined -> ok;
        N when is_integer(N), N >= 0 -> ok;
        Bad -> {error, {invalid_coalesce_ms, Bad}}
    end.

is_path(Path) when is_list(Path) ->
    lists:all(
        fun(Step) ->
            is_atom(Step) orelse is_binary(Step) orelse is_integer(Step)
        end,
        Path
    );
is_path(_) ->
    false.

%% =============================================================================
%% INTERNAL — navigation / normalisation
%% =============================================================================

navigate([], Value) ->
    Value;
navigate([Key | Rest], Map) when is_map(Map) ->
    case maps:find(Key, Map) of
        {ok, V} -> navigate(Rest, V);
        error -> ?MISSING
    end;
navigate(_Path, _NonMap) ->
    ?MISSING.

normalize(none, V) ->
    V;
normalize(downcase, V) when is_binary(V) ->
    string:lowercase(V);
normalize(downcase, V) ->
    V.

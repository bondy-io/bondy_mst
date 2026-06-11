%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_index_key).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Order-preserving composite `(Term, PrimaryKey)` codec for the secondary
index keyspace.

A secondary index is materialised as an **ordered composite keyspace**
queried by range, rather than via leveled's native 2i (which is
incompatible with `head_only` mode). Each index entry is keyed by

```
SecKey = <<TermEnc/binary, 0, PrimaryKey/binary>>
```

so the entries sort first by the normalised, order-preserving term
encoding and then by the raw primary key. Equality reads scan the
contiguous run for one term; range reads scan a `[Lo, Hi)` window.

## Term encoding (`encode_term/1`)

- **Binary terms** are encoded directly (byte order is the comparison
  order callers want).
- **Integer terms** are first mapped to a fixed-width, sign-biased
  big-endian form `<<(N + (1 bsl 63)):64>>` so two's-complement signed
  order becomes unsigned lexicographic order. v1 restricts integer
  terms to the signed 64-bit range; anything outside raises `badarg`.

The byte string is then run through an **order-preserving,
prefix-free, self-delimiting escape** so the encoded term contains no
`0x00` byte:

```
0x00 -> 0x01 0x01
0x01 -> 0x01 0x02
b    -> b            (b >= 0x02)
```

The single `0x00` separator therefore sorts strictly before any encoded
term byte (all `>= 0x01`), which guarantees the primary-key suffix
never corrupts term order even when one term is a byte-prefix of
another. Recovering the primary key is a scan to the first (and only)
`0x00`.

### Deviation from the design's literal byte values

`MST_DB_DESIGN.md §13` sketches the escape as `0x00 -> 0x00 0x01` with a
*bare* `0x00` separator. That scheme is not order-preserving for an
arbitrary appended primary key: when term `T1` is a prefix of term
`T2`, the byte following `T1`'s separator is the primary key's first
byte, which can compare greater than the `0x01` that opens `T2`'s
escaped continuation — inverting the intended `T1 < T2` order. We use a
prefix-free *monotone* code (escape into the `0x01` range, reserve
`0x00` solely as the separator) which is provably order-preserving for
any primary-key suffix. Same goal as the design (order-preserving,
self-delimiting, recover the key by scanning to the separator), correct
construction. See the project convention on preferring empirically
correct implementations over paper-design contracts.

## Bounds

- `equality_bounds(T)` -> `{<<TermEnc(T), 0>>, <<TermEnc(T), 1>>}`, the
  half-open `[Low, High)` window covering exactly term `T`'s entries.
- `range_bounds(Lo, Hi)` -> `{<<TermEnc(Lo), 0>>, <<TermEnc(Hi), 0>>}`,
  the half-open window covering terms in `[Lo, Hi)`.

Both are half-open `[Low, High)` so callers map them onto
`bondy_db_core:range/range_all` (exclusive upper bound) without an
off-by-one.
""").

-export([encode/2]).
-export([encode_term/1]).
-export([decode_pk/1]).
-export([equality_bounds/1]).
-export([range_bounds/2]).
-export([bucket/2]).
-export([shard/3]).

-type term_value() :: binary() | integer().
%% A single index term, before order-preserving encoding. v1 restricts a
%% column to one type (binary OR integer); mixing types in one index
%% would need a leading type tag, deferred.

-export_type([term_value/0]).

-define(SEP, 0).
-define(INT_BIAS, (1 bsl 63)).
-define(INT_MIN, -(1 bsl 63)).
-define(INT_MAX, ((1 bsl 63) - 1)).
-define(IDX_INFIX, "/$idx/").

%% =============================================================================
%% API
%% =============================================================================

-doc """
Encode a `(Term, PrimaryKey)` pair into the composite secondary key.
`PrimaryKey` is appended raw after the `0x00` separator.
""".
-spec encode(term_value(), binary()) -> binary().

encode(Term, PrimaryKey) when is_binary(PrimaryKey) ->
    <<(encode_term(Term))/binary, ?SEP, PrimaryKey/binary>>.

-doc """
Encode a single term into its order-preserving, `0x00`-free byte form.
Exposed for the bounds helpers and for callers building keys directly.
""".
-spec encode_term(term_value()) -> binary().

encode_term(Term) when is_binary(Term) ->
    escape(Term);
encode_term(Term) when is_integer(Term), Term >= ?INT_MIN, Term =< ?INT_MAX ->
    escape(<<(Term + ?INT_BIAS):64/big-unsigned>>);
encode_term(Term) when is_integer(Term) ->
    %% Outside the signed 64-bit range supported in v1.
    erlang:error(badarg, [Term]).

-doc """
Recover the primary key from a composite secondary key by scanning to
the single `0x00` separator. The encoded term contains no `0x00` byte,
so the first match is unambiguously the separator.
""".
-spec decode_pk(binary()) -> binary().

decode_pk(SecKey) when is_binary(SecKey) ->
    case binary:match(SecKey, <<?SEP>>) of
        {Pos, 1} ->
            binary:part(SecKey, Pos + 1, byte_size(SecKey) - Pos - 1);
        nomatch ->
            erlang:error(badarg, [SecKey])
    end.

-doc """
Half-open `[Low, High)` bounds covering exactly the entries for term
`T`. `Low` is `T`'s smallest possible key (empty primary key); `High`
is the first key strictly greater than every `T` entry yet less than any
other term's entries.
""".
-spec equality_bounds(term_value()) -> {binary(), binary()}.

equality_bounds(T) ->
    Enc = encode_term(T),
    {<<Enc/binary, 0>>, <<Enc/binary, 1>>}.

-doc """
Half-open `[Low, High)` bounds covering terms in `[Lo, Hi)`. `Lo` is
included (its smallest key), `Hi` is excluded (its smallest key is the
exclusive upper bound).
""".
-spec range_bounds(term_value(), term_value()) -> {binary(), binary()}.

range_bounds(Lo, Hi) ->
    {<<(encode_term(Lo))/binary, 0>>, <<(encode_term(Hi))/binary, 0>>}.

-doc """
The storage-layer bucket for an index's cells:
`<<PrimaryBucket, "/$idx/", IndexName>>`. Each `(NS, IndexName, SecShard)`
already has its own shard-set, so this only needs to isolate realms (the
`PrimaryBucket` prefix); the `IndexName` suffix keeps the bucket
self-describing and matches `MST_DB_DESIGN.md §13`. Reader and writer
MUST agree on this layout — it is the single source of truth.
""".
-spec bucket(binary(), atom()) -> binary().

bucket(PrimaryBucket, IndexName) when
    is_binary(PrimaryBucket), is_atom(IndexName)
->
    <<PrimaryBucket/binary, ?IDX_INFIX,
        (atom_to_binary(IndexName, utf8))/binary>>.

-doc """
The secondary shard a term lands in: `phash2({Bucket, Term}, ShardCount)`.
Term-sharded, so all `(Term, _)` entries for one term live in one shard —
an equality read hits exactly that shard; a range read scatters. `Term`
is the *normalised* term value (the reader and writer normalise
identically via the index spec). The single source of truth for placement,
shared by `bondy_db` reads and the secondary writer.
""".
-spec shard(binary(), term(), pos_integer()) -> non_neg_integer().

shard(Bucket, Term, ShardCount) when
    is_binary(Bucket), is_integer(ShardCount), ShardCount > 0
->
    erlang:phash2({Bucket, Term}, ShardCount).

%% =============================================================================
%% INTERNAL
%% =============================================================================

%% Order-preserving, prefix-free escape into the `0x01` range so the
%% result contains no `0x00`. `0x00 -> 0x01 0x01`, `0x01 -> 0x01 0x02`,
%% every other byte unchanged. The code is monotone and prefix-free, so
%% lexicographic order of encoded strings matches that of the originals.
escape(Bin) ->
    %% Fast path: the overwhelmingly common term contains no `0x00`/`0x01`,
    %% so a single C-level scan lets us return the input binary unchanged
    %% instead of rebuilding it byte-by-byte. `encode_term/1` is on the hot
    %% indexed-write path and is called again per bound on every read.
    case binary:match(Bin, [<<0>>, <<1>>]) of
        nomatch -> Bin;
        _ -> <<<<(esc_byte(B))/binary>> || <<B>> <= Bin>>
    end.

esc_byte(0) -> <<1, 1>>;
esc_byte(1) -> <<1, 2>>;
esc_byte(B) -> <<B>>.

%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_path_sharded).
-behaviour(bondy_oplog_path_strategy).

-include("bondy_doc.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Sharded layout: `<BaseDir>/<hash:2>/<hash:4>/<InstanceId>/`.

The shard prefixes are the first 2 and first 4 hex characters of
`sha256(InstanceId)`. This keeps the per-directory child count
manageable for installations with millions of instances.
""").

-export([storage_path/2]).
-export([discover/1]).

storage_path(InstanceId, BaseDir) when
    is_binary(InstanceId), is_binary(BaseDir)
->
    Hash = hex(crypto:hash(sha256, InstanceId)),
    Shard1 = binary:part(Hash, 0, 2),
    Shard2 = binary:part(Hash, 0, 4),
    filename:join([BaseDir, Shard1, Shard2, InstanceId]).

discover(BaseDir) when is_binary(BaseDir) ->
    Pattern = unicode:characters_to_list(
        filename:join([BaseDir, "*", "*", "*"])
    ),
    [
        unicode:characters_to_binary(filename:basename(P))
     || P <- filelib:wildcard(Pattern),
        filelib:is_dir(P)
    ].

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
hex(Bin) ->
    <<<<(nibble(N))>> || <<N:4>> <= Bin>>.

%% @private
nibble(N) when N < 10 -> $0 + N;
nibble(N) -> $a + N - 10.

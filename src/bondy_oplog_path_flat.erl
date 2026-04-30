%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_path_flat).
-behaviour(bondy_oplog_path_strategy).

-include("bondy_mst.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Flat layout: `<BaseDir>/<InstanceId>/`. Suitable for small fixed
instance sets.
""").

-export([storage_path/2]).
-export([discover/1]).

storage_path(InstanceId, BaseDir) when
    is_binary(InstanceId), is_binary(BaseDir)
->
    filename:join([BaseDir, InstanceId]).

discover(BaseDir) when is_binary(BaseDir) ->
    Pattern = unicode:characters_to_list(filename:join(BaseDir, "*")),
    [
        unicode:characters_to_binary(filename:basename(P))
     || P <- filelib:wildcard(Pattern),
        filelib:is_dir(P)
    ].

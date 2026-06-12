%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_app).

-behaviour(application).

-include("bondy_doc.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Application entry point for the `bondy_oplog` write/replication layer and the
`bondy_db` consumer facade it powers.

It boots the oplog supervision tree (`bondy_oplog_sup`) and performs the two
pieces of process-independent boot wiring the layer needs:

- `bondy_oplog_leveled_tag:install/0` — register the projection fold-tag
  extractor/head-builder with leveled (see `?BONDY_FOLD_TAG`). This is an
  oplog/leveled-layer concern.
- `bondy_mst_config:init/0` — initialise the configuration of the `bondy_mst`
  replication-structure library this layer is built on. In the two-application
  layout this also runs via `bondy_mst`'s own application start before
  `bondy_oplog` starts; it is idempotent, so calling it here keeps the
  single-application build correct.
""").

-export([start/2, stop/1]).

%% =============================================================================
%% APPLICATION CALLBACKS
%% =============================================================================

start(_StartType, _StartArgs) ->
    ok = bondy_oplog_leveled_tag:install(),
    ok = bondy_mst_config:init(),
    bondy_oplog_sup:start_link().

stop(_State) ->
    ok.

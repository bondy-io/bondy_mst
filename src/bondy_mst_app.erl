%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_mst_app).

-behaviour(application).

-export([start/2, stop/1]).

%% =============================================================================
%% API
%% =============================================================================

start(_StartType, _StartArgs) ->
    ok = register_leveled_tag_hooks(),
    bondy_mst_sup:start_link().

stop(_State) ->
    ok.


%% =============================================================================
%% INTERNAL
%% =============================================================================

%% Wire the `?BONDY_FOLD_TAG` extractor and head-builder into leveled's
%% per-tag override mechanism (`_design/catalogue_expansion_plan.md`
%% §3.4). Leveled reads these via `application:get_env(leveled, ...)`
%% inside `leveled_head:get_appdefined_function/3`; it only consults
%% them for non-builtin tags, so this registration is a no-op for any
%% bucket that uses `?STD_TAG`.
register_leveled_tag_hooks() ->
    bondy_oplog_leveled_tag:install().

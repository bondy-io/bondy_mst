%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_path_strategy).

-include("bondy_doc.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Behaviour for computing per-instance storage paths
(`_design/10_new_design.md` §6.5).

Two implementations ship with the library:

- `bondy_oplog_path_flat` — `<BaseDir>/<InstanceId>/`. Simple;
  suitable for small numbers of instances.
- `bondy_oplog_path_sharded` — `<BaseDir>/<hash:2>/<hash:4>/<InstanceId>/`.
  Recommended when an installation will host many instances; modern
  filesystems handle millions of small directories efficiently when the
  prefix is sharded.

Consumers may provide their own strategy when neither built-in fits.
""").

-callback storage_path(
    InstanceId :: binary(),
    BaseDir :: binary()
) -> file:filename_all().

-callback discover(BaseDir :: binary()) -> [InstanceId :: binary()].

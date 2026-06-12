%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_db_leveled_sup).
-behaviour(supervisor).

-include("bondy_doc.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
`simple_one_for_one` supervisor for leveled Bookies under a `bondy_db`
topology (`MST_DB_DESIGN.md` §18 — PR9).

The supervisor itself is a regular OTP supervisor; topology modules
(`bondy_db_topology_single_bookie`, `bondy_db_topology_per_entity`)
call `start_bookie/2` to provision Bookies and own their lifecycle
inside their own state. The supervisor is started lazily by the
topology's `init/2` rather than wired under `bondy_mst_sup` — the
lifetime of leveled Bookies is bounded by the lifetime of the
topology that owns them.

## Lifecycle

`start_link/0` spawns an unnamed supervisor. The caller (the topology
module's `init/2`) gets the supervisor pid back and stashes it inside
its own state. Topology `shutdown/1` calls `stop/1` here, which exits
the supervisor and brings every child Bookie down with it.

Each Bookie is `temporary` — supervisor restart-after-crash would
deliver a fresh Bookie pid that the topology's existing routing map
does not know about. The topology is responsible for any restart
policy it wants (re-call `start_bookie/2`, refresh its routing map).
""").

-export([start_link/0]).
-export([stop/1]).
-export([start_bookie/2]).

-export([init/1]).

%% =============================================================================
%% API
%% =============================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.

start_link() ->
    supervisor:start_link(?MODULE, []).

-doc """
Stop the supervisor and every Bookie it owns. Returns `ok` once every
child has terminated.

Children are terminated via `supervisor:terminate_child/2` so leveled's
`terminate/2` runs and flushes the inker. After every child is gone,
the supervisor itself is unlinked and killed — supervisors do not
expose a clean self-stop API and `exit(Sup, shutdown)` is not honoured
by an arbitrary caller.
""".
-spec stop(Sup :: pid()) -> ok.

stop(Sup) when is_pid(Sup) ->
    %% Terminate children first so leveled flushes cleanly.
    Children = [
        Pid
     || {_Id, Pid, _Type, _Mods} <-
            supervisor:which_children(Sup),
        is_pid(Pid)
    ],
    lists:foreach(
        fun(Pid) ->
            _ = catch supervisor:terminate_child(Sup, Pid)
        end,
        Children
    ),
    %% Now bring the supervisor itself down. We may be the linking
    %% parent (start_link/0) or just a holder of the pid — `kill`
    %% works either way and we have already flushed the children.
    Ref = erlang:monitor(process, Sup),
    _ = catch unlink(Sup),
    exit(Sup, kill),
    receive
        {'DOWN', Ref, process, Sup, _} -> ok
    after 5_000 ->
        true = erlang:demonitor(Ref, [flush]),
        ok
    end.

-doc """
Provision a leveled Bookie under the supervisor at `Dir` with `Opts`.

`Opts` is the proplist passed straight to `leveled_bookie:book_start/1`.
At minimum it must include the keys leveled requires (typically
`root_path`); see leveled's documentation for the full list. This
module does not validate `Opts` — that is leveled's job.

Returns the Bookie pid on success.
""".
-spec start_bookie(
    Sup :: pid(),
    Opts :: proplists:proplist()
) -> {ok, pid()} | {error, term()}.

start_bookie(Sup, Opts) when is_pid(Sup), is_list(Opts) ->
    supervisor:start_child(Sup, [Opts]).

%% =============================================================================
%% SUPERVISOR CALLBACKS
%% =============================================================================

init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 0,
        period => 1
    },
    %% `temporary` because a restart would hand the topology a fresh
    %% Bookie pid it has no way to learn about; the topology owns
    %% restart policy. `shutdown => 30_000` matches the `stop/1`
    %% deadline so leveled has time to flush.
    ChildSpec = #{
        id => leveled_bookie,
        start => {leveled_bookie, book_start, []},
        restart => temporary,
        shutdown => 30_000,
        type => worker,
        modules => [leveled_bookie]
    },
    {ok, {SupFlags, [ChildSpec]}}.

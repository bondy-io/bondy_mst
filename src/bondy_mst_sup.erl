%%%-------------------------------------------------------------------
%% @doc bondy_mst top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(bondy_mst_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).




%% =============================================================================
%% API
%% =============================================================================



start_link() ->
    case supervisor:start_link({local, ?SERVER}, ?MODULE, []) of
        {ok, _} = OK ->
            Children = supervisor:which_children(?MODULE),
            case lists:keyfind(leveled, 1, Children) of
                {leveled, Pid, worker, _} ->
                    _ = persistent_term:put({bondy_mst, leveled}, Pid),
                    OK;
                false ->
                    OK
            end;
        Error ->
            Error
    end.




%% =============================================================================
%% SUPERVISOR CALLBACKS
%% =============================================================================




init([]) ->
    ok = bondy_mst_config:init(),
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    ChildSpecs = maybe_append_leveled(maybe_append_rocksdb_manager([])),

    {ok, {SupFlags, ChildSpecs}}.


maybe_append_leveled(Acc) ->
    case bondy_mst_config:get([store, leveled], []) of
            [] ->
                Acc;
            Opts ->
                [
                    #{
                        id => leveled,
                        start => {leveled_bookie, book_start, [Opts]},
                        restart => permanent,
                        shutdown => 5_000,
                        type => worker,
                        modules => [leveled_bookie]
                    } | Acc
                ]
    end.

maybe_append_rocksdb_manager(Acc) ->
    case bondy_mst_config:get([store, rocksdb], []) of
        [] ->
            Acc;
        Opts ->
            [
                #{
                    id => bondy_mst_rocksdb_manager,
                    start => {
                        bondy_mst_rocksdb_manager,
                        start_link,
                        [Opts]
                    },
                    restart => permanent,
                    shutdown => 5000,
                    type => worker,
                    modules => [bondy_mst_rocksdb_manager]
                } | Acc
            ]
    end.

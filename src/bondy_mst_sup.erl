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
    ChildSpecs =
        case bondy_mst_config:get([store, leveled], []) of
            [] ->
                [];
            Opts ->
                [
                    #{
                        id => leveled,
                        start => {leveled_bookie, book_start, [Opts]},
                        restart => permanent,
                        shutdown => 5_000,
                        type => worker,
                        modules => [leveled_bookie]
                    }
                ]
        end,
    {ok, {SupFlags, ChildSpecs}}.


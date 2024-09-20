%%%-------------------------------------------------------------------
%% @doc bondy_mst public API
%% @end
%%%-------------------------------------------------------------------

-module(bondy_mst_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    bondy_mst_sup:start_link().

stop(_State) ->
    ok.

%% internal functions

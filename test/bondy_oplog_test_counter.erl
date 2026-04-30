%% A trivial G-Counter-style CRDT used by Stage 5 tests.
%%
%% Events: `{inc, N}` increments by N. State: integer total.
%% interpret_cog/2 sums over the batch. query/2 returns the count.

-module(bondy_oplog_test_counter).

-behaviour(bondy_oplog_crdt).

-export([causal_tier/0]).
-export([init/0]).
-export([interpret_cog/2]).
-export([query/2]).

causal_tier() ->
    tier_0.

init() ->
    0.

interpret_cog(Events, State) ->
    lists:foldl(
        fun(E, Acc) ->
            case bondy_oplog_event:op(E) of
                {inc, N} when is_integer(N) -> Acc + N;
                _ -> Acc
            end
        end,
        State,
        Events
    ).

query(value, State) ->
    State.

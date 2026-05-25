%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_mst_jepsen_http_set).

-behaviour(cowboy_handler).

-include_lib("kernel/include/logger.hrl").

%% HTTP shim for the OR-set workload.
%%
%%   GET  /sets/:table/:realm/:key
%%        → 200, body = space-separated decimal members,
%%          x-bondy-hlc + x-bondy-node headers
%%
%%   POST /sets/:table/:realm/:key
%%        body: value=<binary>
%%        → 200 after the applier durably installs the {add, Hlc, V, Dot}
%%
%% The OR-set fold expects `dot()` as `{NodeId :: binary(), Counter ::
%% non_neg_integer()}`. The handler generates a node-unique dot via an
%% atomics counter shared across this beam — the node_id binary plus a
%% monotonic counter is a unique pair across the cluster as long as
%% disterl is correctly partitioning nodes by sname.

-export([init/2]).
-export([next_dot/0]).

-define(BODY_OPTS, #{length => 64_000}).
-define(COUNTER_TABLE, bondy_mst_jepsen_dot).

init(Req = #{method := <<"GET">>}, State) ->
    Reply = handle_get(Req),
    {ok, reply(Req, Reply), State};
init(Req0 = #{method := <<"POST">>}, State) ->
    {ok, KeyVals, Req1} =
        cowboy_req:read_urlencoded_body(Req0, ?BODY_OPTS),
    Reply = handle_post(Req1, KeyVals),
    {ok, reply(Req1, Reply), State};
init(Req0, State) ->
    {ok,
     cowboy_req:reply(405,
        #{<<"allow">> => <<"GET, POST">>}, <<>>, Req0),
     State}.

%% =============================================================================
%% GET — return the live set members as a space-separated string
%% =============================================================================

handle_get(Req) ->
    case bind_table(Req) of
        {table, Table} ->
            Realm = cowboy_req:binding(realm, Req),
            Key   = cowboy_req:binding(key,   Req),
            case bondy_db:read(Table, Realm, Key) of
                {ok, Members, Hlc} when is_list(Members) ->
                    %% PR-2 step 2 (2026-05-21): `bondy_db:read/3` now
                    %% returns the **value** (orset → ordset of element
                    %% binaries), not the underlying fold state. Encode
                    %% as a space-separated list for the Jepsen client.
                    {ok, 200, hlc_headers(Hlc), encode_members(Members)};
                {ok, undefined, Hlc} ->
                    {ok, 200, hlc_headers(Hlc), <<>>};
                not_found ->
                    {ok, 200, [], <<>>};
                {error, _} = E ->
                    {error, E}
            end;
        Reply ->
            Reply
    end.

%% =============================================================================
%% POST — add a value to the set with a freshly minted dot
%% =============================================================================

handle_post(Req, KeyVals) ->
    case bind_table(Req) of
        {table, Table} ->
            Realm = cowboy_req:binding(realm, Req),
            Key   = cowboy_req:binding(key,   Req),
            Value = proplists:get_value(<<"value">>, KeyVals, <<>>),
            Hlc   = bondy_db:tick(Table),
            Dot   = next_dot(),
            case bondy_db:apply(Table, Realm, Key,
                                {add, Hlc, Value, Dot}) of
                ok ->
                    {ok, 200, hlc_headers(Hlc), <<>>};
                {error, _} = E ->
                    {error, E}
            end;
        Reply ->
            Reply
    end.

%% =============================================================================
%% Dot generation
%% =============================================================================

%% Generates a fresh OR-set dot `{NodeIdBin, Counter}` unique to this
%% beam. The counter is held in an atomics-backed ETS row keyed by
%% the module — first call lazily initialises it. Atomics is the right
%% primitive (lock-free + monotonic without serialising through a
%% gen_server); the ETS table only serves as a node-wide handle to the
%% atomics ref.
-spec next_dot() -> {binary(), non_neg_integer()}.
next_dot() ->
    Ref = ensure_counter_ref(),
    N = atomics:add_get(Ref, 1, 1),
    {atom_to_binary(node(), utf8), N}.

ensure_counter_ref() ->
    case ets:whereis(?COUNTER_TABLE) of
        undefined ->
            %% Race-free init: first beam to call this creates the
            %% named table; everyone else gets `badarg` from
            %% `ets:new/2` (named-table collision) and falls through
            %% to the lookup branch.
            try ets:new(?COUNTER_TABLE,
                        [named_table, public, set,
                         {read_concurrency, true}]) of
                _ ->
                    Ref = atomics:new(1, [{signed, false}]),
                    true = ets:insert_new(?COUNTER_TABLE, {ref, Ref}),
                    Ref
            catch
                error:badarg ->
                    [{ref, Ref}] = ets:lookup(?COUNTER_TABLE, ref),
                    Ref
            end;
        _ ->
            [{ref, Ref}] = ets:lookup(?COUNTER_TABLE, ref),
            Ref
    end.

%% =============================================================================
%% Helpers (shared shape with bondy_mst_jepsen_http_handler)
%% =============================================================================

bind_table(Req) ->
    TableBin = cowboy_req:binding(table, Req),
    case lists:keyfind(TableBin, 1, table_index()) of
        false        -> {ok, 404, [], <<"unknown-table">>};
        {_, Name}    ->
            case bondy_mst_jepsen_cluster:table(Name) of
                {ok, T} -> {table, T};
                error   -> {ok, 503, [], <<"table-unavailable">>}
            end
    end.

table_index() ->
    [{atom_to_binary(N, utf8), N}
     || N <- bondy_mst_jepsen_cluster:tables()].

encode_members(Members) ->
    %% Space-separated members; same wire shape rakvstore's set workload
    %% uses, so a Clojure `(str/split ...)` recovers the set.
    case Members of
        [] -> <<>>;
        _  ->
            iolist_to_binary(
                lists:join(<<" ">>, lists:sort(Members))
            )
    end.

hlc_headers(Hlc) when is_integer(Hlc) ->
    [{<<"x-bondy-hlc">>, integer_to_binary(Hlc)},
     {<<"x-bondy-node">>, atom_to_binary(node(), utf8)}].

reply(Req, {ok, Status, Headers, Body}) ->
    cowboy_req:reply(Status, headers_map(Headers), Body, Req);
reply(Req, {error, Reason}) ->
    ?LOG_WARNING(#{
        description => "jepsen set http error",
        reason => Reason
    }),
    cowboy_req:reply(503, #{}, io_lib:format("error: ~p", [Reason]), Req).

headers_map(Headers) ->
    maps:from_list([
        {Name, to_value(V)} || {Name, V} <- Headers
    ]).

to_value(V) when is_binary(V) -> V;
to_value(V) when is_list(V)   -> iolist_to_binary(V);
to_value(V) when is_atom(V)   -> atom_to_binary(V, utf8).

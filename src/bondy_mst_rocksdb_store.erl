%% ===========================================================================
%%  bondy_mst_rocksdb_store.erl -
%%
%%  Copyright (c) 2023-2024 Leapsight. All rights reserved.
%%
%%  Licensed under the Apache License, Version 2.0 (the "License");
%%  you may not use this file except in compliance with the License.
%%  You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%%  Unless required by applicable law or agreed to in writing, software
%%  distributed under the License is distributed on an "AS IS" BASIS,
%%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%  See the License for the specific language governing permissions and
%%  limitations under the License.
%% ===========================================================================


-module(bondy_mst_rocksdb_store).
-moduledoc """
This module implements the `bondy_mst_store` behaviour using RocksDB.

As opposed to other backend stores, this module does not offer support for read
concurrency, and as a result:

* Versioning is not implemented, every mutating operation returns a copy of the
map; and
* All calls to `free/2` are made effective immediately by removing the pages
from the map.
""".

-behaviour(bondy_mst_store).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

-define(TX_REF_KEY(DBRef), {?MODULE, DBRef, tx_ref}).
-define(TX_DEPTH_KEY(DBRef), {?MODULE, DBRef, tx_depth}).
-define(TX_EPOCH_KEY(DBRef), {?MODULE, DBRef, tx_epoch}).


-record(?MODULE, {
    ref             ::  rocksdb:db_handle(),
    name            ::  binary(),
    opts            ::  opts_map(),
    root_key        ::  binary()
}).

-type t()           ::  #?MODULE{}.
-type opt()         ::  {name, binary()}
                        | {transactions, tx_kind()}.
-type opts()        ::  [opt()] | opts_map().
-type opts_map()    ::  #{
                            name := binary(),
                            transactions => tx_kind()
                        }.
-type tx_kind()     ::  pessimistic | optimistic.
-type page()        ::  bondy_mst_page:t().


-export_type([t/0]).
-export_type([page/0]).


%% API
-export([copy/3]).
-export([delete/1]).
-export([free/3]).
-export([gc/2]).
-export([get/2]).
-export([get_root/1]).
-export([has/2]).
-export([missing_set/2]).
-export([new/1]).
-export([page_refs/1]).
-export([put/2]).
-export([set_root/2]).
-export([transaction/2]).



%% =============================================================================
%% BONDY_MST_STORE CALLBACKS
%% =============================================================================



-doc """
""".
-spec new(Opts :: opts()) -> t() | no_return().

new(Opts) when is_list(Opts) ->
    new(maps:from_list(Opts));

new(#{name := Name} = Opts0) when is_binary(Name) ->
    DefaultOpts = #{
        name => undefined,
        transactions => optimistic,
        read_concurrency => true
    },

    Opts = maps:merge(DefaultOpts, Opts0),

    %% Only optimistic for the time being
    ok = maps:foreach(
        fun
            (name, undefined = V) ->
                error({badarg, [{name, V}]});

            (transactions, V) ->
                V =:= optimistic orelse error({badarg, [{transactions, V}]});

            (_, _) ->
                ok
        end,
        Opts
    ),


    Ref = persistent_term:get({bondy_mst, rocksdb}),

    #?MODULE{
        ref = Ref,
        name = Name,
        opts = Opts,
        root_key = prefixed_key(Name, ?ROOT_KEY)
    }.


-doc """
""".
-spec get_root(T :: t()) -> Root :: hash() | undefined.

get_root(#?MODULE{root_key = RootKey} = T) ->
    do_get(T, RootKey).


-doc """
""".
-spec set_root(T :: t(), Hash :: hash()) -> t().

set_root(#?MODULE{root_key = RootKey} = T, Hash) ->
     Fun = fun() ->
        ok = do_put(T, RootKey, Hash),
        T
    end,
    transaction(T, Fun).


-doc """
""".
-spec get(T :: t(), Hash :: hash()) -> Page :: page() | undefined.

get(#?MODULE{name = Name} = T, Hash) ->
    do_get(T, prefixed_key(Name, Hash)).


-doc """
""".
-spec has(T :: t(), Hash :: hash()) -> boolean().

has(#?MODULE{name = Name} = T, Hash) when is_binary(Hash) ->
    case do_get(T, prefixed_key(Name, Hash)) of
        {ok, _} ->
            true;

        not_found ->
            false;

        {error, Reason} ->
            error(format_reason(Reason))
    end.


-doc """
""".
-spec put(T :: t(), Page :: page()) -> {Hash :: hash(), T :: t()}.

put(#?MODULE{name = Name, root_key = RootKey} = T, Page) ->
    %% We put and update root atomically
    Fun = fun() ->
        Hash = bondy_mst_utils:hash(Page),
        ok = do_put(T, RootKey, Hash),
        ok = do_put(T, prefixed_key(Name, Hash), encode_value(Page)),
        {Hash, T}
    end,
    transaction(T, Fun).


-doc """
""".
-spec copy(t(), Target :: bondy_mst_store:t(), Hash :: hash()) -> t().

copy(#?MODULE{name = Name} = T, Target, Hash) ->

    case bondy_mst_store:get(Target, Hash) of
        undefined ->
            T;

        Page ->
            PageRefs = bondy_mst_store:page_refs(Target, Page),
            T = lists:foldl(
                fun(PageRef, Acc) -> copy(Acc, Target, PageRef) end,
                T,
                PageRefs
            ),
            Value = encode_value(Page),
            Fun = fun() ->
                TxRef = tx_ref(T),
                Key = prefixed_key(Name, Hash),
                case rocksdb:transaction_put(TxRef, Key, Value) of
                    ok ->
                        T;

                    {error, Reason} ->
                        error(format_reason(Reason))
                end
            end,
            transaction(T, Fun)
    end.


-doc """
Can only be called within a transaction.
""".
-spec free(T :: t(), Hash :: hash(), Page :: page()) -> T :: t() | no_return().

free(#?MODULE{opts = #{read_concurrency := true}} = T, Hash, Page0) ->
    is_in_tx(T) orelse error(not_in_transaction),
    Page = bondy_mst_page:set_freed_at(Page0, tx_epoch(T)),
    ok = do_put(T, prefixed_key(T#?MODULE.name, Hash), encode_value(Page)),
    T;

free(#?MODULE{opts = #{read_concurrency := false}} = T, Hash, _Page) ->
    Fun = fun() ->
        TxRef = tx_ref(T),
        Key = prefixed_key(T#?MODULE.name, Hash),
        case rocksdb:transaction_delete(TxRef, Key) of
            ok ->
                T;

            {error, Reason} ->
                error(format_reason(Reason))
        end
    end,
    transaction(T, Fun).



-doc """
""".
-spec gc(T :: t(), KeepRoots :: [list()]) -> T :: t().

gc(#?MODULE{ref = Ref, opts = #{read_concurrency := true}} = T, Epoch)
when is_integer(Epoch) ->
    {ok, Itr} = rocksdb:iterator(Ref, []),
    {ok, BatchRef} = rocksdb:batch(),
    Name = T#?MODULE.name,
    IterAction = {seek, <<Name/binary, 0>>},

    try
        gc_aux(rocksdb:iterator_move(Itr, IterAction), T, Itr, BatchRef, Epoch),
        case rocksdb:write_batch(Ref, BatchRef, []) of
            ok ->
                ok;

            {error, Reason} ->
                ?LOG_ERROR(#{
                    message => "Error while writing batch to store",
                    reason => Reason
                }),
                ok
        end
    after
        rocksdb:iterator_close(Itr),
        rocksdb:release_batch(BatchRef)
    end;

gc(#?MODULE{opts = #{read_concurrency := true}} = T, KeepRoots)
when is_list(KeepRoots)->
    %% TODO GC
    T;

gc(#?MODULE{opts = #{read_concurrency := false}} = T, _KeepRoots) ->
    %% Do nothing, we free instead
    T.


-doc """
""".
-spec missing_set(T :: t(), Root :: binary()) -> sets:set(page()).

missing_set(T, Root) ->
    case get(T, Root) of
        undefined ->
            sets:from_list([Root], [{version, 2}]);

        Page ->
            lists:foldl(
                fun(P, Acc) -> sets:union(Acc, missing_set(T, P)) end,
                sets:new([{version, 2}]),
                page_refs(Page)
            )
    end.


-doc """
""".
-spec page_refs(Page :: page()) -> [binary()].

page_refs(Page) ->
    bondy_mst_page:refs(Page).


-spec delete(t()) -> ok.

delete(#?MODULE{ref = _Ref, name = _Name}) ->
    %% TODO fold over bucket (name) elements and delete them
    ok.



-doc """
""".
-spec transaction(t(), fun(() -> any())) -> any() | no_return().

transaction(#?MODULE{} = T, Fun) ->
    try
        ok = maybe_begin_tx(T),
        Result = Fun(),
        ok = maybe_commit_tx(T),
        Result
    catch
        throw:Reason0:Stacktrace ->
            Reason = format_reason(Reason0),
            %% An internal exception
            %% We force a rollback
            ok = maybe_rollback(T),
            ?LOG_ERROR(#{
                message => "Transaction rollback",
                reason => Reason,
                stacktrace => Stacktrace
            }),
            maybe_throw(is_nested_tx(T), Reason);

        error:Reason0:Stacktrace ->
            Reason = format_reason(Reason0),
            %% A user exception, we need to raise it again up the
            %% nested transation stack and out
            %% We force a rollback
            ok = maybe_rollback(T),
            ?LOG_ERROR(#{
                message => "Transaction error",
                reason => Reason,
                stacktrace => Stacktrace
            }),
            error(Reason)
    after
        ok = maybe_release_tx(T)
    end.




%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
do_get(T, Key) ->
    do_get(T, Key, undefined).


%% @private
do_get(#?MODULE{ref = Ref, root_key = RootKey} = T, Key, Default)
when is_binary(Key) ->

    Result = case is_in_tx(T) of
        true ->
            rocksdb:transaction_get(tx_ref(T), Key, []);

        false ->
            rocksdb:get(Ref, Key, [])
    end,

    case Result of
        {ok, Hash} when Key == RootKey ->
            Hash;

        {ok, Bin} ->
            binary_to_term(Bin);

        not_found ->
            Default;

        {error, Reason} ->
            error(format_reason(Reason))
    end.


%% @private
do_put(#?MODULE{} = T, Key, Value) when is_binary(Key), is_binary(Value) ->
    TxRef = tx_ref(T),

    case rocksdb:transaction_put(TxRef, Key, Value) of
        ok ->
            ok;

        {error, Reason} ->
            error(format_reason(Reason))
    end.


%% @private
prefixed_key(Name, ?ROOT_KEY) ->
    <<?ROOT_KEY/binary, 0, Name/binary>>;

prefixed_key(Name, Hash) when is_binary(Name) andalso is_binary(Hash) ->
    <<Name/binary, 0, Hash/binary>>.


%% @private
encode_value(Value)->
    term_to_binary(Value, ?T2B_OPTS).




%% ============================================================================
%% PRIVATE: TRANSACTIONS
%% ============================================================================



%% @private
maybe_begin_tx(T) ->
    %% We should increase tx_depth before calling is_nested_tx/1
    ok = increment_tx_depth(T),

    case is_nested_tx(T) of
        true ->
            ok;

        false ->
            begin_tx(T)
    end.


%% @private
begin_tx(#?MODULE{ref = Ref}) ->
    case rocksdb:transaction(Ref, []) of
        {ok, TxRef} ->
            Epoch = erlang:system_time(millisecond),
            undefined = erlang:put(?TX_REF_KEY(Ref), TxRef),
            undefined = erlang:put(?TX_EPOCH_KEY(Ref), Epoch),
            ok;

        {error, Reason} ->
            throw(Reason)
    end.


%% @private
tx_ref(#?MODULE{ref = Ref}) ->
    erlang:get(?TX_REF_KEY(Ref)).


%% @private
tx_epoch(#?MODULE{ref = Ref}) ->
    erlang:get(?TX_EPOCH_KEY(Ref)).


%% @private
maybe_commit_tx(T) ->
    case is_nested_tx(T) of
        true ->
            ok;

        false ->
            commit_tx(T)
    end.



%% @private
commit_tx(#?MODULE{ref = Ref}) ->
    TxRef = erlang:get(?TX_REF_KEY(Ref)),

    case rocksdb:transaction_commit(TxRef) of
        ok ->
            ok;

        {error, Reason} ->
            error(format_reason(Reason))
    end.


%% @private
maybe_rollback(#?MODULE{ref = Ref} = T) ->
    case is_nested_tx(T) of
        true ->
            ok;

        false ->
            TxRef = erlang:get(?TX_REF_KEY(Ref)),

            case rocksdb:transaction_rollback(TxRef) of
                ok ->
                    ok;

                {error, Reason} ->
                    error(format_reason(Reason))
            end
    end.


%% @private
maybe_release_tx(#?MODULE{} = T) ->
    case is_nested_tx(T) of
        true ->
            ok = decrement_tx_depth(T);

        false ->
            ok = decrement_tx_depth(T),
            release_tx(T)
    end.


%% @private
release_tx(#?MODULE{ref = Ref}) ->
    _ = erlang:erase(?TX_EPOCH_KEY(Ref)),
    TxRef = erlang:erase(?TX_REF_KEY(Ref)),
    ok = rocksdb:release_transaction(TxRef),
    ok.


%% @private
increment_tx_depth(#?MODULE{ref = Ref}) ->
    Key = ?TX_DEPTH_KEY(Ref),

    case erlang:get(Key) of
        undefined ->
            undefined = erlang:put(Key, 1),
            ok;

        N when N >= 1 ->
            N = erlang:put(Key, N + 1),
            ok
    end.


%% @private
decrement_tx_depth(#?MODULE{ref = Ref}) ->
    Key = ?TX_DEPTH_KEY(Ref),

    case erlang:get(Key) of
        1 ->
            1 = erlang:erase(Key),
            ok;

        N when N > 1 ->
            N = erlang:put(Key, N - 1),
            ok
    end.


%% @private
tx_depth(#?MODULE{ref = Ref}) ->
    case erlang:get(?TX_DEPTH_KEY(Ref)) of
        undefined ->
            0;

        N when N >= 1 ->
            N
    end.


%% @private
is_nested_tx(#?MODULE{} = T) ->
    tx_depth(T) > 1.


%% @private
is_in_tx(#?MODULE{} = T) ->
    tx_ref(T) =/= undefined.


%% @private
-spec maybe_throw(InTx :: boolean(), any()) -> no_return() | {error, any()}.

maybe_throw(true, Reason) ->
    throw(Reason);

maybe_throw(false, Reason) ->
    {error, Reason}.

format_reason({error, Reason}) ->
    %% rocksdb:transaction_ API returns {error, {error, Reason}}
    format_reason(Reason);

format_reason("Resource busy: ") ->
    optimistic_tx_conflict;

format_reason(Term) ->
    Term.



%% @private
gc_aux({error, iterator_closed}, _T, _Itr, _BatchRef, _Epoch) ->
  throw(iterator_closed);

gc_aux({error, invalid_iterator}, _T, _Itr, _BatchRef, _Epoch) ->
  ok;

%% gc_aux({ok, K, _}, Itr, #?MODULE{root_key = K} = T, BatchRef, Epoch) ->
%%     gc_aux(rocksdb:iterator_move(Itr, next), T, Itr, BatchRef, Epoch);

gc_aux({ok, K, V}, T, Itr, BatchRef, Epoch) ->
    Page = binary_to_term(V),
    _ = case bondy_mst_page:freed_at(Page) =< Epoch of
        true ->
            io:format(">>>>>>>> will delete ~p~n", [K]),
            ok = rocksdb:batch_delete(BatchRef, K);

        false ->
            ok
    end,
    gc_aux(rocksdb:iterator_move(Itr, next), T, Itr, BatchRef, Epoch).




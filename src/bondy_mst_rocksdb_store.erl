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

%% -----------------------------------------------------------------------------
%% @doc This module implements the `bondy_mst_store' behaviour using RocksDB.
%% This store offers read-concurrency via the use of persistent data sturcture
%% concepts.
%% @end
%% -----------------------------------------------------------------------------
-module(bondy_mst_rocksdb_store).

-behaviour(bondy_mst_store).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

-define(TX_REF_KEY(DBRef), {?MODULE, DBRef, tx_ref}).
-define(TX_DEPTH_KEY(DBRef), {?MODULE, DBRef, tx_depth}).
-define(TX_EPOCH_KEY(DBRef), {?MODULE, DBRef, tx_epoch}).


-record(?MODULE, {
    name                ::  binary(),
    ref                 ::  rocksdb:db_handle(),
    hashing_algorithm   ::  atom(),
    opts                ::  opts_map(),
    root_key            ::  binary()
}).

-type t()               ::  #?MODULE{}.
-type opt()             ::  {name, binary()}
                            | {transactions, tx_kind()}
                            | {persistent, boolean()}.
-type opts()            ::  [opt()] | opts_map().
-type opts_map()        ::  #{
                                name := binary(),
                                transactions => tx_kind(),
                                persistent => boolean()
                            }.
-type tx_kind()         ::  pessimistic | optimistic.
-type page()            ::  bondy_mst_page:t().


-export_type([t/0]).
-export_type([page/0]).


%% API
-export([copy/3]).
-export([close/1]).
-export([delete/1]).
-export([free/3]).
-export([gc/2]).
-export([get/2]).
-export([get_root/1]).
-export([has/2]).
-export([list/1]).
-export([missing_set/2]).
-export([open/2]).
-export([page_refs/1]).
-export([put/2]).
-export([set_root/2]).
-export([transaction/2]).



%% =============================================================================
%% BONDY_MST_STORE CALLBACKS
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec open(Algo :: atom(), Opts :: opts()) -> t() | no_return().

open(Algo, Opts) when is_atom(Algo), is_list(Opts) ->
    open(Algo, maps:from_list(Opts));

open(Algo, Opts0) when is_atom(Algo), is_map(Opts0) ->
    DefaultOpts = #{
        name => undefined,
        transactions => optimistic,
        persistent => true
    },

    Opts = maps:merge(DefaultOpts, Opts0),

    ok = maps:foreach(
        fun
            (name, V) ->
                is_binary(V)
                orelse error({badarg, [{name, V}]});

            (transactions, V) ->
                %% Only optimistic for the time being

                V =:= optimistic
                orelse error({badarg, [{transactions, V}]});

            (persistent, V) ->
                is_boolean(V)
                orelse error({badarg, [{persistent, V}]})
        end,
        Opts
    ),

    Name = maps:get(name, Opts),
    Ref = persistent_term:get({bondy_mst, rocksdb}),

    #?MODULE{
        name = Name,
        ref = Ref,
        hashing_algorithm = Algo,
        opts = Opts,
        root_key = prefixed_key(Name, ?ROOT_KEY)
    }.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec close(t()) -> ok.

close(#?MODULE{}) ->
    ok.



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_root(T :: t()) -> Root :: hash() | undefined.

get_root(#?MODULE{root_key = RootKey} = T) ->
    do_get(T, RootKey).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec set_root(T :: t(), Hash :: hash()) -> t().

set_root(#?MODULE{root_key = RootKey} = T, Hash) ->
     Fun = fun() ->
        ok = do_put(T, RootKey, Hash),
        T
    end,
    transaction(T, Fun).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get(T :: t(), Hash :: hash()) -> Page :: page() | undefined.

get(#?MODULE{name = Name} = T, Hash) when is_binary(Hash) ->
    do_get(T, prefixed_key(Name, Hash)).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec has(T :: t(), Hash :: hash()) -> boolean().

has(#?MODULE{name = Name} = T, Hash) when is_binary(Hash) ->
    Result = do_get(T, prefixed_key(Name, Hash)),
    Result =/= undefined.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec put(T :: t(), Page :: page()) -> {Hash :: hash(), T :: t()}.

put(#?MODULE{name = Name, hashing_algorithm = Algo} = T, Page) ->
    bondy_mst_page:is_type(Page) orelse error(badarg),

    Fun = fun() ->
        Hash = bondy_mst_page:hash(Page, Algo),
        ok = do_put(T, prefixed_key(Name, Hash), encode_value(Page)),
        {Hash, T}
    end,
    transaction(T, Fun).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
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


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec list(t()) -> [page()].

list(#?MODULE{ref = Ref} = T) ->
    Name = T#?MODULE.name,
    {ok, Itr} = rocksdb:iterator(Ref, []),
    Prefix = prefixed_key(Name),
    IterAction = {seek, Prefix},

    try
        do_list(rocksdb:iterator_move(Itr, IterAction), T, Prefix, Itr, [])
    after
        rocksdb:iterator_close(Itr)
    end.



%% -----------------------------------------------------------------------------
%% @doc Can only be called within a transaction.
%% @end
%% -----------------------------------------------------------------------------
-spec free(T :: t(), Hash :: hash(), Page :: page()) -> T :: t() | no_return().

free(#?MODULE{opts = #{persistent := true}} = T, Hash, Page0) ->
    %% We keep the hash and page, marking it free. We then gc/2 to actually
    %% delete them.
    is_in_tx(T) orelse error(not_in_transaction),
    Page = bondy_mst_page:set_freed_at(Page0, tx_epoch(T)),
    ok = do_put(T, prefixed_key(T#?MODULE.name, Hash), encode_value(Page)),
    T;

free(#?MODULE{opts = #{persistent := false}} = T, Hash, _Page) ->
    %% We immediately delete
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



%% -----------------------------------------------------------------------------
%% @doc This call can be made concurrently i.e. in a different process than the
%% owner of the tree.
%% @end
%% -----------------------------------------------------------------------------
-spec gc(T :: t(), KeepRoots :: [list()]) -> T :: t().

gc(#?MODULE{ref = Ref, opts = #{persistent := true}} = T, Epoch)
when is_integer(Epoch) ->
    {ok, Itr} = rocksdb:iterator(Ref, []),
    {ok, BatchRef} = rocksdb:batch(),
    Name = T#?MODULE.name,
    Prefix = prefixed_key(Name),
    IterAction = {seek, Prefix},

    try
        ok = gc_batch_delete(
            rocksdb:iterator_move(Itr, IterAction),
            T, Prefix, Itr, BatchRef, Epoch
            ),

        case rocksdb:write_batch(Ref, BatchRef, []) of
            ok ->
                T;

            {error, Reason} ->
                ?LOG_ERROR(#{
                    message => "Error while writing batch to store",
                    reason => Reason
                }),
                T
        end
    after
        rocksdb:iterator_close(Itr),
        rocksdb:release_batch(BatchRef)
    end;

gc(#?MODULE{opts = #{persistent := true}} = T, KeepRoots)
when is_list(KeepRoots) ->
    %% Review: not supported yet
    T;

gc(#?MODULE{opts = #{persistent := false}} = T, _KeepRoots) ->
    %% No garbage as free/3 deletes immediately
    T.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec missing_set(T :: t(), Root :: binary()) -> sets:set(hash()).

missing_set(T, Root) ->
    case get(T, Root) of
        undefined ->
            sets:from_list([Root], [{version, 2}]);

        Page ->
            lists:foldl(
                fun(Hash, Acc) -> sets:union(Acc, missing_set(T, Hash)) end,
                sets:new([{version, 2}]),
                page_refs(Page)
            )
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec page_refs(Page :: page()) -> [hash()].

page_refs(Page) ->
    bondy_mst_page:refs(Page).


-spec delete(t()) -> ok.

delete(#?MODULE{ref = _Ref, name = _Name}) ->
    %% TODO fold over bucket (name) elements and delete them
    ok.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
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
prefixed_key(Name) when is_binary(Name)->
    <<Name/binary, 0>>.


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
            Epoch = erlang:monotonic_time(),
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
gc_batch_delete({error, iterator_closed}, _T, _Pref, _Itr, _BatchRef, _Epoch) ->
  throw(iterator_closed);

gc_batch_delete({error, invalid_iterator}, _T, _Pref, _Itr, _BatchRef, _Epoch) ->
  ok;

gc_batch_delete({ok, K, V}, T, Prefix, Itr, BatchRef, Epoch) ->
    case binary:longest_common_prefix([K, Prefix]) == byte_size(Prefix) of
        true ->
            Page = binary_to_term(V),
            _ = case bondy_mst_page:freed_at(Page) =< Epoch of
                true ->
                    ok = rocksdb:batch_delete(BatchRef, K);

                false ->
                    ok
            end,
            Action = rocksdb:iterator_move(Itr, next),
            gc_batch_delete(Action, T, Prefix, Itr, BatchRef, Epoch);

        false ->
            %% We finished iterating with Prefix
            ok
    end.


%% @private
do_list({error, iterator_closed}, _, _, _, _) ->
    throw(iterator_closed);

do_list({error, invalid_iterator}, _, _, _, Acc) ->
    Acc;

do_list({ok, K, V}, T, Prefix, Itr, Acc) ->
    case binary:longest_common_prefix([K, Prefix]) == byte_size(Prefix) of
        true ->
            Page = binary_to_term(V),
            Action = rocksdb:iterator_move(Itr, next),
            do_list(Action, T, Prefix, Itr, [Page | Acc]);

        false ->
            Acc
    end.
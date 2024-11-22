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
Non-concurrent, MST backend using `rocksdb`.
""".

-behaviour(bondy_mst_store).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

-define(TX_REF_KEY(DBRef), {?MODULE, DBRef, tx_ref}).
-define(TX_DEPTH_KEY(DBRef), {?MODULE, DBRef, tx_depth}).

-record(?MODULE, {
    ref         ::  rocksdb:db_handle(),
    name        ::  binary()
}).

-type t()       ::  #?MODULE{}.
-type page()    ::  bondy_mst_page:t().

-export_type([t/0]).
-export_type([page/0]).


%% API
-export([copy/3]).
-export([delete/1]).
-export([free/2]).
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
-spec new(Opts :: map() | list()) -> t().

new(Opts) when is_list(Opts) ->
    new(maps:from_list(Opts));

new(#{name := Name} = _Opts) when is_binary(Name) ->
    Ref = persistent_term:get({bondy_mst, rocksdb}),
    #?MODULE{ref = Ref, name = Name}.


-doc """
""".
-spec get_root(T :: t()) -> Root :: hash() | undefined.

get_root(#?MODULE{} = T) ->
    do_get(T, ?ROOT_KEY).


-doc """
""".
-spec set_root(T :: t(), Hash :: hash()) -> t().

set_root(#?MODULE{} = T, Hash) ->
    ok = do_put(T, ?ROOT_KEY, Hash),
    T.


-doc """
""".
-spec get(T :: t(), Hash :: binary()) -> Page :: page() | undefined.

get(#?MODULE{} = T, Hash) ->
    do_get(T, Hash).


-doc """
""".
-spec has(T :: t(), Hash :: binary()) -> boolean().

has(#?MODULE{} = T, Hash) ->
    case do_get(T, Hash) of
        {ok, _} ->
            true;

        not_found ->
            false;

        {error, Reason} ->
            error(format_reason(Reason))
    end.


-doc """
""".
-spec put(T :: t(), Page :: page()) -> {Hash :: binary(), T :: t()}.

put(#?MODULE{} = T, Page) ->
    Hash = bondy_mst_utils:hash(Page),
    %% We put and update root atomically
    Fun = fun() ->
        ok = do_put(T, Hash, Page),
        T = set_root(T, Hash),
        {Hash, T}
    end,
    {Hash, T} = transaction(T, Fun).


-doc """
""".
-spec copy(t(), Target :: bondy_mst_store:t(), Hash :: binary()) -> t().

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
            Value = term_to_binary(Page, ?T2B_OPTS),
            Fun = fun() ->
                TxRef = tx_ref(T),
                Key = encode_key(Name, Hash),
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
""".
-spec free(T :: t(), Hash :: binary()) -> T :: t().

free(#?MODULE{name = Name} = T, Hash) ->
    Fun = fun() ->
        TxRef = tx_ref(T),
        Key = encode_key(Name, Hash),
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

gc(#?MODULE{} = T, _KeepRoots) ->
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
                message => "Transaction error",
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
do_get(#?MODULE{ref = Ref, name = Name} = T, Hash)
when is_binary(Hash) orelse Hash =:= ?ROOT_KEY ->

    Result = case is_in_tx(T) of
        true ->
            rocksdb:transaction_get(tx_ref(T), encode_key(Name, Hash), []);

        false ->
            rocksdb:get(Ref, encode_key(Name, Hash), [])
    end,

    case Result of
        {ok, Bin} when Hash =:= ?ROOT_KEY ->
            Bin;

        {ok, Bin} ->
            binary_to_term(Bin);

        not_found ->
            undefined;

        {error, Reason} ->
            error(format_reason(Reason))
    end.


%% @private
do_put(#?MODULE{} = T, ?ROOT_KEY, Hash) when is_binary(Hash) ->
    do_put_tx(T, ?ROOT_KEY, Hash);

do_put(#?MODULE{} = T, Hash, Value) when is_binary(Hash) ->
    do_put_tx(T, Hash, term_to_binary(Value, ?T2B_OPTS)).

%% @private
do_put_tx(#?MODULE{name = Name} = T, Hash, Value)
when is_binary(Hash) orelse Hash =:= ?ROOT_KEY ->
    Fun = fun() ->
        TxRef = tx_ref(T),
        Key = encode_key(Name, Hash),

        case rocksdb:transaction_put(TxRef, Key, Value) of
            ok ->
                ok;

            {error, Reason} ->
                error(format_reason(Reason))
        end
    end,
    transaction(T, Fun).


encode_key(Name, Key) when is_binary(Name) andalso is_binary(Key) ->
    <<Name/binary, 0, Key/binary>>;

encode_key(Name, ?ROOT_KEY) ->
    <<Name/binary, 0, (term_to_binary(?ROOT_KEY, ?T2B_OPTS))/binary>>.

%% decode_key(Bin) ->
%%     [Bin1, Bin2] = binary:split(Bin, <<0>>),
%%     {binary_to_term(Bin1), binary_to_term(Bin2)}.



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
            undefined = erlang:put(?TX_REF_KEY(Ref), TxRef),
            ok;

        {error, Reason} ->
            throw(Reason)
    end.


%% @private
tx_ref(#?MODULE{ref = Ref}) ->
    erlang:get(?TX_REF_KEY(Ref)).


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




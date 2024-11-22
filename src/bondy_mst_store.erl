

-module(bondy_mst_store).
-moduledoc """
Behaviour to be implemented for page stores to allow their manipulation.

This behaviour may also be implemented by store proxies that track
operations and implement different synchronization or caching mechanisms.
""".

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").


-record(?MODULE, {
    mod                 ::  module(),
    state               ::  backend(),
    tx_supported        ::  boolean()
}).

-type t()               ::  #?MODULE{}.
-type page()            ::  any().
-type backend()         ::  any().

-export_type([t/0]).
-export_type([backend/0]).
-export_type([page/0]).

%% API
-export([copy/3]).
-export([delete/1]).
-export([free/2]).
-export([gc/2]).
-export([get/2]).
-export([get_root/1]).
-export([has/2]).
-export([is_type/1]).
-export([missing_set/2]).
-export([new/2]).
-export([page_refs/2]).
-export([put/2]).
-export([set_root/2]).
-export([transaction/2]).


%% =============================================================================
%% CALLBACKS
%% =============================================================================


-callback new(Opts :: map()) -> backend().

-callback get_root(backend()) -> hash() | undefined.

-callback set_root(backend(), hash()) -> backend().

-callback get(backend(), page()) -> page() | undefined.

-callback has(backend(), page()) -> boolean().

-callback put(backend(), page()) -> {Hash :: binary(), backend()}.

-callback copy(backend(), OtherStore :: t(), Hash :: binary()) -> backend().

-callback free(backend(), Hash :: binary()) -> backend().

-callback gc(backend(), KeepRoots :: [list()]) -> backend().

-callback missing_set(backend(), Root :: binary()) -> [Pages :: list()].

-callback page_refs(Page :: page()) -> Refs :: [binary()].

-callback delete(backend()) -> ok.

-callback transaction(backend(), Fun :: fun(() -> any())) ->
    any() | no_return().

-optional_callbacks([transaction/2]).

%% =============================================================================
%% API
%% =============================================================================



-spec new(Mod :: module(), Opts :: map()) -> t() | no_return().

new(Mod, Opts) when is_atom(Mod) andalso (is_map(Opts) orelse is_list(Opts)) ->
    #?MODULE{
        mod = Mod,
        state = Mod:new(Opts),
        tx_supported = supports_transactions(Mod)
    }.


-spec is_type(any()) -> boolean().

is_type(#?MODULE{}) -> true;
is_type(_) -> false.


-doc """
Get the root page.

Returns page or `undefined`.
""".
-spec get_root(Store :: t()) -> Root :: hash() | undefined.

get_root(#?MODULE{mod = Mod, state = State}) ->
    Mod:get_root(State).


-doc """
Get the root page.

Returns page or `undefined`.
""".
-spec set_root(Store :: t(), Hash :: binary()) -> ok.

set_root(#?MODULE{mod = Mod, state = State0} = T, Hash) when is_binary(Hash) ->
    State = Mod:set_root(State0, Hash),
    T#?MODULE{state = State}.


-doc """
Get a page referenced by its hash.

Returns page or `undefined`.
""".
-spec get(Store :: t(), Page :: page()) -> Page :: page() | undefined.

get(#?MODULE{mod = Mod, state = State}, Page) ->
    Mod:get(State, Page).


-doc """
""".
-spec has(Store :: t(), Page :: page()) -> boolean().

has(#?MODULE{mod = Mod, state = State}, Page) ->
    Mod:has(State, Page).


-doc """
Put a page. Argument is the content of the page, returns the
hash that the store has associated to it.

Returns {hash, store}
""".
-spec put(Store :: t(), Page :: page()) -> {Hash :: binary(), Store :: t()}.

put(#?MODULE{mod = Mod, state = State0} = T, Page) ->
    {Hash, State} = Mod:put(State0, Page),
    {Hash, T#?MODULE{state = State}}.


-doc """
""".
-spec copy(Store :: t(), OtherStore :: t(), Hash :: binary()) -> Store :: t().

copy(#?MODULE{mod = Mod, state = State0} = T, OtherStore, Hash) ->
    T#?MODULE{state = Mod:copy(State0, OtherStore, Hash)}.


-doc """
""".
-spec free(Store :: t(), Hash :: binary()) -> Store :: t().

free(#?MODULE{mod = Mod, state = State0} = T, Hash) ->
    T#?MODULE{state = Mod:free(State0, Hash)}.


-doc """
""".
-spec gc(Store :: t(), KeepRoots :: [list()]) -> Store :: t().

gc(#?MODULE{mod = Mod, state = State0} = T, KeepRoots) ->
    T#?MODULE{state = Mod:gc(State0, KeepRoots)}.


-doc """
""".
-spec page_refs(Store :: t(), Page :: page()) -> Refs :: [binary()].

page_refs(#?MODULE{mod = Mod}, Page) ->
    Mod:page_refs(Page).


-doc """
Get the list of pages we know we are missing starting at this root.
""".
-spec missing_set(Store :: t(), Root :: binary()) -> [Pages :: list()].

missing_set(#?MODULE{mod = Mod, state = State}, Root) ->
    Mod:missing_set(State, Root).


-doc """
""".
-spec delete(Store :: t()) -> ok.

delete(#?MODULE{mod = Mod, state = State}) ->
    Mod:delete(State).




%% =============================================================================
%% TRANSACTION API
%% =============================================================================


-doc """
""".
-spec transaction(Store :: t(), Fun :: fun(() -> any())) ->
    any() | {error, Reason :: any()}.

transaction(#?MODULE{tx_supported = true, mod = Mod, state = State}, Fun) ->
    Mod:transaction(State, Fun);

transaction(#?MODULE{tx_supported = false}, Fun) ->
    Fun().



%% =============================================================================
%% PRIVATE
%% =============================================================================



supports_transactions(Mod) ->
    erlang:function_exported(Mod, module_info, 0)
        orelse code:ensure_loaded(Mod),

    erlang:function_exported(Mod, transaction, 2).


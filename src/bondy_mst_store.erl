

-module(bondy_mst_store).
-moduledoc """
Behaviour to be implemented for page stores to allow their manipulation.

This behaviour may also be implemented by store proxies that track
operations and implement different synchronization or caching mechanisms.
""".

-record(?MODULE, {
    mod :: module(),
    state :: backend()
}).

-type t()               ::  #?MODULE{}.
-type page()            ::  any().
-type backend()     ::  any().

-export_type([t/0]).
-export_type([backend/0]).
-export_type([page/0]).

%% API
-export([new/2]).
-export([get/2]).
-export([has/2]).
-export([put/2]).
-export([copy/3]).
-export([missing_set/2]).
-export([page_refs/2]).
-export([free/2]).
-export([gc/2]).



%% =============================================================================
%% CALLBACKS
%% =============================================================================


-callback new(Opts :: map()) -> backend().

-callback get(backend(), page()) -> page() | undefined.

-callback has(backend(), page()) -> boolean().

-callback put(backend(), page()) -> {Hash :: binary(), backend()}.

-callback copy(backend(), OtherStore :: t(), Hash :: binary()) -> backend().

-callback free(backend(), Hash :: binary()) -> backend().

-callback gc(backend(), KeepRoots :: [list()]) -> backend().

-callback missing_set(backend(), Root :: binary()) -> [Pages :: list()].

-callback page_refs(Page :: page()) -> Refs :: [binary()].


%% =============================================================================
%% API
%% =============================================================================



-spec new(Mod :: module(), Opts :: map()) -> t() | no_return().

new(Mod, Opts) when is_atom(Mod) andalso (is_map(Opts) orelse is_list(Opts)) ->
    #?MODULE{mod = Mod, state = Mod:new(Opts)}.


-spec is_type(any()) -> boolean().

is_type(#?MODULE{}) -> true;
is_type(#?MODULE{}) -> false.


-doc """
Get a page referenced by its hash.

Returns page
""".
-spec get(Store :: t(), Page :: page()) -> Page :: page().

get(#?MODULE{mod = Mod, state = State}, Page) ->
    Mod:get(State, Page).



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


-spec copy(Store :: t(), OtherStore :: t(), Hash :: binary()) -> Store :: t().

copy(#?MODULE{mod = Mod, state = State0} = T, OtherStore, Hash) ->
    T#?MODULE{state = Mod:copy(State0, OtherStore, Hash)}.


-spec free(Store :: t(), Hash :: binary()) -> Store :: t().

free(#?MODULE{mod = Mod, state = State0} = T, Hash) ->
    T#?MODULE{state = Mod:free(State0, Hash)}.


-spec gc(Store :: t(), KeepRoots :: [list()]) -> Store :: t().

gc(#?MODULE{mod = Mod, state = State0} = T, KeepRoots) ->
    T#?MODULE{state = Mod:gc(State0, KeepRoots)}.


-spec page_refs(Store :: t(), Page :: page()) -> Refs :: [binary()].

page_refs(#?MODULE{mod = Mod}, Page) ->
    Mod:page_refs(Page).


-doc """
Get the list of pages we know we are missing starting at this root.
""".
-spec missing_set(Store :: t(), Root :: binary()) -> [Pages :: list()].

missing_set(#?MODULE{mod = Mod, state = State}, Root) ->
    Mod:missing_set(State, Root).



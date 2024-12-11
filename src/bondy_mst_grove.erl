%% ===========================================================================
%%  bondy_mst.erl -
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
%%
%%  This module contains a port the code written in Elixir for the
%%  simulations shown in the paper: Merkle Search Trees: Efficient State-Based
%%  CRDTs in Open Networks by Alex Auvolat, François Taïani
%% ===========================================================================

%% -----------------------------------------------------------------------------
%% @doc
%% This module implements a sychronised group of Merkle Search Trees (trees)
%% replicas across a cluster.
%%
%% This module implements a the logic for anti-entropy exchanges. The idea is
%% for the user to choose the right infrastructure e.g. using this module as
%% helper for a `gen_server' or `gen_statem'
%%
%% Anti-entropy exchanges are performed in the background and without blocking
%% local operations. The underlying tree is not changed until all the remote
%% information necessary for the merge is obtained from a peer.
%%
%% This module allows a set of remote trees that we want to merge with the local
%% tree to be kept in the state (`#?bondy_mst_grove.merges') and all missing
%% pages are requested to the remote peers. Once all pages are locally
%% available, the merge operation is done without network communication and the
%% local tree is updated.
%%
%% The number of active exchanges is limited by the option `max_merges'.
%%
%% ## Network Operations
%% This module relies on a callback module provided by you that implements the following callbacks:
%%
%% * send/2
%% * broadcast/1
%% * on_merge/1
%% @end
%% -----------------------------------------------------------------------------

-module(bondy_mst_grove).


-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

-record(?MODULE, {
    node_id                 ::  node_id(),
    tree                    ::  bondy_mst:t(),
    callback_mod            ::  module(),
    max_merges = 6          ::  pos_integer(),
    max_same_merge = 1      ::  pos_integer(),
    merges = #{}            ::  #{node_id() => hash()},
    last_broadcast_time     ::  integer() | undefined
}).

-record(event, {
    from                    ::  node_id(),
    root                    ::  hash(),
    key                     ::  any(),
    value                   ::  any()
}).

-record(get, {
    from                    ::  node_id(),
    root                    ::  hash(),
    set                     ::  sets:set(hash())
}).

-record(put, {
    from                    ::  node_id(),
    set                     ::  sets:set(hash())
}).

-record(missing, {
    from                    ::  node_id()
}).


-type t()                   ::  #?MODULE{}.


%% Normally a node() but we allow a binary for simulation purposes
-type node_id()             ::  node() | binary().

-type opts()                ::  [bondy_mst:opt() | opt()]
                                | opts_map().
-type opt()                 ::  {max_merges, pos_integer()}
                                | {max_same_merge, pos_integer()}
                                | {callback_mod, module()}.
-type opts_map()            ::  #{
                                    store => bondy_mst_store:t(),
                                    merger => bondy_mst:merger(),
                                    comparator => bondy_mst:comparator(),
                                    max_merges => pos_integer(),
                                    max_same_merge => pos_integer(),
                                    callback_mod => module()
                                }.

-type event()               ::  #event{}.
-type get_cmd()             ::  #get{}.
-type missing_cmd()         ::  #missing{}.
-type put_cmd()             ::  #put{}.
-type message()             ::  event()
                                | get_cmd()
                                | put_cmd()
                                | missing_cmd().

-export_type([t/0]).
-export_type([event/0]).
-export_type([node_id/0]).
-export_type([message/0]).

-export([new/2]).
-export([tree/1]).
-export([handle/2]).
-export([event_data/1]).
-export([trigger/2]).
-export([put/3]).



%% =============================================================================
%% CALLBACKS
%% =============================================================================



%% Called when this module wants to send a message to a peer.
-callback send(Peer :: node_id(), message()) -> ok | {error, any()}.


%% Whenever this module wants to gossip an event it will call
%% `Module:broadcast/1'.
%% The callback `Module' is responsible for sending the event to some random peers, either by implementing or using a peer sampling service e.g.
%% `partisan_plumtree_broadcast'.
-callback broadcast(Event :: event()) -> ok | {error, any()}.


%% Called after merging a tree page from a remote tree.
%% The implemeneter can use this to extract the entries and merge the data in a
%% main store when the tree is used only for anti-entropy.
-callback on_merge(Page :: bondy_mst_page:t()) -> ok.


-optional_callbacks([on_merge/1]).



%% =============================================================================
%% API
%% =============================================================================



-spec new(node_id(), opts()) -> Grove :: t() | no_return().

new(NodeId, Opts) when is_list(Opts) ->
    new(NodeId, maps:from_list(Opts));

new(NodeId, Opts0) when
(is_atom(NodeId) orelse is_binary(NodeId)) andalso is_map(Opts0) ->
    %% Configure the tree
    TreeOpts = maps:with([store, merger, comparator], Opts0),
    Tree = bondy_mst:new(TreeOpts),

    %% Configure the grove
    GroveOpts = maps:with([callback_mod, max_merges, max_same_merge], Opts0),
    CallbackMod = validate_callback_mod(GroveOpts),
    MaxMerges = maps:get(max_merges, GroveOpts, 6),
    MaxSameMerges = maps:get(max_same_merge, GroveOpts, 1),

    #?MODULE{
        node_id = NodeId,
        callback_mod = CallbackMod,
        tree = Tree,
        max_merges = MaxMerges,
        max_same_merge = MaxSameMerges
    }.



%% -----------------------------------------------------------------------------
%% @doc Returns the underlying tree.
%% @end
%% -----------------------------------------------------------------------------
-spec tree(t()) -> bondy_mst:t().

tree(#?MODULE{tree = Tree}) ->
    Tree.


%% =============================================================================
%% API: TREE API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Calls `bondy_mst:put/3' and if the operation changed the tree (previous
%% and new root differ), it bradcasts the change to the peers by calling
%% the callback's `Module:broadcast/1' with an `event()'.
%%
%% When the event is received by the peer it must handle it calling `handle/2'
%% on its local grove.
%% @end
%% -----------------------------------------------------------------------------
-spec put(Grove0 :: t(), Key :: any(), Value :: any()) -> Grove1 :: t().

put(Grove0, Key, Value) ->
    Tree0 = Grove0#?MODULE.tree,
    Root0 = bondy_mst:root(Tree0),
    Tree = bondy_mst:put(Tree0, Key, Value),
    Root = bondy_mst:root(Tree),
    Grove = Grove0#?MODULE{tree = Tree},

    case Root0 == Root of
        true ->
            Grove;
        false ->
            Event = #event{
                from = Grove#?MODULE.node_id,
                root = Root,
                key = Key,
                value = Value
            },
            ok = (Grove#?MODULE.callback_mod):broadcast(Event),
            Grove
    end.



%% =============================================================================
%% API: ANTI-ENTROPY EXCHANGE PROTOCOL
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Call this function when your node receives a message or broadcast from a
%% peer.
%% @end
%% -----------------------------------------------------------------------------
handle(Grove0, #event{} = Event) ->
    Tree0 = Grove0#?MODULE.tree,
    Peer = Event#event.from,
    PeerRoot = Event#event.root,
    Key = Event#event.key,
    Value = Event#event.value,

    telemetry:execute(
        [bondy_mst, broadcast, recv],
        #{count => 1},
        #{peer => Peer}
    ),

    Root = bondy_mst:root(Tree0),

    case Root == PeerRoot of
        true ->
            ?LOG_INFO(#{
                message => "No merge required, trees in sync.",
                peer => Peer
            }),
            Grove0;

        false when Key == undefined andalso Value == undefined ->
            maybe_merge(Grove0, Peer, PeerRoot);

        false ->
            Tree1 = bondy_mst:put(Tree0, Key, Value),
            NewRoot = bondy_mst:root(Tree1),
            Grove1 = Grove0#?MODULE{tree = Tree1},

            case Root == NewRoot of
                true ->
                    maybe_merge(Grove1, Peer, PeerRoot);

                false ->
                    Merges0 = Grove1#?MODULE.merges,
                    Merges = maps:filter(
                        fun(_, V) -> V =/= NewRoot end,
                        Merges0
                    ),
                    Grove2 = Grove1#?MODULE{merges = Merges},
                    Grove3 = maybe_broadcast(Grove2, Event),

                    case NewRoot =/= PeerRoot of
                        true ->
                            maybe_merge(Grove3, Peer, PeerRoot);
                        false ->
                            ?LOG_INFO(#{
                                message => "Event merged, trees in sync.",
                                peer => Peer
                            }),
                            Grove3
                    end
            end
    end;

handle(Grove, #get{from = Peer, root = PeerRoot, set = Set}) ->
    %% We are being asked to produce a set of pages
    Tree = Grove#?MODULE.tree,
    Store = bondy_mst:store(Tree),

    %% We determine the hashes we don't have
    Missing = sets:filter(
        fun(Hash) -> not bondy_mst_store:has(Store, Hash) end,
        Set
    ),

    ok =
        case sets:is_empty(Missing) of
            true ->
                HashPages = sets:fold(
                    fun(Hash, Acc) ->
                        Page = bondy_mst_store:get(Store, Hash),
                        sets:add_element({Hash, Page}, Acc)
                    end,
                    sets:new([{version, 2}]),
                    Set
                ),
                Cmd = #put{from = Grove#?MODULE.node_id, set = HashPages},
                (Grove#?MODULE.callback_mod):send(Peer, Cmd);

            false ->
                Cmd = #missing{from = Grove#?MODULE.node_id},
                (Grove#?MODULE.callback_mod):send(Peer, Cmd)
        end,

    case PeerRoot == bondy_mst:root(Tree) of
        true ->
            Grove;

        false ->
            maybe_merge(Grove, Peer, PeerRoot)
    end;

handle(Grove, #put{from = Peer, set = Set}) ->
    case maps:is_key(Peer, Grove#?MODULE.merges) of
        true ->
            Tree = sets:fold(
                fun({_Hash, Page}, Acc0) ->
                    Acc = bondy_mst:put_page(Acc0, Page),
                    %% Call the callback merge function
                    ok = on_merge(Grove, Page),
                    Acc
                end,
                Grove#?MODULE.tree,
                Set
            ),
            Grove#?MODULE{tree = Tree};

        false ->
            Grove
    end;

handle(Grove, #missing{from = Peer}) ->
    case maps:take(Peer, Grove#?MODULE.merges) of
        {_, Merges} ->
            %% Abandon merge
            telemetry:execute(
                [bondy_mst, merge, abandoned],
                #{count => 1},
                #{peer => Peer}
            ),
            Grove#?MODULE{merges = Merges};

        error ->
            Grove
    end;

handle(_Grove, Cmd) ->
    error({unknown_event, Cmd}).


%% -----------------------------------------------------------------------------
%% @doc Triggers an exchange by sending the local tree's root to `Peer'.
%% The exchange might not proper when Peer has reached `max_merges'.
%% @end
%% -----------------------------------------------------------------------------
trigger(Grove, Peer) when is_atom(Peer) ->
    Event = #event{
        from = Grove#?MODULE.node_id,
        root = bondy_mst:root(Grove#?MODULE.tree),
        key = undefined,
        value = undefined
    },
    ok = (Grove#?MODULE.callback_mod):send(Peer, Event),
    Grove.


%% -----------------------------------------------------------------------------
%% @doc Extracts a key-value pair from the `event()`.
%% @end
%% -----------------------------------------------------------------------------
event_data(#event{key = Key, value = Value}) ->
    {Key, Value}.



%% =============================================================================
%% PRIVATE
%% =============================================================================

validate_callback_mod(Opts) ->
    CallbackMod = maps:get(callback_mod, Opts),

    is_atom(CallbackMod)
        orelse error({badarg, [{callback_mod, CallbackMod}]}),

    bondy_mst_utils:implements_behaviour(CallbackMod, ?MODULE)
        orelse error(
            iolib:format(
                "Expected ~p to implement behaviour ~p",
                [CallbackMod, ?MODULE]
            )
        ),
    CallbackMod.


on_merge(Grove, Page) ->
    try
        bondy_mst_utils:apply_lazy(
            Grove#?MODULE.callback_mod, on_merge, 1, [Page], fun() -> ok end
        )
    catch
        Class:Reason:Stacktrace ->
            ?LOG_ERROR(#{
                message => "Unexpected error while calling callback 'on_merge'",
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
        ok
    end.

%% @private
maybe_broadcast(Grove, undefined) ->
    %% The original paper schedules a broadcast, but we don't
    Grove;

maybe_broadcast(Grove, Event) ->
    Now = erlang:monotonic_time(),
    (Grove#?MODULE.callback_mod):broadcast(Event),
    Grove#?MODULE{last_broadcast_time = Now}.


%% @private
maybe_merge(#?MODULE{} = Grove, _, undefined) ->
    Grove;

maybe_merge(#?MODULE{} = Grove0, Peer, PeerRoot) ->
    Max = Grove0#?MODULE.max_merges,
    MaxSame = Grove0#?MODULE.max_same_merge,
    Merges = Grove0#?MODULE.merges,
    Count = count_merges(Merges, PeerRoot),

    case Count < MaxSame andalso map_size(Merges) < Max of
        true ->
            Grove = Grove0#?MODULE{merges = maps:put(Peer, PeerRoot, Merges)},
            merge(Grove, Peer);

        false ->
            ?LOG_INFO(#{
                message => "Skipping merge, limit reached", peer => Peer}
            ),
            Grove0
    end.


%% @private
count_merges(Merges, Root) ->
     maps:fold(
        fun
            (_, V, Acc) when V == Root ->
                Acc + 1;

            (_, _, Acc) ->
                Acc
        end,
        0,
        Merges
    ).


%% @private
merge(Grove, Peer) ->
    ?LOG_INFO(#{
        message => "Starting merge",
        peer => Peer
    }),
    Tree = Grove#?MODULE.tree,
    PeerRoot = maps:get(Peer, Grove#?MODULE.merges),

    MissingSet = bondy_mst:missing_set(Tree, PeerRoot),

    case sets:is_empty(MissingSet) of
        true ->
            ?LOG_INFO(#{
                message => "All pages locally available."
            }),
            %% All pages are locally available, so we perform the merge and
            %% update the local tree.
            do_merge(Grove, Peer, PeerRoot);

        false ->
            ?LOG_INFO(#{
                message => "Requesting missing pages from peer.",
                peer => Peer
            }),
            %% We still have missing pages, so we request them and keep the
            %% remote reference in a buffer until we receive those pages from
            %% the peer.
            Root = bondy_mst:root(Tree),
            Cmd = #get{
                from = Grove#?MODULE.node_id,
                root = Root,
                set = MissingSet
            },
            ok = (Grove#?MODULE.callback_mod):send(Peer, Cmd),
            Grove
    end.


do_merge(Grove0, Peer, PeerRoot) ->
    Tree0 = Grove0#?MODULE.tree,
    Root = bondy_mst:root(Tree0),

    Tree1 = bondy_mst:merge(Tree0, Tree0, PeerRoot),
    NewRoot = bondy_mst:root(Tree1),
    true = sets:is_empty(bondy_mst:missing_set(Tree1, NewRoot)),

    NewMerges = maps:filter(
        fun(_, V) -> V =/= PeerRoot andalso V =/= NewRoot end,
        Grove0#?MODULE.merges
    ),

    Grove1 = Grove0#?MODULE{
        tree = Tree1,
        merges = NewMerges
    },

    case Root =/= NewRoot of
        true ->
            %% NewEvents = bondy_mst:diff_to_list(Tree1, Tree1, Root),
            %% Entropy measurement here!
            Grove = maybe_gc(Grove1),
            ok =
                case NewRoot =/= PeerRoot of
                    true ->
                        Event = #event{
                            from = Grove#?MODULE.node_id,
                            root = NewRoot,
                            key = undefined,
                            value = undefined
                        },
                        (Grove#?MODULE.callback_mod):send(Peer, Event);

                    false ->
                        ok
                end;

        false ->
            Grove1
    end.


maybe_gc(Grove) ->
    %% @TODO.
    Grove.


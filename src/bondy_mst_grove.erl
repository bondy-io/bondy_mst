%% ===========================================================================
%%  bondy_mst.erl -
%%
%%  Copyright (c) 2023-2025 Leapsight. All rights reserved.
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

-module(bondy_mst_grove).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

?MODULEDOC("""
This module implements a sychronised group of Merkle Search Tree
replicas across a cluster.

This module implements a the logic for anti-entropy exchanges. The idea is
for the user to choose the right infrastructure e.g. using this module as
helper for a `gen_server` or `gen_statem`

Anti-entropy exchanges are performed in the background and without blocking
local operations. The underlying tree is not changed until all the remote
information necessary for the merge is obtained from a peer.

This module allows a set of remote trees that we want to merge with the local
tree to be kept in the state (`#?bondy_mst_grove.merges`) and all missing
pages are requested to the remote peers. Once all pages are locally
available, the merge operation is done without network communication and the
local tree is updated.

The number of active exchanges is limited by the option `max_merges`.

## Keys
Keys can be anything except for the reserved atom `undefined`.

## Network Operations
This module relies on a callback module provided by you that implements the
following callbacks:

* send/2
* broadcast/1
""").

-record(?MODULE, {
    node_id                         ::  node_id(),
    callback_mod                    ::  module(),
    tree                            ::  bondy_mst:t(),
    %% Set it to false if you are using a peer service that handles gossip
    %% When true, every gossip message received will be broadcasted again
    fwd_broadcast = false           ::  boolean(),
    %% The interval, measured in milliseconds, between two fwd_broadcasts.
    fwd_broadcast_interval = 1000       ::  integer(),
    last_fwd_broadcast_time             ::  integer() | undefined,
    %% The maximum number of concurrent merges.
    %% Bounds the size of 'merges'.
    max_merges = 6                  ::  pos_integer(),
    %% The maximum number of concurrent merges having the same root.
    %% This occurs when considering merging with N peers, as two or more of
    %% them might be in sync and hence having the same root hash.
    max_merges_per_root = 1         ::  pos_integer(),
    %% The max number of versions to keep i.e. the version will not be eligible
    %% for garbage collection.
    max_versions = 10               ::  pos_integer(),
    %% The time, measured in milliseconds, after which a version becomes
    %% eligible for garbage collection.
    version_ttl = timer:minutes(1)  ::  pos_integer(),
    %% This map's size is bounded by max_versions.
    history = #{}                   ::  #{epoch() => hash()},
    %% A map that tracks the ongoing merge exchanges. Its size is bounded by
    %% max_merges.
    merges = #{}                    ::  #{node_id() => hash()},
    %% A queue containing the latest gossiped roots from peers i.e. candidates
    %% for merges.
    %% It colesces base on peer and thus its size is naturally bounded to the
    %% number of peers in the grove.
    merge_backlog                   ::  bondy_mst_coalescing_queue:t(),
    %% A queue of delayed broadcasts.
    broadcast_backlog               ::  bondy_mst_coalescing_queue:t()
}).

%% The payload use for broadcasting changes to peers in the grove.
%% key and value can be 'undefined' when triggering an exchange.
-record(gossip, {
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
    map                     ::  #{hash() := bondy_mst_page:t()}
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
                                | {max_merges_per_root, pos_integer()}
                                | {callback_mod, module()}.
-type opts_map()            ::  #{
                                    store => bondy_mst_store:t(),
                                    merger => bondy_mst:merger(),
                                    comparator => bondy_mst:comparator(),
                                    max_merges => pos_integer(),
                                    max_merges_per_root => pos_integer(),
                                    callback_mod => module()
                                }.

-type gossip()              ::  #gossip{}.
-type get_cmd()             ::  #get{}.
-type missing_cmd()         ::  #missing{}.
-type put_cmd()             ::  #put{}.
-type message()             ::  gossip()
                                | get_cmd()
                                | put_cmd()
                                | missing_cmd().
-type gossip_data()         ::  #{
                                    from => node_id(),
                                    root => hash(),
                                    key => undefined,
                                    value => undefined
                                }
                                | #{
                                    from => node_id(),
                                    root => hash(),
                                    key => any(),
                                    value => any()
                                }.
-export_type([t/0]).
-export_type([gossip/0]).
-export_type([node_id/0]).
-export_type([message/0]).
-export_type([gossip_data/0]).

-export([broadcast_pending/1]).
-export([cancel_merge/2]).
-export([gc/1]).
-export([gc/2]).
-export([gossip_data/1]).
-export([gossip_message/2]).
-export([gossip_message/4]).
-export([handle/2]).
-export([is_stale/2]).
-export([merges/1]).
-export([new/2]).
-export([node_id/1]).
-export([put/3]).
-export([put/4]).
-export([root/1]).
-export([tree/1]).
-export([trigger/2]).



%% =============================================================================
%% CALLBACKS
%% =============================================================================


?DOC("""
Called when this module wants to send a message to a peer.
""").
-callback send(Peer :: node_id(), message()) -> ok | {error, any()}.


?DOC("""
Whenever this module wants to send a gossip message it will call
`Module:broadcast/1`.
The callback `Module` is responsible for sending the gossip to some random
peers, either by implementing or using a peer sampling service e.g.
`partisan_plumtree_broadcast`.
""").
-callback broadcast(Event :: gossip()) -> ok | {error, any()}.


?DOC("""
Called when a merge exchange has finished.
""").
-callback on_merge(Peer :: node()) -> ok.


-optional_callbacks([on_merge/1]).


%% =============================================================================
%% API
%% =============================================================================


?DOC("""
Cretes a new grove.

# Options
* `store => bondy_mst_store:t()`,
* `merger => bondy_mst:merger()`,
* `comparator => bondy_mst:comparator()`,
* `max_merges => pos_integer()`,
* `max_merges_per_root => pos_integer()`,
* `callback_mod => module()`
""").
-spec new(node_id(), opts()) -> Grove :: t() | no_return().

new(NodeId, Opts) when is_list(Opts) ->
    new(NodeId, maps:from_list(Opts));

new(NodeId, Opts0) when
(is_atom(NodeId) orelse is_binary(NodeId)) andalso is_map(Opts0) ->
    %% Configure the tree
    TreeOpts = maps:with([store, merger, comparator], Opts0),
    Tree = bondy_mst:new(TreeOpts),

    %% Configure the grove
    GroveOpts = maps:with(
        [callback_mod, max_merges, max_merges_per_root], Opts0
    ),
    CallbackMod = validate_callback_mod(GroveOpts),
    MaxMerges = maps:get(max_merges, GroveOpts, 6),
    MaxSameMerges = maps:get(max_merges_per_root, GroveOpts, 1),

    #?MODULE{
        node_id = NodeId,
        callback_mod = CallbackMod,
        tree = Tree,
        max_merges = MaxMerges,
        max_merges_per_root = MaxSameMerges,
        last_fwd_broadcast_time = erlang:monotonic_time(),
        merge_backlog = bondy_mst_coalescing_queue:new(),
        broadcast_backlog = bondy_mst_coalescing_queue:new()
    }.



?DOC("""
Returns the grove's local `node_id`.
""").
-spec node_id(t()) -> node_id().

node_id(#?MODULE{node_id = Val}) ->
    Val.


?DOC("""
Returns the grove's local tree.
""").
-spec tree(t()) -> bondy_mst:t().

tree(#?MODULE{tree = Tree}) ->
    Tree.


%% =============================================================================
%% API: TREE API
%% =============================================================================


?DOC("""
Returns the root of the grove's local tree.
""").
-spec root(t()) -> hash() | undefined.

root(#?MODULE{tree = Tree}) ->
    bondy_mst:root(Tree).


?DOC("""
Calls `bondy_mst:put/3` on the local tree.
If the operation changed the tree (previous and new root differ), it broadcasts
the change to the peers by calling the callback's `Module:broadcast/1` with a
`gossip()`.

When the gossip event is received by the peer it must handle it calling
`handle/2` on its local grove instance which will merge the value locally and
possibly trigger an exchange.
""").
-spec put(Grove0 :: t(), Key :: any(), Value :: any()) -> Grove1 :: t().

put(Grove, Key, Value) ->
    put(Grove, Key, Value, #{}).


?DOC("""
Calls `bondy_mst:put/3` on the local tree and if the operation changed
the tree (previous and new root differ), it broadcasts the change to the
peers by calling the callback's `Module:broadcast/1` with a `gossip()`.

When the event is received by the peer it must handle it calling `handle/2`
on its local grove instance.

## Options

* `broadcast => boolean` - If `false` it doesn`t broadcast the change to
peers. This means you will reply on peers performing periodic anti-entropy
exchanges to learn about the change. Default is `true`.
""").
-spec put(Grove0 :: t(), Key :: any(), Value :: any(), Opts :: key_value:t()) ->
    Grove1 :: t().

put(Grove0, Key, Value, Opts) ->
    Store = bondy_mst:store(Grove0#?MODULE.tree),

    Fun = fun() ->
        NodeId = Grove0#?MODULE.node_id,
        Tree0 = Grove0#?MODULE.tree,
        Root0 = bondy_mst:root(Tree0),

        %% We perform the put and update the grove state
        Tree = bondy_mst:put(Tree0, Key, Value),
        Grove1 = Grove0#?MODULE{tree = Tree},

        %% We obtain the new root
        Root = bondy_mst:root(Tree),

        %% We store the new root on the version history so that is not
        %% immediately elegible for garbage collection.
        %% The version will automatically elegible when its TTL is reached or
        %% when history has reached its maximum size.
        Grove = add_history(Grove1, Root),

        %% We conditionally broadcast
        case Root0 =/= Root andalso key_value:get(broadcast, Opts, true) of
            true ->
                Gossip = #gossip{
                    from = NodeId,
                    root = Root,
                    key = Key,
                    value = Value
                },
                broadcast(Grove, Gossip);

            _ ->
                Grove
        end

    end,
    bondy_mst_store:transaction(Store, Fun).


?DOC("""
Triggers a garbage collection.
Garbage collection removes all pages that are not descendants of either the
current root or the roots of versions in the history set whose TTL have not been
reached.
""").
-spec gc(t()) -> t().
gc(#?MODULE{} = Grove) ->
    gc(Grove, keep_roots(Grove)).


?DOC("""
Triggers a garbage collection overriding the versions to keep.
Garbage collection removes all pages that are not descendants of either the
current root or the roots in `KeepRoots`.
""").
-spec gc(t(), KeepRoots :: [binary()]) -> t();
        (t(), Epoch :: integer()) -> t().

gc(#?MODULE{} = Grove, Arg) when is_list(Arg) orelse is_integer(Arg) ->
    %% bondy_mst:gc will add the current root when is_list(Arg)
    Tree = bondy_mst:gc(Grove#?MODULE.tree, Arg),
    Grove#?MODULE{tree = Tree}.


?DOC("""
Returns the list of peers that have ongoing merges with this node.
""").
-spec merges(t()) -> [node()].

merges(#?MODULE{merges = Merges}) ->
    maps:keys(Merges).


?DOC("""
Cancels an ongoing merge (if it exists for peer `Peer`).

You should use a fault detector to cancel merges when a peer crashes.
""").
-spec cancel_merge(t(), node_id()) -> ok.

cancel_merge(#?MODULE{merges = Merges} = Grove, Peer) ->
    %% TODO This should cleanup all pages stored in the tree that have been
    %% synced but not merged yet. But carefull as pages might be used by
    %% multiple merges
    Grove#?MODULE{merges = maps:without([Peer], Merges)}.



%% =============================================================================
%% API: ANTI-ENTROPY EXCHANGE PROTOCOL
%% =============================================================================



?DOC("""
Broadcasts all gossip messages in the backlog.
""").
-spec broadcast_pending(t()) -> t().

broadcast_pending(#?MODULE{} = Grove) ->
    LastTime = Grove#?MODULE.last_fwd_broadcast_time,
    Pred = fun({_, Time}) -> LastTime < Time end,
    broadcast_pending(Grove, Pred).


?DOC("""
Triggers an exchange by sending the local tree's root to `Peer`.
The exchange might not occur if Peer has reached its `max_merges`.
""").
-spec trigger(t(), node_id()) -> ok.

trigger(#?MODULE{node_id = Peer}, Peer) ->
    ok;

trigger(#?MODULE{} = Grove, Peer) when is_atom(Peer) ->
    Event = #gossip{
        from = Grove#?MODULE.node_id,
        root = root(Grove),
        key = undefined,
        value = undefined
    },
    (Grove#?MODULE.callback_mod):send(Peer, Event).


?DOC("""
Returns a map with the contents of a `gossip()` message.
""").
-spec gossip_data(gossip()) -> gossip_data().

gossip_data(#gossip{} = Gossip) ->
    #{
        from => Gossip#gossip.from,
        root => Gossip#gossip.root,
        key => Gossip#gossip.key,
        value => Gossip#gossip.value
    }.


-spec gossip_message(node_id(), hash()) -> gossip().

gossip_message(Peer, Root) ->
    gossip_message(Peer, Root, undefined, undefined).


-spec gossip_message(node_id(), hash(), any(), any()) -> gossip().

gossip_message(Peer, Root, Key, Value) ->
    #gossip{
        from = Peer,
        root = Root,
        key = Key,
        value = Value
    }.


?DOC("""
Returns `true` if `PeerRoot` is not contained in the tree.
""").
-spec is_stale(t(), hash()) -> boolean().

is_stale(#?MODULE{} = Grove, PeerRoot) ->
    Tree = Grove#?MODULE.tree,
    Root = bondy_mst:root(Tree),

    case Root == PeerRoot of
        true ->
            false;

        false ->
            MissingSet = bondy_mst:missing_set(Tree, PeerRoot),
            not sets:is_empty(MissingSet)
    end.


?DOC("""
Call this function when your node receives a message or broadcast from a
peer.
""").
handle(Grove0, #gossip{} = Gossip) ->
    Tree0 = Grove0#?MODULE.tree,
    Peer = Gossip#gossip.from,
    PeerRoot = Gossip#gossip.root,
    Key = Gossip#gossip.key,
    Value = Gossip#gossip.value,

    telemetry:execute(
        [bondy_mst, broadcast, recv],
        #{count => 1},
        #{peer => Peer, pid => self()}
    ),

    Root = bondy_mst:root(Tree0),

    case Root == PeerRoot of
        true ->
            ?LOG_DEBUG(#{
                message => <<"No merged required, replicas in sync">>,
                root => encode_hash(PeerRoot),
                peer => Peer
            }),
            Grove0;

        false when Key == undefined andalso Value == undefined ->
            maybe_merge(Grove0, Peer, PeerRoot);

        false ->
            %% We insert the broadcasted change and get the new root
            Tree1 = bondy_mst:put(Tree0, Key, Value),
            Grove1 = Grove0#?MODULE{tree = Tree1},
            NewRoot = bondy_mst:root(Tree1),

            case Root =/= NewRoot of
                true ->
                    Grove2 = cancel_merges(Grove1, NewRoot),
                    Grove3 = maybe_broadcast(Grove2, Gossip),

                    case NewRoot =/= PeerRoot of
                        true ->
                            %% We have missing data
                            maybe_merge(Grove3, Peer, PeerRoot);

                        false ->
                            ?LOG_DEBUG(#{
                                message => <<
                                    "Broadcasted data merged, replicas in sync"
                                >>,
                                root => encode_hash(NewRoot),
                                peer => Peer
                            }),
                            Grove3
                    end;

                false ->
                    %% We have missing data
                    maybe_merge(Grove1, Peer, PeerRoot)
            end
    end;

handle(Grove, #get{from = Peer, root = PeerRoot, set = Set}) ->
    ?LOG_DEBUG(#{
        message => <<"Received GET message">>,
        peer => Peer,
        set_size => sets:size(Set)
    }),
    %% We are being asked to produce a set of pages
    Tree = Grove#?MODULE.tree,
    Store = bondy_mst:store(Tree),

    %% We determine the hashes we don`t have
    Missing = sets:filter(
        fun(Hash) -> not bondy_mst_store:has(Store, Hash) end,
        Set
    ),

    ok =
        case sets:is_empty(Missing) of
            true ->
                %% We collect the pages for the requested hashes and reply
                Map = sets:fold(
                    fun(Hash, Acc) ->
                        Page = bondy_mst_store:get(Store, Hash),
                        maps:put(Hash, Page, Acc)
                    end,
                    #{},
                    Set
                ),
                Msg = #put{from = Grove#?MODULE.node_id, map = Map},
                (Grove#?MODULE.callback_mod):send(Peer, Msg);

            false ->
                %% We don`t have all the pages, we reply a missing message
                Msg = #missing{from = Grove#?MODULE.node_id},
                (Grove#?MODULE.callback_mod):send(Peer, Msg)
        end,

    case PeerRoot == bondy_mst:root(Tree) of
        true ->
            Grove;

        false ->
            maybe_merge(Grove, Peer, PeerRoot)
    end;

handle(Grove, #put{from = Peer, map = Map}) ->
    case maps:is_key(Peer, Grove#?MODULE.merges) of
        true ->
            ?LOG_DEBUG(#{
                message => <<"Received peer data">>,
                peer => Peer,
                payload_size => maps:size(Map)
            }),
            Tree = maps:fold(
                fun(Hash0, Page, Acc0) ->
                    {Hash1, Acc} = bondy_mst:put_page(Acc0, Page),

                    %% Post-condition: Input and output hash should be the same
                    %% A difference might occur when the peer runs a different
                    %% implementation or when is using a different hashing
                    %% algorithm, in which case we fail.
                    Hash0 == Hash1
                        orelse error({inconsistency, Hash0, Page, Hash1}),

                    Acc
                end,
                Grove#?MODULE.tree,
                Map
            ),
            merge(Grove#?MODULE{tree = Tree}, Peer);

        false ->
            ?LOG_DEBUG(#{
                message => <<
                    "Ignored data from peer. Peer is not in merge set."
                >>,
                peer => Peer,
                payload_size => maps:size(Map)
            }),
            %% REVIEW: Will this happen when a previous merge has been evicted
            %% from the merge buffer?
            Grove
    end;

handle(Grove, #missing{from = Peer}) ->
    case maps:take(Peer, Grove#?MODULE.merges) of
        {_, Merges} ->
            %% Abandon merge
            telemetry:execute(
                [bondy_mst, merge, abandoned],
                #{count => 1},
                #{peer => Peer, pid => self()}
            ),
            Grove#?MODULE{merges = Merges};

        error ->
            Grove
    end;

handle(_Grove, Msg) ->
    error({unknown_event, Msg}).




%% =============================================================================
%% PRIVATE
%% =============================================================================


%% @private
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


%% @private
-spec maybe_broadcast(t(), gossip()) -> t().

maybe_broadcast(#?MODULE{fwd_broadcast = false} = Grove, _) ->
    Grove;

maybe_broadcast(Grove0, #gossip{key = undefined, value = undefined} = Gossip) ->
    Now = erlang:monotonic_time(),
    Elapsed = elapsed(Now, Grove0#?MODULE.last_fwd_broadcast_time),

    %% We make sure we broadcast pending gossip messages first
    Grove = broadcast_pending(Grove0),

    case Elapsed >= Grove#?MODULE.fwd_broadcast_interval of
        true ->
            broadcast(Grove, Gossip);

        false ->
            %% Delay broadcast, coalescing by Peer
            Backlog = bondy_mst_coalescing_queue:in(
                Grove#?MODULE.broadcast_backlog,
                Gossip#gossip.from,
                {Gossip, Now}
            ),
            Grove#?MODULE{broadcast_backlog = Backlog}
    end;

maybe_broadcast(Grove0, Gossip) ->
    %% We make sure we broadcast pending gossip messages first
    Grove = broadcast_pending(Grove0),
    broadcast(Grove, Gossip).


%% @private
broadcast(Grove0, Gossip) ->
    case (Grove0#?MODULE.callback_mod):broadcast(Gossip) of
        ok ->
            Grove0#?MODULE{last_fwd_broadcast_time = erlang:monotonic_time()};

        {error, Reason} ->
            ?LOG_ERROR(#{
                message => <<"Error while broadcasting gossip message">>,
                data => Gossip,
                reason => Reason
            }),
            Grove0
    end.


%% private
broadcast_pending(#?MODULE{} = Grove, Pred) when is_function(Pred, 1) ->
    B0 = Grove#?MODULE.broadcast_backlog,

    case bondy_mst_coalescing_queue:out_when(B0, Pred) of
        {empty, B0} ->
            Grove;

        {{value, {Gossip, _Time}}, B1} ->
            _ = broadcast(Grove, Gossip),
            broadcast_pending(Grove#?MODULE{broadcast_backlog = B1}, Pred)
    end.


%% @private
-spec maybe_merge(t(), node_id(), hash() | undefined) -> t().

maybe_merge(#?MODULE{} = Grove, Peer, undefined) ->
    %% Peer is empty, so we trigger an exchange in the other direction
    ok = trigger(Grove, Peer),
    Grove;

maybe_merge(#?MODULE{} = Grove0, Peer, PeerRoot) ->
    Max = Grove0#?MODULE.max_merges,
    MaxSame = Grove0#?MODULE.max_merges_per_root,
    Merges = Grove0#?MODULE.merges,
    Same = count_same_merges(Merges, PeerRoot),
    Size = map_size(Merges),

    case Same < MaxSame andalso Size < Max of
        true ->
            Grove = Grove0#?MODULE{merges = maps:put(Peer, PeerRoot, Merges)},
            ?LOG_INFO(#{
                message => <<"Starting merge with peer.">>,
                peer => Peer,
                merge_count => Size + 1,
                max_merges => {Max, MaxSame}
            }),
            merge(Grove, Peer);

        false ->
            ?LOG_DEBUG(#{
                message => <<
                    "Skipping merge, merge concurrency limits reached."
                >>,
                peer => Peer,
                merge_count => Size + 1,
                max_merges => {Max, MaxSame}
            }),
            Grove0
    end.


%% @private
count_same_merges(Merges, Root) ->
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
%% This is called directly only when receiving a PUT. Otherwise it is called
%% by maybe_merge/2.
merge(Grove0, Peer) ->
    Tree = Grove0#?MODULE.tree,

    PeerRoot = maps:get(Peer, Grove0#?MODULE.merges),
    %% Pre-condition
    true = PeerRoot =/= undefined,

    MissingSet = bondy_mst:missing_set(Tree, PeerRoot),

    case sets:is_empty(MissingSet) of
        true ->
            %% All pages are locally available, so we perform the merge and
            %% update the local tree.
            ?LOG_DEBUG(#{
                message => <<
                    "All peer pages are now locally available. "
                    "Finishing merge."
                >>,
                peer => Peer
            }),
            Grove = do_merge(Grove0, Peer, PeerRoot),
            ok = on_merge(Grove, Peer),
            Grove;

        false ->
            %% We still have missing pages, so we request them and keep the
            %% remote reference in a buffer until we receive those pages from
            %% peer.
            ?LOG_DEBUG(#{
                message => <<"Requesting missing pages from peer.">>,
                missing_count => sets:size(MissingSet),
                peer => Peer
            }),
            Root = bondy_mst:root(Tree),
            Cmd = #get{
                from = Grove0#?MODULE.node_id,
                root = Root,
                set = MissingSet
            },
            ok = (Grove0#?MODULE.callback_mod):send(Peer, Cmd),
            Grove0
    end.


%% @private
do_merge(Grove0, Peer, PeerRoot) ->
    Tree0 = Grove0#?MODULE.tree,
    Root = bondy_mst:root(Tree0),

    Tree1 = bondy_mst:merge(Tree0, Tree0, PeerRoot),
    NewRoot = bondy_mst:root(Tree1),


    %% Post-condition
    true = sets:is_empty(bondy_mst:missing_set(Tree1, NewRoot)),

    NewMerges = maps:filter(
        fun(_, V) -> V =/= PeerRoot andalso V =/= NewRoot end,
        Grove0#?MODULE.merges
    ),

    Grove1 = Grove0#?MODULE{tree = Tree1, merges = NewMerges},

    case Root =/= NewRoot of
        true ->
            %% We store the new root on the version history so that is not
            %% immediately elegible for garbage collection.
            %% The version will automatically elegible when its TTL is reached
            %% or when history has reached its maximum size.
            Grove2 = add_history(Grove1, NewRoot),
            Grove = maybe_gc(Grove2),

            case NewRoot =/= PeerRoot of
                true ->
                    Event = #gossip{
                        from = Grove#?MODULE.node_id,
                        root = NewRoot,
                        key = undefined,
                        value = undefined
                    },
                    (Grove#?MODULE.callback_mod):send(Peer, Event);

                false ->
                    ok
            end,
            Grove;

        false ->
            Grove1
    end.


%% @private
on_merge(Grove0, Peer) ->
    ?LOG_INFO(#{
        message => <<"Finished merge with peer.">>,
        peer => Peer
    }),

    Grove = broadcast_pending(Grove0),

    try
        bondy_mst_utils:apply_lazy(
            Grove#?MODULE.callback_mod,
            on_merge,
            1,
            [Peer],
            fun() -> ok end
        )
    catch
        Class:Reason:Stacktrace ->
            ?LOG_ERROR(#{
                message => "Error while evaluating callback on_merge/1",
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            })
    end.


%% @private
maybe_gc(Grove) ->
    %% We preserve all roots being merged and all history roots
    gc(Grove, keep_roots(Grove)).


%% @private
encode_hash(undefined) -> undefined;
encode_hash(Bin) -> binary:encode_hex(Bin).


%% @private
cancel_merges(Grove, NewRoot) ->
    %% We remove any ongoing merges matching the merged NewRoot.
    Merges = maps:filter(fun(_, V) -> V =/= NewRoot end, Grove#?MODULE.merges),

    %% We remove any pending merges matching the merged NewRoot.
    Backlog = bondy_mst_coalescing_queue:filter(
        Grove#?MODULE.merge_backlog,
        fun(_, V) -> V =/= NewRoot end
    ),

    Grove#?MODULE{merges = Merges, merge_backlog = Backlog}.


%% @private
add_history(#?MODULE{} = Grove, Root) ->
    Epoch = erlang:monotonic_time(),
    TTL = Grove#?MODULE.version_ttl,

    %% We purge expired versions
    H1 = maps:filter(
        fun(Epoch0, _Root) -> elapsed(Epoch, Epoch0) =< TTL end,
        Grove#?MODULE.history
    ),
    %% We add the new version
    H = maps:put(Epoch, Root, H1),

    Grove#?MODULE{history = H}.


%% @private
keep_roots(#?MODULE{} = Grove) ->
    MergeRoots = maps:values(Grove#?MODULE.merges),
    HistoryRoots = maps:values(Grove#?MODULE.history),
    MergeRoots ++ HistoryRoots.


%% @private
elapsed(Start, Stop) ->
    elapsed(Start, Stop, millisecond).

%% @private
elapsed(Start, Stop, Unit) ->
    erlang:convert_time_unit(Start - Stop, native, Unit).




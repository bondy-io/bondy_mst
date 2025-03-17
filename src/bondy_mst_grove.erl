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
%% This module implements a sychronised group of Merkle Search Tree
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
%% This module relies on a callback module provided by you that implements the
%% following callbacks:
%%
%% * send/2
%% * broadcast/1
%% @end
%% -----------------------------------------------------------------------------

-module(bondy_mst_grove).

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

-record(?MODULE, {
    node_id                 ::  node_id(),
    callback_mod            ::  module(),
    tree                    ::  bondy_mst:t(),
    last_broadcast_time     ::  integer() | undefined,
    max_merges = 6          ::  pos_integer(),
    max_same_merge = 1      ::  pos_integer(),
    merges = #{}            ::  #{node_id() => hash()}
}).

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

-type gossip()              ::  #gossip{}.
-type get_cmd()             ::  #get{}.
-type missing_cmd()         ::  #missing{}.
-type put_cmd()             ::  #put{}.
-type message()             ::  gossip()
                                | get_cmd()
                                | put_cmd()
                                | missing_cmd().

-export_type([t/0]).
-export_type([gossip/0]).
-export_type([node_id/0]).
-export_type([message/0]).

-export([cancel_merge/2]).
-export([gc/2]).
-export([gossip_data/1]).
-export([handle/2]).
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



%% Called when this module wants to send a message to a peer.
-callback send(Peer :: node_id(), message()) -> ok | {error, any()}.


%% Whenever this module wants to send a gossip message it will call
%% `Module:broadcast/1'.
%% The callback `Module' is responsible for sending the gossip to some random
%% peers, either by implementing or using a peer sampling service e.g.
%% `partisan_plumtree_broadcast'.
-callback broadcast(Event :: gossip()) -> ok | {error, any()}.


%% Called when a merge exchange has finished
-callback on_merge(Peer :: node()) -> ok.


-optional_callbacks([on_merge/1]).


%% =============================================================================
%% API
%% =============================================================================


%% -----------------------------------------------------------------------------
%% @doc Cretes a new grove.
%% @end
%% -----------------------------------------------------------------------------
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
%% @doc Returns the grove's local node_id
%% @end
%% -----------------------------------------------------------------------------
-spec node_id(t()) -> node_id().

node_id(#?MODULE{node_id = Val}) ->
    Val.


%% -----------------------------------------------------------------------------
%% @doc Returns the grove's local tree.
%% @end
%% -----------------------------------------------------------------------------
-spec tree(t()) -> bondy_mst:t().

tree(#?MODULE{tree = Tree}) ->
    Tree.


%% =============================================================================
%% API: TREE API
%% =============================================================================


%% -----------------------------------------------------------------------------
%% @doc Returns the root of the grove's local tree.
%% @end
%% -----------------------------------------------------------------------------
-spec root(t()) -> hash() | undefined.

root(#?MODULE{tree = Tree}) ->
    bondy_mst:root(Tree).


%% -----------------------------------------------------------------------------
%% @doc Calls `bondy_mst:put/3' on the local tree and if the operation changed
%% the tree (previous and new root differ), it broadcasts the change to the
%% peers by calling the callback's `Module:broadcast/1' with a `gossip()'.
%%
%% When the event is received by the peer it must handle it calling `handle/2'
%% on its local grove instance.
%% @end
%% -----------------------------------------------------------------------------
-spec put(Grove0 :: t(), Key :: any(), Value :: any()) -> Grove1 :: t().

put(Grove, Key, Value) ->
    put(Grove, Key, Value, #{}).


%% -----------------------------------------------------------------------------
%% @doc Calls `bondy_mst:put/3' on the local tree and if the operation changed
%% the tree (previous and new root differ), it broadcasts the change to the
%% peers by calling the callback's `Module:broadcast/1' with a `gossip()'.
%%
%% When the event is received by the peer it must handle it calling `handle/2'
%% on its local grove instance.
%%
%% ## Options
%%
%% * `broadcast => boolean` - If `false` it doesn't broadcast the change to
%% peers. This means you will reply on peers performing periodic anti-entropy
%% exchanges to learn about the change. Default is `true`.
%% @end
%% -----------------------------------------------------------------------------
-spec put(Grove0 :: t(), Key :: any(), Value :: any(), Opts :: key_value:t()) ->
    Grove1 :: t().

put(Grove, Key, Value, Opts) ->
    Store = bondy_mst:store(Grove#?MODULE.tree),
    NodeId = Grove#?MODULE.node_id,
    CBMod = Grove#?MODULE.callback_mod,
    Tree0 = Grove#?MODULE.tree,
    BCast = key_value:get(broadcast, Opts, true),

    Fun = fun() ->
        Root0 = bondy_mst:root(Tree0),
        Tree = bondy_mst:put(Tree0, Key, Value),
        Root = bondy_mst:root(Tree),

        case Root0 =/= Root of
            true when BCast == true ->
                Msg = #gossip{
                    from = NodeId,
                    root = Root,
                    key = Key,
                    value = Value
                },
                ok = CBMod:broadcast(Msg),
                Tree;

            _ ->
                Tree
        end
    end,
    Tree = bondy_mst_store:transaction(Store, Fun),
    Grove#?MODULE{tree = Tree}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec gc(t(), KeepRoots :: [binary()]) -> t();
        (t(), Epoch :: integer()) -> t().

gc(Grove, Arg) ->
    Tree = bondy_mst:gc(Grove#?MODULE.tree, Arg),
    Grove#?MODULE{tree = Tree}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec merges(t()) -> [node()].

merges(#?MODULE{merges = Merges}) ->
    maps:keys(Merges).


%% -----------------------------------------------------------------------------
%% @doc Cancels and ongoing merge (if it exists for peer `Peer`).
%%
%% You should use a fault detector to cancel merges when a peer crashes.
%% @end
%% -----------------------------------------------------------------------------
-spec cancel_merge(t(), node_id()) -> ok.

cancel_merge(#?MODULE{merges = Merges} = Grove, Peer) ->
    %% TODO This should cleanup all pages stored in the tree that have been
    %% synced but not merged yet. But carefull as pages might be used by
    %% multiple merges
    Grove#?MODULE{merges = maps:without([Peer], Merges)}.



%% =============================================================================
%% API: ANTI-ENTROPY EXCHANGE PROTOCOL
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Triggers an exchange by sending the local tree's root to `Peer'.
%% The exchange might not occur if Peer has reached its `max_merges'.
%% @end
%% -----------------------------------------------------------------------------
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


%% -----------------------------------------------------------------------------
%% @doc Extracts a key-value pair from the `gossip()' message.
%% @end
%% -----------------------------------------------------------------------------
gossip_data(#gossip{key = Key, value = Value}) ->
    {Key, Value}.


%% -----------------------------------------------------------------------------
%% @doc Call this function when your node receives a message or broadcast from a
%% peer.
%% @end
%% -----------------------------------------------------------------------------
handle(Grove0, #gossip{} = Event) ->
    Tree0 = Grove0#?MODULE.tree,
    Peer = Event#gossip.from,
    PeerRoot = Event#gossip.root,
    Key = Event#gossip.key,
    Value = Event#gossip.value,

    telemetry:execute(
        [bondy_mst, broadcast, recv],
        #{count => 1},
        #{peer => Peer, pid => self()}
    ),

    Root = bondy_mst:root(Tree0),

    case Root == PeerRoot of
        true ->
            ?LOG_INFO(#{
                message => "Replica in sync",
                root => encode_hash(PeerRoot),
                peer => Peer
            }),
            Grove0;

        false when Key == undefined andalso Value == undefined ->
            %% Full merge
            maybe_merge(Grove0, Peer, PeerRoot);

        false ->
            %% We insert the broadcasted change and get the new root
            Tree1 = bondy_mst:put(Tree0, Key, Value),
            Grove1 = Grove0#?MODULE{tree = Tree1},
            NewRoot = bondy_mst:root(Tree1),

            case Root =/= NewRoot of
                true ->
                    %% We remove from pending merges
                    Merges0 = Grove1#?MODULE.merges,
                    Merges = maps:filter(
                        fun(_, V) -> V =/= NewRoot end,
                        Merges0
                    ),
                    Grove2 = Grove1#?MODULE{merges = Merges},
                    Grove3 = maybe_broadcast(Grove2, Event),

                    case NewRoot =/= PeerRoot of
                        true ->
                            %% We have missing data
                            maybe_merge(Grove3, Peer, PeerRoot);

                        false ->
                            ?LOG_INFO(#{
                                message =>
                                    "Broadcasted data merged, replicas in sync",
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
    ?LOG_INFO(#{
        message => "Received GET message",
        peer => Peer,
        set_size => sets:size(Set)
    }),
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
                %% We don't have all the pages, we reply a missing message
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
            ?LOG_INFO(#{
                message => "Received peer data during sync",
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
            ?LOG_INFO(#{
                message => "Ignored PUT message, peer is not in merge buffer",
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
-spec maybe_broadcast(t(), gossip() | undefined) -> t().

maybe_broadcast(Grove, undefined) ->
    %% The original paper schedules a broadcast, but we don't
    Grove;

maybe_broadcast(Grove, Gossip) ->
    Now = erlang:monotonic_time(),
    case (Grove#?MODULE.callback_mod):broadcast(Gossip) of
        ok ->
            Grove#?MODULE{last_broadcast_time = Now};

        {error, Reason} ->
            ?LOG_ERROR(#{
                message => "Error while broadcasting gossip message",
                reason => Reason
            }),
            Grove
    end.


%% @private
-spec maybe_merge(t(), node_id(), hash() | undefined) -> t().

maybe_merge(#?MODULE{} = Grove, Peer, undefined) ->
    %% Peer is empty, so we trigger an exchange in the other direction
    ok = trigger(Grove, Peer),
    Grove;

maybe_merge(#?MODULE{} = Grove0, Peer, PeerRoot) ->
    Max = Grove0#?MODULE.max_merges,
    MaxSame = Grove0#?MODULE.max_same_merge,
    Merges = Grove0#?MODULE.merges,
    Same = count_same_merges(Merges, PeerRoot),

    case Same < MaxSame andalso map_size(Merges) < Max of
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
merge(Grove0, Peer) ->
    ?LOG_INFO(#{
        message => "Starting merge",
        peer => Peer
    }),

    Tree = Grove0#?MODULE.tree,

    PeerRoot = maps:get(Peer, Grove0#?MODULE.merges),
    %% Pre-condition
    true = PeerRoot =/= undefined,

    MissingSet = bondy_mst:missing_set(Tree, PeerRoot),

    case sets:is_empty(MissingSet) of
        true ->
            %% All pages are locally available, so we perform the merge and
            %% update the local tree.
            ?LOG_INFO(#{
                message => "All pages locally available",
                peer => Peer
            }),
            Grove = do_merge(Grove0, Peer, PeerRoot),
            ok = on_merge(Grove, Peer),
            Grove;

        false ->
            %% We still have missing pages, so we request them and keep the
            %% remote reference in a buffer until we receive those pages from
            %% peer.
            ?LOG_INFO(#{
                message => "Requesting missing pages from peer",
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
            Grove = maybe_gc(Grove1),
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
on_merge(Grove, Peer) ->
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
    %% @TODO.
    Grove.


%% @private
encode_hash(undefined) -> undefined;
encode_hash(Bin) -> binary:encode_hex(Bin).


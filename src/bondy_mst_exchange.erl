-module(bondy_mst_exchange).
-moduledoc"""
This module implements the logic for anti-entropy exchanges. The idea is for the user to choose the right infrastructure e.g. using this module as helper for a `gen_server` or `gen_statem`.

Exchanges are performed in the background and without blocking local operations. The tree is not changed until all the remote information necessary for the merge is obtained from a peer.

The algorithm allows a set of remote trees that we want to merge with the local tree to be kept in the state (`#state.merges`) and all missing pages are requested to the remote peers. Once all blocks are locally available, the merge operation is done without network communication and the local tree is updated.

The number of peers is limited by the option `max_merges`.
""".

-include_lib("kernel/include/logger.hrl").
-include("bondy_mst.hrl").

-record(state, {
    node_id                 ::  node_id(),
    callback_mod            ::  module(),
    tree                    ::  bondy_mst:t(),
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


%% Normally a node() but we allow a binary for simulation purposes
-type node_id()             ::  node() | binary().
-type event()               ::  #event{}.
-type get_cmd()             ::  #get{}.
-type missing_cmd()         ::  #missing{}.
-type put_cmd()             ::  #put{}.
-type message()             ::  event()
                                | get_cmd()
                                | put_cmd()
                                | missing_cmd().


-export_type([event/0]).
-export_type([node_id/0]).
-export_type([message/0]).

-export([init/3]).
-export([handle/2]).



%% =============================================================================
%% CALLBACKS
%% =============================================================================

-doc """
""".
-callback send(Peer :: node_id(), message()) -> ok | {error, any()}.

-doc """
Whenever this module wants to gossip an event it will call `Module:broadcast/1`.
The callback `Module` is responsible for sending the event to some random peers,
either by implementing or using a peer sampling service e.g.
`partisan_plumtree_broadcast`.
""".
-callback broadcast(event()) -> ok | {error, any()}.


-doc """
Called after merging a tree page from a remote tree.
    The implemeneter can use this to extract the entries and merge the data in a main store when the tree is used only for anti-entropy.
""".
-callback on_merge(bondy_mst_page:t()) -> ok.


%% =============================================================================
%% API
%% =============================================================================

-doc """
""".
init(NodeId, Tree, Opts) when is_list(Opts) ->
    init(NodeId, Tree, maps:from_list(Opts));

init(NodeId, Tree, Opts0) when is_map(Opts0) ->
    DefaultOpts = #{
        max_merges => 6,
        max_same_merge => 1
    },
    Opts = maps:merge(DefaultOpts, Opts0),

    CallbackMod = maps:get(callback_mod, Opts0),
    is_atom(CallbackMod) orelse error({badarg, Opts0}),
    bondy_mst_utils:implements_behaviour(CallbackMod, ?MODULE)
        orelse error(
            iolib:format(
                "Expected ~p to implement behaviour ~p",
                [CallbackMod, ?MODULE]
            )
        ),

    MaxMerges = maps:get(max_merges, Opts),
    MaxSameMerges = maps:get(max_same_merge, Opts),

    State = #state{
        node_id = NodeId,
        callback_mod = CallbackMod,
        tree = Tree,
        max_merges = MaxMerges,
        max_same_merge = MaxSameMerges
    },
    {ok, State}.


-doc """
Call this function when your node receives a message or broadcast from a peer.
""".
handle(#event{} = Event, State0) ->

    Tree0 = State0#state.tree,
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
            %% We are in sync with peer
            State0;

        false when Key == undefined andalso Value == undefined ->
            maybe_merge(State0, Peer, PeerRoot);

        false ->
            Tree1 = bondy_mst:insert(Tree0, Key, Value),
            NewRoot = bondy_mst:root(Tree1),
            State1 = State0#state{tree = Tree1},

            case Root == NewRoot of
                true ->
                    maybe_merge(State1, Peer, PeerRoot);

                false ->
                    Merges0 = State1#state.merges,
                    Merges = maps:filter(
                        fun(_, V) -> V =/= NewRoot end,
                        Merges0
                    ),
                    State2 = State1#state{merges = Merges},
                    State3 = maybe_broadcast(State2, Event),

                    case NewRoot =/= PeerRoot of
                        true ->
                            maybe_merge(State3, Peer, PeerRoot);
                        false ->
                            State3
                    end
            end
    end;

handle(#get{from = Peer, root = PeerRoot, set = Set}, State) ->
    %% We are being asked to produce a set of pages
    Tree = State#state.tree,
    Store = bondy_mst:store(Tree),

    %% We determine the hashes we don't have
    Missing = sets:filter(
        fun(Hash) -> bondy_mst_store:has(Store, Hash) end,
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
                Cmd = #put{from = State#state.node_id, set = HashPages},
                (State#state.callback_mod):send(Peer, Cmd);

            false ->
                Cmd = #missing{from = State#state.node_id},
                (State#state.callback_mod):send(Peer, Cmd)
        end,

    case PeerRoot == bondy_mst:root(Tree) of
        true ->
            State;

        false ->
            maybe_merge(State, Peer, PeerRoot)
    end;

handle(#put{from = Peer, set = Set}, State) ->
    Mod = State#state.callback_mod,

    case maps:is_key(Peer, State#state.merges) of
        true ->
            Tree = sets:fold(
                fun({Hash, Page}, Acc0) ->
                    {Hash, Acc} = bondy_mst:put(Acc0, Page),
                    %% Call the callback merge function
                    ok = Mod:on_merge(Page),
                    Acc
                end,
                State#state.tree,
                Set
            ),
            State#state{tree = Tree};

        false ->
            State
    end;

handle(#missing{from = Peer}, State) ->
    case maps:take(Peer, State#state.merges) of
        {_, Merges} ->
            %% Abandon merge
            telemetry:execute(
                [bondy_mst, merge, abandoned],
                #{count => 1},
                #{peer => Peer}
            ),
            State#state{merges = Merges};

        error ->
            State
    end;

handle(Cmd, _State) ->
    error({unknown_event, Cmd}).




%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
maybe_broadcast(State, undefined) ->
    %% The original paper schedules a broadcast, but we don't
    State;

maybe_broadcast(State, Event) ->
    Now = erlang:monotonic_time(),
    (State#state.callback_mod):broadcast(Event),
    State#state{last_broadcast_time = Now}.


%% @private
maybe_merge(#state{} = State, _, undefined) ->
    State;

maybe_merge(#state{} = State0, Peer, PeerRoot) ->
    Max = State0#state.max_merges,
    MaxSame = State0#state.max_same_merge,
    Merges = State0#state.merges,
    Count = count_merges(Merges, PeerRoot),

    case Count < MaxSame andalso map_size(Merges) < Max of
        true ->
            State = State0#state{merges = maps:put(Peer, PeerRoot, Merges)},
            merge(State, Peer);

        false ->
            State0
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
merge(State, Peer) ->
    Tree = State#state.tree,
    PeerRoot = maps:get(Peer, State#state.merges),

    MissingSet = bondy_mst_store:missing_set(Tree, PeerRoot),

    case sets:is_empty(MissingSet) of
        true ->
            %% All pages are locally available, so we perform the merge and
            %% update the local tree.
            do_merge(State, Peer, PeerRoot);

        false ->
            %% We still have missing pages, so we request them and keep the
            %% remote reference in a buffer until we receive those pages from
            %% the peer.
            Root = bondy_mst:root(Tree),
            Cmd = #get{
                from = State#state.node_id,
                root = Root,
                set = MissingSet
            },
            ok = (State#state.callback_mod):send(Peer, Cmd),
            State
    end.


do_merge(State0, Peer, PeerRoot) ->
    Tree0 = State0#state.tree,
    Root = bondy_mst:root(Tree0),

    Tree1 = bondy_mst:merge(Tree0, Tree0, PeerRoot),
    NewRoot = bondy_mst:root(Tree1),
    true = sets:is_empty(bondy_mst_store:missing_set(Tree1, NewRoot)),

    NewMerges = maps:filter(
        fun(_, V) -> V =/= PeerRoot andalso V =/= NewRoot end,
        State0#state.merges
    ),

    State1 = State0#state{
        tree = Tree1,
        merges = NewMerges
    },

    case Root =/= NewRoot of
        true ->
            %% NewEvents = bondy_mst:diff_to_list(Tree1, Tree1, Root),
            %% Entropy measurement here!
            State = maybe_gc(State1),
            ok =
                case NewRoot =/= PeerRoot of
                    true ->
                        Event = #event{
                            from = State#state.node_id,
                            root = NewRoot,
                            key = undefined,
                            value = undefined
                        },
                        (State#state.callback_mod):send(Peer, Event);

                    false ->
                        ok
                end;

        false ->
            State1
    end.


maybe_gc(State) ->
    State.


-module(bondy_mst_test_grove).

-behaviour(bondy_mst_grove).
-behaviour(gen_server).

-include_lib("common_test/include/ct.hrl").

%% API
-export([peers/0]).
-export([start_all/1]).
-export([start/2]).

%% BONDY_MSRT_GROVE CALLBACKS
-export([send/2]).
-export([broadcast/1]).


%% GEN_SERVER CALLBACKS
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

-type t() :: bondy_mst_groove:t().
-type opts_map()            ::  #{
                                    name => atom(),
                                    store_type => module(),
                                    merger => bondy_mst:merger(),
                                    comparator => bondy_mst:comparator(),
                                    max_merges => pos_integer(),
                                    max_same_merge => pos_integer()
                                }.

%% =============================================================================
%% API
%% =============================================================================

peers() ->
    [
        peer1,
        peer2,
        peer3
    ].


start_all(Opts) ->
    start(Opts, peers()).


start(#{store_type := StoreType, name := Name0} = Opts, Peers) ->
    Started = [
        begin
            Name = <<Name0/binary, "-", (atom_to_binary(NodeId))/binary>>,
            Store = bondy_mst_store:open(
                StoreType, [{name, Name}]
            ),
            {ok, _} = start_link(NodeId, Opts#{store => Store}),
            NodeId
        end
        || NodeId <- Peers
    ],
    {ok, Started}.


start_link(NodeId, Opts) when is_atom(NodeId) ->
    ServerOpts = [
        {spawn_opt, [{message_queue_data, off_heap}]}
    ],
    gen_server:start_link({local, NodeId}, ?MODULE, [NodeId, Opts], ServerOpts).



%% =============================================================================
%% BONDY_MST_GROVE CALLBACKS
%% =============================================================================



send(Peer, Message) ->
    gen_server:cast(Peer, {grove_message, Message}).


broadcast(Gossip) ->
    Myself = element(2, Gossip),
    Peers = peers() -- [Myself],

    ct:pal("Broadcasting event ~p to peer ~p", [Gossip, Peers]),
    _ = [send(Peer, Gossip) || Peer <- Peers],
    ok.



%% =============================================================================
%% GEN_SERVER BEHAVIOR CALLBACKS
%% ============================================================================


init([NodeId, Opts0]) ->
    %% Trap exists otherwise terminate/1 won't be called when shutdown by
    %% supervisor.
    erlang:process_flag(trap_exit, true),

    Defaults = #{
        callback_mod => ?MODULE,
        max_merges => 3,
        max_same_merge => 1
    },

    Opts = maps:merge(Defaults, Opts0),

    %% We create an ets-based MST bound to this process.
    %% The ets table will be garbage collected if this process terminates.
    Grove = bondy_mst_grove:new(NodeId, Opts),
    {ok, Grove}.

handle_call(ping, _From, Grove) ->
    {reply, pong, Grove};

handle_call(root, _From, Grove) ->
    ct:pal("handling root"),
    Reply = bondy_mst_grove:root(Grove),
    {reply, Reply, Grove};

handle_call({get, Key}, _From, Grove) ->
    ct:pal("handling get key: ~p", [Key]),
    Reply = bondy_mst:get(bondy_mst_grove:tree(Grove), Key),
    {reply, Reply, Grove};

handle_call(list, _From, Grove) ->
    ct:pal("handling list"),
    Reply = bondy_mst:to_list(bondy_mst_grove:tree(Grove)),
    {reply, Reply, Grove};

handle_call({put, Key}, _From, Grove0) ->
    ct:pal("handling put key: ~p", [Key]),
    Grove1 = bondy_mst_grove:put(Grove0, Key, true),
    {reply, ok, Grove1};

handle_call({put, Key, Value}, _From, Grove0) ->
    ct:pal("handling put key: ~p, value: ~p", [Key, Value]),
    Grove1 = bondy_mst_grove:put(Grove0, Key, Value),
    {reply, ok, Grove1};

handle_call({trigger, Peer}, _From, Grove) ->
    ct:pal("Triggering sync on peer: ~p", [Peer]),
    Reply = bondy_mst_grove:trigger(Grove, Peer),
    {reply, Reply, Grove};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.


handle_cast({grove_message, Message}, Grove0) ->
    ct:pal(
        "(~p) Handling grove_message: ~p",
        [bondy_mst_grove:node_id(Grove0), Message]
    ),
    Grove = bondy_mst_grove:handle(Grove0, Message),
    {noreply, Grove};

handle_cast(_Request, Grove) ->
    {noreply, Grove}.


handle_info(_, Grove) ->
    {noreply, Grove}.


terminate(_Reason, _Grove) ->
    ok.

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
-export([on_merge/1]).


%% GEN_SERVER CALLBACKS
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

-type t() :: bondy_mst_groove:t().

%% =============================================================================
%% API
%% =============================================================================

peers() ->
    [
        peer1,
        peer2,
        peer3
    ].


start_all(StoreType) ->
    start(StoreType, peers()).


start(StoreType, Peers) ->
    Started = [
        begin
            Store = bondy_mst_store:new(
                StoreType, [{name, atom_to_binary(NodeId)}]
            ),
            {ok, _} = start_link(NodeId, Store),
            NodeId
        end
        || NodeId <- Peers
    ],
    {ok, Started}.


start_link(NodeId, Store) when is_atom(NodeId) ->
    gen_server:start_link({local, NodeId}, ?MODULE, [NodeId, Store], []).



%% =============================================================================
%% BONDY_MST_GROVE CALLBACKS
%% =============================================================================



send(Peer, Message) ->
    gen_server:cast(Peer, {grove_message, Message}).


broadcast(Event) ->
    ct:pal("Broadcasting event ~p", [Event]),
    [send(Peer, Event) || Peer <- peers()],
    ok.


on_merge(_Page) ->
    %% We do nothing as we are using the MST as the store itself.
    ok.



%% =============================================================================
%% GEN_SERVER BEHAVIOR CALLBACKS
%% ============================================================================


init([NodeId, Store]) ->
    %% Trap exists otherwise terminate/1 won't be called when shutdown by
    %% supervisor.
    erlang:process_flag(trap_exit, true),

    Opts = #{
        store => Store,
        callback_mod => ?MODULE,
        max_merges => 3,
        max_same_merge => 1
    },

    %% We create an ets-based MST bound to this process.
    %% The ets table will be garbage collected if this process terminates.
    Grove = bondy_mst_grove:new(NodeId, Opts),
    {ok, Grove}.

handle_call(ping, _From, Grove) ->
    {reply, pong, Grove};

handle_call(root, _From, Grove) ->
    ct:pal("handling root"),
    Reply = bondy_mst:root(bondy_mst_grove:tree(Grove)),
    {reply, Reply, Grove};

handle_call({get, Key}, _From, Grove) ->
    ct:pal("handling get key:~p", [Key]),
    Reply = bondy_mst:get(bondy_mst_grove:tree(Grove), Key),
    {reply, Reply, Grove};

handle_call(list, _From, Grove) ->
    ct:pal("handling list"),
    Reply = bondy_mst:to_list(bondy_mst_grove:tree(Grove)),
    {reply, Reply, Grove};

handle_call({put, Key}, _From, Grove0) ->
    ct:pal("handling put key:~p", [Key]),
    Grove1 = bondy_mst_grove:put(Grove0, Key, true),
    {reply, ok, Grove1};

handle_call({trigger, Peer}, _From, Grove0) ->
    ct:pal("handling sync peer:~p", [Peer]),
    Grove = bondy_mst_grove:trigger(Grove0, Peer),
    Reply = ok,
    {reply, Reply, Grove};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({grove_message, Message}, Grove0) ->
    Grove = bondy_mst_grove:handle(Grove0, Message),
    {noreply, Grove};

handle_cast(_Request, State) ->
    {noreply, State}.


handle_info(_, State) ->
    {noreply, State}.


terminate(_Reason, State) ->
    ok.

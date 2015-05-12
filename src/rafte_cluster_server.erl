%% Copyright (c) 2015, Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc TODO
-module(rafte_cluster_server).

-behaviour(gen_server).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([start_link/1]).
-export([update_server_specs/2]).
-export([child_spec/1]).
-export([broadcast/2]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'gen_server' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%----------------------------------------------------------------------------------------------------------------------
%% Records
%%----------------------------------------------------------------------------------------------------------------------
-record(state,
        {
          cluster_name :: rafte:cluster_name(),
          server_specs :: [rafte:server_spec()],
          generation = 0 :: non_neg_integer()
        }).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec start_link(rafte:cluster_name()) -> {ok, pid()} | {error, Reason::term()}.
start_link(ClusterName) ->
    gen_server:start_link(rafte_local_ns:cluster_server_name(ClusterName), ?MODULE, [ClusterName], []).

-spec update_server_specs(rafte:cluster_name(), [rafte:server_spec()]) -> ok.
update_server_specs(ClusterName, ServerSpecs) ->
    gen_server:cast(rafte_local_ns:cluster_server_name(ClusterName), {update_server_specs, ServerSpecs}).

-spec child_spec(rafte:cluster_name()) -> supervisor:child_spec().
child_spec(ClusterName) ->
    {?MODULE, {?MODULE, start_link, [ClusterName]}, permanent, 5000, worker, [?MODULE]}.

-spec broadcast(rafte:cluster_name(), term()) -> ok.
broadcast(ClusterName, Msg) ->
    gen_server:cast(rafte_local_ns:cluster_server_name(ClusterName), {broadcast, Msg}).

%%----------------------------------------------------------------------------------------------------------------------
%% 'gen_server' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
init([ClusterName]) ->
    State =
        #state{
           cluster_name = ClusterName,
           server_specs = []
          },
    {ok, State}.

%% @private
handle_call(get_server_specs, _From, State) ->
    {reply, State#state.server_specs, State};
handle_call(Request, From, State) ->
    {stop, {unknown_call, Request, From}, State}.

%% @private
handle_cast({update_server_specs, ServerSpecs}, State0) ->
    %% TODO: 更新内容に関しても、最初にクラスタ内で合意を取った方が良いかもしれない
    io:format("# [~p:~p] ~p: update_server_specs=~w\n", [?MODULE, ?LINE, self(), ServerSpecs]),
    State1 = State0#state{server_specs = ServerSpecs, generation = State0#state.generation + 1},
    %% TODO: 世代交代をハンドリングする
    ok = start_local_raft_servers(State1, ServerSpecs),
    {noreply, State1};
handle_cast({broadcast, Msg}, State) ->
    ok = lists:foreach(fun (Node) -> rpc:cast(Node, rafte_server, broadcast_local, [State#state.cluster_name, Msg]) end,
                       unique_nodes(State#state.server_specs)),
    {noreply, State};
handle_cast(Request, State) ->
    {stop, {unknown_cast, Request}, State}.

%% @private
handle_info(Info, State) ->
    {stop, {handle_info, Info}, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec start_local_raft_servers(#state{}, [rafte:server_spec()]) -> ok.
start_local_raft_servers(_, []) ->
    ok;
start_local_raft_servers(State, [Spec | Specs]) when element(1, Spec) =/= node() ->
    start_local_raft_servers(State, Specs);
start_local_raft_servers(State, [_Spec | Specs]) -> % TODO: use Spec
    {ok, _} = rafte_server_sup:start_child(State#state.cluster_name, length(State#state.server_specs)),
    start_local_raft_servers(State, Specs).

-spec unique_nodes([rafte:server_spec()]) -> [node()].
unique_nodes(Specs) ->
    lists:usort([element(1, Spec) || Spec <- Specs]).

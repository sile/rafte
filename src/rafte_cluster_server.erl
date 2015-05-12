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
    State1 = State0#state{server_specs = ServerSpecs, generation = State0#state.generation + 1},
    %% TODO: setup new generation cluster
    {noreply, State1};
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

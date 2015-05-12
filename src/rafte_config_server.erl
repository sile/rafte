%% Copyright (c) 2015, Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc TODO
-module(rafte_config_server).

-behaviour(gen_server).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([start_link/2]).
-export([get_server_specs/1]).
-export([child_spec/2]).

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
          server_specs :: [rafte:server_spec()]
        }).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec start_link(rafte:cluster_name(), [rafte:server_spec()]) -> {ok, pid()} | {error, Reason::term()}.
start_link(ClusterName, ServerSpecs) ->
    gen_server:start_link(rafte_local_ns:config_server_name(ClusterName), ?MODULE, [ClusterName, ServerSpecs], []).

-spec get_server_specs(rafte:cluster_name()) -> [rafte:server_spec()].
get_server_specs(ClusterName) ->
    gen_server:call(rafte_local_ns:config_server_name(ClusterName), get_server_specs).

-spec child_spec(rafte:cluster_name(), [rafte:server_spec()]) -> supervisor:child_spec().
child_spec(ClusterName, ServerSpecs) ->
    {?MODULE, {?MODULE, start_link, [ClusterName, ServerSpecs]}, permanent, 5000, worker, [?MODULE]}.

%%----------------------------------------------------------------------------------------------------------------------
%% 'gen_server' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
init([ClusterName, ServerSpecs]) ->
    State =
        #state{
           cluster_name = ClusterName,
           server_specs = ServerSpecs
          },
    ok = rafte_cluster_server:update_server_specs(ClusterName, ServerSpecs),
    {ok, State}.

%% @private
handle_call(get_server_specs, _From, State) ->
    {reply, State#state.server_specs, State};
handle_call(Request, From, State) ->
    {stop, {unknown_call, Request, From}, State}.

%% @private
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

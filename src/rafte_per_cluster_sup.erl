%% Copyright (c) 2015, Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc TODO
-module(rafte_per_cluster_sup).

-behaviour(supervisor).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([start_link/2, child_spec/2]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([init/1]).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @doc Starts supervisor process
-spec start_link(rafte:cluster_name(), [rafte:server_spec()]) -> {ok, pid()} | {error, Reason::term()}.
start_link(ClusterName, ServerSpecs) ->
    supervisor:start_link(?MODULE, [ClusterName, ServerSpecs]).

%% TODO: child_spec(rafte_config:config())
-spec child_spec(rafte:cluster_name(), [rafte:server_spec()]) -> supervisor:child_spec().
child_spec(ClusterName, ServerSpecs) ->
    {ClusterName, {?MODULE, start_link, [ClusterName, ServerSpecs]}, permanent, 5000, supervisor, [?MODULE]}.

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
init([ClusterName, ServerSpecs]) ->
    Children =
        [
         rafte_server_sup:child_spec(ClusterName),
         rafte_cluster_server:child_spec(ClusterName),
         rafte_config_server:child_spec(ClusterName, ServerSpecs)
        ],
    {ok, { {rest_for_one, 5, 10}, Children} }.

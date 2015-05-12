%% Copyright (c) 2015, Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc TODO
-module(rafte_local_ns).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([
         child_spec/0,
         config_server_name/1,
         cluster_server_name/1,
         server_sup_name/1
        ]).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    local:name_server_child_spec(?MODULE).

-spec config_server_name(rafte:cluster_name()) -> local:otp_name().
config_server_name(ClusterName) ->
    local:otp_name({?MODULE, {config_server, ClusterName}}).

-spec cluster_server_name(rafte:cluster_name()) -> local:otp_name().
cluster_server_name(ClusterName) ->
    local:otp_name({?MODULE, {cluster_server, ClusterName}}).

-spec server_sup_name(rafte:cluster_name()) -> local:otp_name().
server_sup_name(ClusterName) ->
    local:otp_name({?MODULE, {server_sup, ClusterName}}).

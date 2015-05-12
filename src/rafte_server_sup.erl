%% Copyright (c) 2015, Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc TODO
-module(rafte_server_sup).

-behaviour(supervisor).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([start_link/1, child_spec/1, start_child/2, which_servers/1]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([init/1]).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @doc Starts supervisor process
-spec start_link(rafte:cluster_name()) -> {ok, pid()} | {error, Reason::term()}.
start_link(ClusterName) ->
    supervisor:start_link(rafte_local_ns:server_sup_name(ClusterName), ?MODULE, [ClusterName]).

-spec child_spec(rafte:cluster_name()) -> supervisor:child_spec().
child_spec(ClusterName) ->
    {?MODULE, {?MODULE, start_link, [ClusterName]}, permanent, 5000, supervisor, [?MODULE]}.

-spec start_child(rafte:cluster_name(), pos_integer()) -> {ok, pid()} | {error, Reason::term()}.
start_child(ClusterName, MemberCount) ->
    supervisor:start_child(rafte_local_ns:server_sup_name(ClusterName), [MemberCount]).

-spec which_servers(rafte:cluster_name()) -> [pid()].
which_servers(ClusterName) ->
    [Pid || {_, Pid, _, _} <- supervisor:which_children(rafte_local_ns:server_sup_name(ClusterName))].

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
init([ClusterName]) ->
    ChildSpec = rafte_server:child_spec(ClusterName),
    {ok, { {simple_one_for_one, 5, 10}, [ChildSpec]} }.

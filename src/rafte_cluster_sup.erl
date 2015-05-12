%% Copyright (c) 2015, Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc TODO
-module(rafte_cluster_sup).

-behaviour(supervisor).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([start_link/0, start_child/2, terminate_child/1, child_spec/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([init/1]).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @doc Starts supervisor process
-spec start_link() -> {ok, pid()} | {error, Reason::term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_child(rafte:cluster_name(), [rafte:server_spec()]) -> {ok, pid()} | {error, Reason::term()}.
start_child(ClusterName, ServerSpecs) ->
    ChildSpec = rafte_per_cluster_sup:child_spec(ClusterName, ServerSpecs),
    supervisor:start_child(?MODULE, ChildSpec).

-spec terminate_child(rafte:cluster_name()) -> ok.
terminate_child(ClusterName) ->
    _ = supervisor:terminate_child(?MODULE, ClusterName),
    _ = supervisor:delete_child(?MODULE, ClusterName),
    ok.

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    {?MODULE, {?MODULE, start_link, []}, permanent, 5000, supervisor, [?MODULE]}.

%%----------------------------------------------------------------------------------------------------------------------
%% 'supervisor' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
init([]) ->
    {ok, { {one_for_one, 5, 10}, []} }.

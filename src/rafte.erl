%% Copyright (c) 2015, Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc TODO
%%
%% 未対応機能:
%% - 動的なクラスタ構成の変更
-module(rafte).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([
         create_cluster/2,
         delete_cluster/1,
         call/2, call/3,
         cast/2
        ]).

-export_type([
              cluster_name/0,
              server_spec/0
             ]).

%%----------------------------------------------------------------------------------------------------------------------
%% Types
%%----------------------------------------------------------------------------------------------------------------------
-type server_spec() :: {node(), module(), atom(), [term()]}. % node + MFArgs
-type cluster_name() :: atom().

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec create_cluster(cluster_name(), [server_spec()]) -> ok | {error, Reason::term()}.
create_cluster(ClusterName, ServerSpecs) ->
    error(not_implemented, [ClusterName, ServerSpecs]).

-spec delete_cluster(cluster_name()) -> ok.
delete_cluster(ClusterName) ->
    error(not_implemented, [ClusterName]).

-spec call(cluster_name(), term()) -> term().
call(ClusterName, Arg) ->
    call(ClusterName, Arg, 5000).

-spec call(cluster_name(), term(), timeout()) -> term().
call(ClusterName, Arg, Timeout) ->
    error(not_implemented, [ClusterName, Arg, Timeout]).

-spec cast(cluster_name(), term()) -> ok.
cast(ClusterName, Arg) ->
    error(not_implemented, [ClusterName, Arg]).

%% Copyright (c) 2015, Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc TODO
%%
%% TODO: gen_fsm
-module(rafte_server).

%%-behaviour(gen_fsm).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([start_link/2, child_spec/1]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'gen_fsm' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

%%----------------------------------------------------------------------------------------------------------------------
%% Records & Types
%%----------------------------------------------------------------------------------------------------------------------
-record(state,
        {
          cluster_name :: rafte:cluster_name(),
          member_count :: pos_integer()
        }).

%%-type role() :: follower | candidate | leader.

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec start_link(rafte:cluster_name(), pos_integer()) -> {ok, pid()} | {error, Reason::term()}.
start_link(ClusterName, MemberCount) ->
    gen_fsm:start_link(?MODULE, [ClusterName, MemberCount], []).

-spec child_spec(rafte:cluster_name()) -> supervisor:child_spec().
child_spec(ClusterName) ->
    {?MODULE, {?MODULE, start_link, [ClusterName]}, permanent, 5000, worker, [?MODULE]}.

%%----------------------------------------------------------------------------------------------------------------------
%% 'gen_fsm' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
init([ClusterName, MemberCount]) ->
    State =
        #state{
           cluster_name = ClusterName,
           member_count = MemberCount
          },
    {ok, follower, State}.

%% @private
handle_event(Event, StateName, State) ->
    {stop, {unknown_event, Event, StateName}, State}.

%% @private
handle_sync_event(Event, From, StateName, State) ->
    {stop, {unknown_sync_event, Event, From, StateName}, State}.

%% @private
handle_info(Info, StateName, State) ->
    {stop, {info, Info, StateName}, State}.

%% @private
terminate(_Reason, _StateName, _State) ->
    ok.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% Copyright (c) 2015, Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc TODO
-module(rafte_rpc).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([
         request_vote/5,
         reply/2
        ]).

-export_type([
              from/0
             ]).

%%----------------------------------------------------------------------------------------------------------------------
%% Types
%%----------------------------------------------------------------------------------------------------------------------
-type from() :: {pid(), reference()}.

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------

%% $5.2
%%
%% NOTE: 後で非同期に`{Ref, pid(), {NewCurrentTerm, VoteGranted::boolean()}'
%%       といった形式のメッセージが送信される (最大でmember_count個だけ)
%% => 簡単のために自分自身にも送信してしまう
-spec request_vote(rafte:cluster_name(), rafte_server:raft_term(), rafte_server:candidate_id(),
                   rafte_server:log_index(), % $5.4
                   rafte_server:raft_term()  % $5.4
                  ) -> reference().
request_vote(ClusterName, Term, CandidateId, LastLogIndex, LastLogTerm) ->
    Ref = make_ref(),
    From = {CandidateId, Ref},
    ok = rafte_cluster_server:broadcast(ClusterName, {request_vote, From, {Term, CandidateId, LastLogIndex, LastLogTerm}}),
    Ref.

-spec reply(from(), term()) -> ok.
reply({Pid, Ref}, Msg) ->
    _ = Pid ! {Ref, self(), {reply, Msg}},
    ok.

%% Copyright (c) 2015, Takeru Ohta <phjgt308@gmail.com>
%%
%% @doc TODO
-module(rafte_rpc).

%%----------------------------------------------------------------------------------------------------------------------
%% Exported API
%%----------------------------------------------------------------------------------------------------------------------
-export([
         request_vote/5,
         append_entries/2,
         append_entries/3,
         reply/2
        ]).

-export_type([
              from/0,
              append_entries_arg/0
             ]).

%%----------------------------------------------------------------------------------------------------------------------
%% Types
%%----------------------------------------------------------------------------------------------------------------------
-type from() :: {pid(), reference(), atom()}.

-type append_entries_arg() ::
        {
          rafte:raft_term(),
          rafte_server:leader_id(),
          PrevLogIndex :: rafte_server:log_index(), % TODO: この二つは常にセットで扱ってしまっても良さそう({index, term})
          PrevLogTerm :: rafte_server:raft_term(),
          Entries :: rafte_server:log(), % log entries to store(empty for heartbeat)
          LeaderCommit :: rafte_server:log_index()
        }.

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------

%% NOTE: rpcのリクエストないしレスポンスのtermが自分よりも大きかったら、current_termを更新して、followerになる ($5.1)


%% $5.2
%%
%% NOTE: 後で非同期に`{Ref, pid(), {Term, VoteGranted::boolean()}'
%%       といった形式のメッセージが送信される (最大でmember_count個だけ)
%% => 簡単のために自分自身にも送信してしまう
-spec request_vote(rafte:cluster_name(), rafte_server:raft_term(), rafte_server:candidate_id(),
                   rafte_server:log_index(), % $5.4
                   rafte_server:raft_term()  % $5.4
                  ) -> reference().
request_vote(ClusterName, Term, CandidateId, LastLogIndex, LastLogTerm) ->
    Ref = make_ref(),
    From = {CandidateId, Ref, request_vote},
    ok = rafte_cluster_server:broadcast(ClusterName, {request_vote, From, {Term, CandidateId, LastLogIndex, LastLogTerm}}),
    Ref.

%% $5.3
%%
%% NOTE: 後で非同期に`{Ref, pid(), {Term, Success::boolean()}'といった形式のメッセージが送信される
%%
%% XXX: follower毎に送信内容を変更したいのでブロードキャストではだめ
-spec append_entries(refte:cluster_name(), append_entries_arg()) -> reference().
append_entries(ClusterName, Arg) ->
    Ref = make_ref(),
    From = {self(), Ref, append_entries},
    ok = rafte_cluster_server:broadcast(ClusterName, {append_entries, From, Arg}),
    Ref.

%% 個別送信版
-spec append_entries(refte:cluster_name(), pid(), append_entries_arg()) -> reference().
append_entries(_ClusterName, Pid, Arg) ->
    Ref = make_ref(),
    From = {self(), Ref, append_entries},
    ok = gen_fsm:send_all_state_event(Pid, {append_entries, From, Arg}), % TODO: rafte_serverが提供する関数を使う
    Ref.

-spec reply(from(), term()) -> ok.
reply({Pid, Ref, Term}, Msg) ->
    _ = Pid ! {Ref, self(), Term, {reply, Msg}},
    ok.

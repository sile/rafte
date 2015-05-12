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
-export([start_link/2, child_spec/1, broadcast_local/2, cast_command/2]).
-export_type([leader_id/0]).

%%----------------------------------------------------------------------------------------------------------------------
%% 'gen_fsm' Callback API
%%----------------------------------------------------------------------------------------------------------------------
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).
-export([follower/2, candidate/2, leader/2]).

%%----------------------------------------------------------------------------------------------------------------------
%% Macros & Records & Types
%%----------------------------------------------------------------------------------------------------------------------
%% -define(ELECTION_TIMEOUT_MIN, 150). % ms
%% -define(ELECTION_TIMEOUT_MAX, 300). % ms

-define(ELECTION_TIMEOUT_MIN, 1500). % ms
-define(ELECTION_TIMEOUT_MAX, 3000). % ms

-define(HEARTBEAT_INTERVAL, (?ELECTION_TIMEOUT_MIN div 2)).

-define(NEXT_TIMEOUT, random_range(?ELECTION_TIMEOUT_MIN, ?ELECTION_TIMEOUT_MAX)).

-define(DEBUG(Fmt, Role, Args), io:format("# [~p:~p] ~p@~p: " ++ Fmt ++ "\n", [?MODULE, ?LINE, self(), Role | Args])).
-define(DEBUG(Fmt, Args), io:format("# [~p:~p] ~p: " ++ Fmt ++ "\n", [?MODULE, ?LINE, self() | Args])).

-define(PERSISTENT, state.persistent#persistent).

-record(persistent, % => stable storageに保存される必要がある
        {
          current_term = 0 :: raft_term(),
          voted_for :: undefined | candidate_id(),

          %% NOTE: ログに入っただけではcommitされていない(過半数が保持して初めてcommit)
          %% NOTE: 便宜上logは逆順で保持されている
          log = [] :: log()
        }).

-record(common_volatile,
        {
          %% commit_index > last_applied なら、log[last_applied]を適用して、last_appliedをインクリメントする($5.3)
          commit_index = 0 :: log_index(),
          last_applied = 0 :: log_index(),

          state_machine = [] :: [{atom(), term()}]
        }).

-record(leader_volatile,
        {
          %% 初期値は自分(leader)のログインデックス(last_applied)
          %% (followerと齟齬があるなら、共通部分まで辿って上書きする(RPCの一貫))
          next_index = [] :: [{pid(), log_index()}],  % default: leader's last_log_index+1

          match_index = [] :: [{pid(), log_index()}] % default: 0
        }).

-record(state,
        {
          cluster_name :: rafte:cluster_name(),
          member_count :: pos_integer(),
%          alives = [] :: [pid()], % length(alives) =< member_count-1 (self() is excluded)

          persistent = #persistent{} :: #persistent{},
          common_volatile = #common_volatile{} :: #common_volatile{},
          leader_volatile :: #leader_volatile{} | undefined,

          leader :: undefined | leader_id(),

          vote_ref :: undefined | reference(),
          voted_count = 0 :: non_neg_integer(),

          heartbeat_ref :: undefined | reference()
        }).

-type state_name() :: follower | candidate | leader.

%% TODO: カスタマイズ可能にする
-type command() :: {set, atom(), term()}
                 | {del, atom()}
                 | noop. % system command (carry no log entry) => TODO: delete?

-type raft_term() :: non_neg_integer().
-type log_index() :: non_neg_integer().

-type candidate_id() :: pid().
-type leader_id() :: pid().

-type log() :: [{raft_term(), log_index(), command()}]. % NOTE: log_index/0 は便宜上追加しているだけで本質的には不要

%%----------------------------------------------------------------------------------------------------------------------
%% Exported Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec start_link(rafte:cluster_name(), pos_integer()) -> {ok, pid()} | {error, Reason::term()}.
start_link(ClusterName, MemberCount) ->
    gen_fsm:start_link(?MODULE, [ClusterName, MemberCount], []).

-spec child_spec(rafte:cluster_name()) -> supervisor:child_spec().
child_spec(ClusterName) ->
    {?MODULE, {?MODULE, start_link, [ClusterName]}, permanent, 5000, worker, [?MODULE]}.

-spec broadcast_local(rafte:cluster_name(), term()) -> ok.
broadcast_local(ClusterName, Msg) ->
    %% TODO: 世代を考慮する必要はあるかもしれない
    lists:foreach(fun (Pid) -> gen_fsm:send_all_state_event(Pid, Msg) end, rafte_server_sup:which_servers(ClusterName)).

-spec cast_command(rafte:cluster_name(), command()) -> ok.
cast_command(ClusterName, Cmd) ->
    [Pid | _] = rafte_server_sup:which_servers(ClusterName), % XXX:
    gen_fsm:send_all_state_event(Pid, {cast_command, Cmd}).

%%----------------------------------------------------------------------------------------------------------------------
%% 'gen_fsm' Callback Functions
%%----------------------------------------------------------------------------------------------------------------------
%% @private
init([ClusterName, MemberCount]) ->
    random:seed(now()),
    ?DEBUG("start: ~p", follower, [ClusterName]),
    State =
        #state{
           cluster_name = ClusterName,
           member_count = MemberCount
          },
    Timeout = ?NEXT_TIMEOUT,
    ?DEBUG("timeout=~p", follower, [Timeout]),
    {ok, follower, State, Timeout}.

%% @private
handle_event({request_vote, From, Arg}, StateName0, State0) ->
    Term = element(1, Arg),
    {StateName1, State1} = check_rpc_term(Term, StateName0, State0),
    {Result, State2} = handle_request_vote(Arg, State1),
    ok = rafte_rpc:reply(From, Result),
    {next_state, StateName1, State2, ?NEXT_TIMEOUT};
handle_event({append_entries, {Pid, _, _}, _Arg}, StateName0, State0) when Pid =:= self() ->
    {next_state, StateName0, State0};
handle_event({append_entries, From, Arg}, StateName0, State0) ->
    Term = element(1, Arg),
    {StateName1, State1} = check_rpc_term(Term, StateName0, State0),
    {Result, State2} = handle_append_entries(Arg, State1),
    ok = rafte_rpc:reply(From, Result),
    {next_state, StateName1, State2, ?NEXT_TIMEOUT};
handle_event({cast_command, Cmd}, leader, State) ->
    ?DEBUG("add commnad: ~p", leader, [Cmd]),
    %% NOTE: 同期はheartbeatに任せる(実装が楽なので)
    P = State#state.persistent,
    Log0 = State#?PERSISTENT.log,
    Log1 = [{State#?PERSISTENT.current_term, last_log_index(Log0)+1, Cmd} | Log0],
    State1 = State#state{persistent = P#persistent{log = Log1}},
    %% ?DEBUG("log: ~w", [State1#?PERSISTENT.log]),
    {next_state, leader, State1};
handle_event({cast_command, Cmd}, StateName, State) ->
    case State#state.leader of
        undefined -> ?DEBUG("leader doesn't exist", StateName, []);
        Leader    -> ?DEBUG("redirect to ~p", StateName, [Leader]),
                     gen_fsm:send_all_state_event(Leader, {cast_command, Cmd})
    end,
    {next_state, StateName, State, ?NEXT_TIMEOUT}; % XXX: こういった処理でタイムアウトが延長するのは良くない
handle_event(Event, StateName, State) ->
    {stop, {unknown_event, Event, StateName}, State}.

%% @private
handle_sync_event(Event, From, StateName, State) ->
    {stop, {unknown_sync_event, Event, From, StateName}, State}.

%% @private
handle_info({Ref, Pid, _, {reply, {Term, false}}}, candidate, State0 = #state{vote_ref = Ref}) ->
    ?DEBUG("rejected from ~p {term=~p}", candidate, [Pid, Term]),
    {StateName1, State1} = check_rpc_term(Term, candidate, State0),
    {next_state, StateName1, State1, ?NEXT_TIMEOUT};
handle_info({Ref, Pid, _, {reply, {Term, true}}}, candidate, State0 = #state{vote_ref = Ref}) ->
    ?DEBUG("voted from ~p {term=~p, count=~p/~p}", candidate, [Pid, Term, State0#state.voted_count + 1,
                                                               State0#state.member_count]),
    State1 = State0#state{voted_count = State0#state.voted_count + 1},
    case State1#state.voted_count > (State1#state.member_count div 2) of
        false -> {next_state, candidate, State1, ?NEXT_TIMEOUT};
        true  ->
            %% 過半数得票 => leader!
            ?DEBUG("switch to ~p {term=~p}", candidate, [leader, State1#?PERSISTENT.current_term]),

            State2 = heartbeat(State1#state{vote_ref = undefined, leader_volatile = #leader_volatile{}}),
            {next_state, leader, State2} % NOTE: leaderはtimeoutしない
    end;
handle_info({_Ref, Pid, append_entries, {reply, {Term, Success, LogIndex}}}, leader, State0) ->
    case check_rpc_term(Term, leader, State0) of
        {follower, State1} -> {next_state, follower, State1, ?NEXT_TIMEOUT};
        {leader, State1} ->
            case Success of
                true ->
                    %% 同期完了
                    State2 = update_node_index(Pid, LogIndex, State1),
                    State3 = commit(State2),
                    State4 = apply_commited_log(State3),
                    {next_state, leader, State4};
                false ->
                    %% 不整合が残っている
                    State2 = sync(Pid, LogIndex-1, State1),
                    {next_state, leader, State2}
            end
    end;
handle_info({Ref, Pid, _, {reply, Msg}}, StateName, State) ->
    ?DEBUG("unknown reply(maybe too old): ref=~p, pid=~p, msg=~w", StateName, [Ref, Pid, Msg]),
    {next_state, StateName, State, ?NEXT_TIMEOUT};
handle_info(Info, StateName, State) ->
    {stop, {unknown_info, Info, StateName}, State}.

%% @private
terminate(_Reason, _StateName, _State) ->
    ?DEBUG("terminate: ~p", _StateName, [_Reason]),
    ok.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% [follower]
%% - candidate/leaderからのRPCに応答する
%%   - ! 逆にfollower以外は応答しないのか？ => termが小さい場合はリジェクトするので結果的には応答しないに等しい(?)
%% - timeoutしたらcandidateになる

%% @private
follower(timeout, State0) ->
    State1 = ready_election(State0),
    ?DEBUG("switch to ~p {term=~p}", follower, [candidate, State1#?PERSISTENT.current_term]),
    VoteRef = request_vote(State1),
    {next_state, candidate, State1#state{vote_ref = VoteRef}, ?NEXT_TIMEOUT}.

%% [candidate]
%% - condidateへの遷移時:
%%   - current_term++
%%   - 自分に投票
%%   - vote_rpcを他に送る
%% - 過半数を取得したらleaderに
%% - append_entire_rpcをleaderから取得したらfollowerに
%% - timeoutしたら新しい選挙を始める

%% @private
candidate(timeout, State0) ->
    %% 投票期限を切れた(maybe split vote)ので、もう一度
    State1 = ready_election(State0),
    ?DEBUG("vote retry {term=~p}", candidate, [State1#?PERSISTENT.current_term]),
    VoteRef = request_vote(State1),
    {next_state, candidate, State1#state{vote_ref = VoteRef}, ?NEXT_TIMEOUT};
candidate(Event, State) ->
    {stop, {unknown_candiate_event, Event}, State}.

%% [leader]
%% - election時にからのappend-rpc(heartbeat)を送る. timeoutを防ぐために定期的に送る ($5.2)
%% - クライアントからコマンドを受けたら、ローカルログに追加し、状態機械に適用後に応答する ($5.3)
%%   - NOTE: 状態機械に適用されるのはcommit後 (all servers条件を参照)
%% - クライアントとindex/termに齟齬があったら是正するよ、的な話($5.3)。完了したらnextIndex[]/matchIndex[]を更新
%% - N > commitIndex and is_majorit(mmatchIndex[i] >= N) and log[N].term=current then commitIndex = N ($5.3,$5.4)
%%
%% - ! leaderはタイムアウトしない

%% @private
leader(heartbeat, State0) ->
    State1 = heartbeat(State0),
    {next_state, leader, State1};
leader(Event, State) ->
    {stop, {unknown_leader, Event}, State}.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec ready_election(#state{}) -> #state{}.
ready_election(State = #state{persistent = Persistent}) ->
    Term = Persistent#persistent.current_term + 1,
    State#state{leader = undefined,
                persistent = Persistent#persistent{current_term = Term, voted_for = self()}, voted_count = 0}.

-spec random_range(pos_integer(), pos_integer()) -> pos_integer().
random_range(Min, Max) ->
    random:uniform(Max - Min) + Min.

-spec request_vote(#state{}) -> reference().
request_vote(State) ->
    rafte_rpc:request_vote(State#state.cluster_name, State#?PERSISTENT.current_term, self(),
                           last_log_index(State#?PERSISTENT.log), last_log_term(State#?PERSISTENT.log)).

-spec last_log_index(log()) -> log_index().
last_log_index([])              -> 0;
last_log_index([{_, I, _} | _]) -> I.

-spec last_log_term(log()) -> raft_term().
last_log_term([])              -> 0;
last_log_term([{T, _, _} | _]) -> T.

-spec handle_request_vote(Arg, #state{}) -> {Result, #state{}} when
      Arg :: {rafte_server:raft_term(), rafte_server:candidate_id(),
              rafte_server:log_index(), rafte_server:raft_term()},
      Result :: {rafte_server:raft_term(), boolean()}.
handle_request_vote({Term, CandidateId, LastLogIndex, LastLogTerm}, State = #state{persistent = #persistent{log = Log}}) ->
    %% NOTE: 基本的には早いもの順で投票するが $5.4 で別の制限も追加されている (古すぎる候補者は却下)
    case Term < State#?PERSISTENT.current_term of
        true  -> {{State#?PERSISTENT.current_term, false}, State}; % 要求者の時間が古すぎ ($5.1)
        false ->
            %% $5.2, $5.4
            case State#?PERSISTENT.voted_for of
                VotedFor when is_pid(VotedFor), VotedFor =/= CandidateId -> % 別の候補に投票済み
                    {{State#?PERSISTENT.current_term, false}, State};
                _ -> % 新しい候補 or 既に候補となっている(投票されている)
                    case {LastLogTerm, LastLogIndex} >= {last_log_term(Log), last_log_index(Log)} of % TODO: 比較方法確認
                        false -> {{State#?PERSISTENT.current_term, fales}, State}; % 要求者のログは古い
                        true  ->
                            %% 要求者のログは古くない => 投票!
                            State1 = State#state{persistent = State#?PERSISTENT{voted_for = CandidateId}},
                            ?DEBUG("vote for ~p {term=~p}", [CandidateId, Term]),
                            {{State#?PERSISTENT.current_term, true}, State1}
                    end
            end
    end.

-spec handle_append_entries(rafte_rpc:append_entries_arg(), #state{}) -> {Result, #state{}} when
      Result :: {rafte_server:raft_term(), boolean()}.
handle_append_entries({Term, LeaderId, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit},
                      State0 = #state{persistent = #persistent{current_term = CurrentTerm, log = Log}}) ->
    case Term < CurrentTerm of
        true  -> {{CurrentTerm, false, PrevLogIndex}, State0}; % 1. 要求者の時間が古すぎ ($5.1) => leaderがfollowerに
        false ->
            State = State0#state{leader = LeaderId},
            case check_log(PrevLogIndex, PrevLogTerm, Log) of
                not_exist ->
                    ?DEBUG("no such index(~p)", follower, [{PrevLogIndex, PrevLogTerm}]),
                    {{CurrentTerm, false, PrevLogIndex}, State}; % 2. ログが古い ($5.3) => leaderがリトライ
                differ    ->
                    ?DEBUG("differ index(~p)", follower, [{PrevLogIndex, PrevLogTerm}]),
                    {{CurrentTerm, false, PrevLogIndex}, State}; % 3. 不整合 ($5.3) => leaderがリトライ
                exist     ->
                    Entries =/= [] andalso ?DEBUG("update: entries=~w", [Entries]),
                    %% 4/5. leaderに合わせてログを更新
                    State1 = update_log(PrevLogIndex, Entries, LeaderCommit, State),
                    State2 = apply_commited_log(State1),
                    {{CurrentTerm, true, last_log_index(State2#?PERSISTENT.log)}, State2}
            end
    end.

-spec update_log(log_index(), log(), log_index(), #state{}) -> #state{}.
update_log(PrevLogIndex, Entries, LeaderCommit, State = #state{common_volatile = C,
                                                               persistent = P = #persistent{log = Log0}}) ->
    Log1 = lists:filter(fun ({_, I, _}) -> I =< PrevLogIndex end, Log0), % 衝突部分を除去
    Log2 = Entries ++ Log1,
    %%?DEBUG("log1=~w, log2=~w, log0=~w, prev=~p", [Log1, Log2, Log0, PrevLogIndex]),

    true = LeaderCommit >= C#common_volatile.commit_index, % ! この条件は常に成り立ちそうな気がした(不確か)

    State#state{common_volatile = C#common_volatile{commit_index = LeaderCommit},
                persistent = P#persistent{log = Log2}}.

-spec apply_commited_log(#state{}) -> #state{}.
apply_commited_log(State = #state{common_volatile = C}) ->
    case C#common_volatile.commit_index > C#common_volatile.last_applied of
        false -> State;
        true  ->
            ?DEBUG("apply: ~p~~~p", [C#common_volatile.last_applied, C#common_volatile.commit_index]),

            %% コミットされた差分を適用する
            Result =
                lists:foldr(
                  fun ({_, _, Command}, Acc) -> apply_command(Command, Acc) end,
                  C#common_volatile.state_machine,
                  lists:takewhile(fun ({_, I, _}) -> I > C#common_volatile.last_applied end,
                                  State#?PERSISTENT.log)
                 ),
            ?DEBUG("result=~w", [Result]),
            State#state{common_volatile = C#common_volatile{last_applied = C#common_volatile.commit_index,
                                                            state_machine = Result}}
    end.

-spec apply_command(command(), [{atom(), term()}]) -> [{atom(), term()}].
apply_command({set, Key, Val}, Assoc) ->
    lists:keystore(Key, 1, Assoc, {Key, Val});
apply_command({del, Key}, Assoc) ->
    lists:keydelete(Key, 1, Assoc).

-spec check_log(log_index(), raft_term(), log()) -> not_exist | differ | exist.
check_log(0, 0, [])         -> exist;
check_log(Index, Term, Log) ->
    %% XXX: もの凄く非効率
    case lists:keyfind(Index, 2, Log) of
        false        -> not_exist;
        {Term, _, _} -> exist;
        _            -> differ
    end.

%% NOTE: rpcのリクエストないしレスポンスのtermが自分よりも大きかったら、current_termを更新して、followerになる ($5.1)
-spec check_rpc_term(raft_term(), state_name(), #state{}) -> {state_name(), #state{}}.
check_rpc_term(Term, StateName, State0) when Term =< State0#?PERSISTENT.current_term ->
    {StateName, State0};
check_rpc_term(Term, StateName, State0 = #state{persistent = P}) ->
    ?DEBUG("too old term(~p < ~p). switch to follower", StateName, [State0#?PERSISTENT.current_term, Term]),
    State1 = State0#state{persistent = P#persistent{current_term = Term, voted_for = undefined},
                          leader_volatile = undefined},
    _ = (catch gen_fsm:cancel_timer(State0#state.heartbeat_ref)), % TODO: 必要なときだけ
    {follower, State1}.

-spec heartbeat(#state{}) -> #state{}.
heartbeat(State0) ->
    %% ?DEBUG("heartbeat: last=~p", leader, [
    %%                                       {last_log_index(State0#?PERSISTENT.log),
    %%                                        last_log_term(State0#?PERSISTENT.log)}
    %%                                      ]),
    Arg = {
      State0#?PERSISTENT.current_term,
      self(),
      last_log_index(State0#?PERSISTENT.log),
      last_log_term(State0#?PERSISTENT.log),
      [],
      State0#state.common_volatile#common_volatile.commit_index
     },
    _ = rafte_rpc:append_entries(State0#state.cluster_name, Arg),
    Ref = gen_fsm:send_event_after(?HEARTBEAT_INTERVAL, heartbeat),
    State0#state{heartbeat_ref = Ref}.

-spec update_node_index(pid(), log_index(), #state{}) -> #state{}.
update_node_index(Pid, Index, State0 = #state{leader_volatile = V}) ->
    %% TODO: monitorで死活監視をしないといけない (Pid)
    NextIndex = lists:keystore(Pid, 1, V#leader_volatile.next_index, {Pid, last_log_index(State0#?PERSISTENT.log)}),
    MatchIndex = lists:keystore(Pid, 1, V#leader_volatile.match_index, {Pid, Index}),
    State0#state{leader_volatile = V#leader_volatile{next_index = NextIndex, match_index = MatchIndex}}.

-spec sync(pid(), log_index(), #state{}) -> #state{}.
sync(Pid, Index, State0 = #state{leader_volatile = V}) ->
    NextIndex = lists:keystore(Pid, 1, V#leader_volatile.next_index, {Pid, Index}), % TODO: heartbeatの時にnext_indexを見る

    Entries = lists:takewhile(fun ({_, I, _}) -> I > Index end, State0#?PERSISTENT.log),

    Arg = {
      State0#?PERSISTENT.current_term,
      self(),
      Index,
%%      element(1, lists:keyfind(Index, 2, State0#?PERSISTENT.log)),
      element(1, lists:keyfind(Index, 2, [{0, 0, sentinel} | State0#?PERSISTENT.log])),
      Entries,
      State0#state.common_volatile#common_volatile.commit_index
     },
    ?DEBUG("sync: pid=~p, index=~p, entries=~w", [Pid, Index, Entries]),
    _ = rafte_rpc:append_entries(State0#state.cluster_name, Pid, Arg), % TODO: 上のTODOが解消すればこっちは不要かも
    State0#state{leader_volatile = V#leader_volatile{next_index = NextIndex}}.

-spec commit(#state{}) -> #state{}.
commit(State = #state{leader_volatile = #leader_volatile{match_index = Match},
                      common_volatile = C}) ->
    Border = State#state.member_count div 2,
    case length(Match) > Border of % majority?
        false -> State;
        true  ->
            MajorityCommit = lists:nth(Border+1, lists:reverse(lists:sort([I || {_, I} <- Match]))),
            case MajorityCommit > C#common_volatile.commit_index andalso
                element(1, lists:keyfind(MajorityCommit, 2, State#?PERSISTENT.log)) =:= State#?PERSISTENT.current_term of
                false -> State;
                true  ->
                    ?DEBUG("commit: ~p~~~p", leader, [C#common_volatile.commit_index, MajorityCommit]),
                    State#state{common_volatile = C#common_volatile{commit_index = MajorityCommit}}
            end
    end.

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
-export([start_link/2, child_spec/1, broadcast_local/2]).

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

-define(NEXT_TIMEOUT, random_range(?ELECTION_TIMEOUT_MIN, ?ELECTION_TIMEOUT_MAX)).

-define(DEBUG(Fmt, Role, Args), io:format("# [~p:~p] ~p@~p: " ++ Fmt ++ "\n", [?MODULE, ?LINE, self(), Role | Args])).
-define(DEBUG(Fmt, Args), io:format("# [~p:~p] ~p: " ++ Fmt ++ "\n", [?MODULE, ?LINE, self() | Args])).

-define(PERSISTENT, state.persistent#persistent).

-record(persistent,
        {
          current_term = 0 :: raft_term(),
          voted_for :: undefined | candidate_id(),
          log = [] :: log(),
          snapshot = [] :: [{atom(), number()}]
        }).

-record(common_volatile,
        {
          commit_index = 0 :: log_index(),
          last_applied = 0 :: log_index()
        }).

-record(leader_volatile,
        {
          next_index = [] :: [{pid(), log_index()}],
          match_index = [] :: [{pid(), log_index()}]
        }).

-record(state,
        {
          cluster_name :: rafte:cluster_name(),
          member_count :: pos_integer(),
          alives = [] :: [pid()], % length(alives) =< member_count-1 (self() is excluded)

          persistent = #persistent{} :: #persistent{},
          common_volatile = #common_volatile{} :: #common_volatile{},
          leader_volatile :: #leader_volatile{} | undefined,

          vote_ref :: undefined | reference(),

          voted_count = 0 :: non_neg_integer()
        }).

%%-type role() :: follower | candidate | leader.

%% TODO: カスタマイズ可能にする
-type command() :: {set, atom(), number()}
                 | {add, atom(), number()}
                 | {del, atom(), number()}.

-type raft_term() :: non_neg_integer().
-type log_index() :: non_neg_integer().

-type candidate_id() :: pid().

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
handle_event({request_vote, From, Arg}, StateName, State0) ->
    {Result, State1} = handle_request_vote(Arg, State0),
    ok = rafte_rpc:reply(From, Result),
    {next_state, StateName, State1, ?NEXT_TIMEOUT};
%% TODO: candidate中にappend_entries_rpcを受けた場合の処理 (別のサーバがリーダーになった)
handle_event(Event, StateName, State) ->
    {stop, {unknown_event, Event, StateName}, State}.

%% @private
handle_sync_event(Event, From, StateName, State) ->
    {stop, {unknown_sync_event, Event, From, StateName}, State}.

%% @private
handle_info({Ref, Pid, {reply, {Term, false}}}, candidate, State0 = #state{vote_ref = Ref}) ->
    ?DEBUG("rejected from ~p {term=~p}", candidate, [Pid, Term]),
    %% TODO: `Term'が自分の値よりも大きい場合は、合わせて更新する必要がありそう？
    {next_state, candidate, State0, ?NEXT_TIMEOUT};
handle_info({Ref, Pid, {reply, {Term, true}}}, candidate, State0 = #state{vote_ref = Ref}) ->
    ?DEBUG("voted from ~p {term=~p, count=~p/~p}", candidate, [Pid, Term, State0#state.voted_count + 1,
                                                               State0#state.member_count]),
    State1 = State0#state{voted_count = State0#state.voted_count + 1},
    case State1#state.voted_count > (State1#state.member_count div 2) of
        false -> {next_state, candidate, State1, ?NEXT_TIMEOUT};
        true  ->
            %% 過半数得票 => leader!
            ?DEBUG("switch to ~p {term=~p}", candidate, [leader, State1#?PERSISTENT.current_term]),
            {next_state, leader, State1#state{vote_ref = undefined}, ?NEXT_TIMEOUT}
    end;
handle_info({Ref, Pid, {reply, Msg}}, StateName, State) ->
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

%% @private
follower(timeout, State0) ->
    State1 = ready_election(State0),
    ?DEBUG("switch to ~p {term=~p}", follower, [candidate, State1#?PERSISTENT.current_term]),

    VoteRef = request_vote(State1),

    {next_state, candidate, State1#state{vote_ref = VoteRef}, ?NEXT_TIMEOUT}.

%% @private
candidate(timeout, State0) ->
    %% 投票期限を切れた(maybe split vote)ので、waitを挟んでやり直す
    Persistent = State0#state.persistent,
    State1 = State0#state{persistent = Persistent#persistent{voted_for = undefined}},
    ?DEBUG("vote timed out. switch to ~p {term=~p}", candidate, [follower, State1#?PERSISTENT.current_term]),
    {next_state, follower, State1#state{vote_ref = undefined}, ?NEXT_TIMEOUT};
candidate(Event, State) ->
    {stop, {unknown_candiate_event, Event}, State}.

%% @private
leader(Event, State) ->
    {stop, {unknown_leader, Event}, State}.

%%----------------------------------------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------------------------------------
-spec ready_election(#state{}) -> #state{}.
ready_election(State = #state{persistent = Persistent}) ->
    Term = Persistent#persistent.current_term + 1,
    State#state{persistent = Persistent#persistent{current_term = Term, voted_for = self()}, voted_count = 0}.

-spec random_range(pos_integer(), pos_integer()) -> pos_integer().
random_range(Min, Max) ->
    random:uniform(Max - Min) + Min.

-spec request_vote(#state{}) -> reference().
request_vote(State) ->
    rafte_rpc:request_vote(State#state.cluster_name, State#?PERSISTENT.current_term, self(),
                           last_log_index(State#?PERSISTENT.log), last_log_term(State#?PERSISTENT.log)).

-spec last_log_index(log()) -> log_index().
last_log_index([])              -> 1;
last_log_index([{I, _, _} | _]) -> I.

-spec last_log_term(log()) -> raft_term().
last_log_term([])              -> 1;
last_log_term([{_, T, _} | _]) -> T.

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

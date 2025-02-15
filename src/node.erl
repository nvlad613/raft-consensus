-module(node).

-include("log.hrl").

-behaviour(gen_fsm).

-export([handle_event/3, handle_sync_event/4, init/1, idle/2, follower/2]).

-type state() :: idle | leader | follower | candidate.

-record(node_data, {
    log :: #log{},
    term :: integer(),
    nodes :: list(),
    leader :: identifier(),
    timer :: reference()
}).

init([Log, InitialState]) ->
    {_, Term} = log:last(Log),
    {ok, InitialState, #node_data{log = Log, term = Term, nodes = [], leader = none, timer = none}}.

handle_event(_Req, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Req, _From, StateName, State) ->
    {noreply, StateName, State}.

idle({add_node, node_name}, State) ->
    Nodes = State#node_data.nodes,
    {next_state, idle, State#node_data{nodes = [node_name | Nodes]}};
idle(start, State) ->
    State1 = reset_election_timer(follower, State),
    {next_state, follower, State1}.

% Election logic:
% Election timer expired, so become a candidate
follower({tick, follower}, State) ->
    {next_state, candidate, State};
follower({tick, _}, State) ->
    {next_state, follower, State};
%
% Election logic:
% Handle <RequestVote>
follower(
    {request_vote, _, LeaderTerm, _},
    State = #node_data{term = LocalTerm}
) when LeaderTerm =< LocalTerm ->
    {next_state, follower, State};
follower(
    {request_vote, {LogIndex, LogTerm}, LeaderTerm, LeaderRef},
    State = #node_data{log = Log}
) ->
    case log_actuality(Log, LogIndex, LogTerm) of
        LessOrEq when LessOrEq == less orelse LessOrEq == equal ->
            State1 = reset_election_timer(follower, State),
            State2 = State1#node_data{term = LeaderTerm, leader = LeaderRef},
            send_answer(State2, ok),
            {next_state, follower, State2};
        _ ->
            {next_state, follower, State}
    end;
%
% Log Replication logic:
% Handle empty <AppendEntries>
follower(
    {append_entries, {LogIndex, LogTerm}, LeaderTerm, LeaderRef},
    State = #node_data{log = Log, term = LocalTerm}
) when LeaderTerm >= LocalTerm ->
    State1 = reset_election_timer(follower, State),
    State2 = State1#node_data{term = LeaderTerm, leader = LeaderRef},
    UncommitedIndex = log:size(Log) - 1,
    case log:commited(Log) of
        % logIndex == replIndex, logTerm == replTerm
        {ReplIndex, _, ReplTerm} when LogIndex =:= ReplIndex andalso LogTerm =:= ReplTerm ->
            send_answer(State2, ok),
            {next_state, follower, State2};
        % logIndex == replIndex, logTerm /= replTerm
        {ReplIndex, _, _} when LogIndex =:= ReplIndex ->
            send_answer(State2, error),
            {next_state, follower, State2};
        % logIndex > replIndex (leader want to commit)
        {ReplIndex, _, ReplTerm} when LogIndex > ReplIndex andalso LogIndex =< UncommitedIndex ->
            case log:at(Log, ReplIndex) of
                {_, _, TermToCommit} when TermToCommit =:= ReplTerm ->
                    State3 = State2#node_data{log = log:commit(Log, LogIndex)},
                    send_answer(State3, ok),
                    {next_state, follower, State3};
                _ ->
                    send_answer(State2, error),
                    {next_state, follower, State2}
            end;
        {ReplIndex, _, _} when LogIndex > ReplIndex ->
            send_answer(State2, error),
            {next_state, follower, State2};
        % logIndex < replIndex (leader want to cut the log)
        {_, _, _} ->
            State3 = State2#node_data{log = log:cut(Log, LogIndex)},
            {_, _, ReplTerm} = log:commited(Log),
            case ReplTerm of
                LogTerm -> send_answer(State3, ok);
                _ -> send_answer(State3, error)
            end,
            {next_state, follower, State3}
    end;
%
% Log Replication logic:
% Handle <AppendEntries> with data
follower(
    {append_entries, {LogIndex, LogTerm, Data}, LeaderTerm, LeaderRef},
    State = #node_data{log = Log, term = LocalTerm}
) when LeaderTerm >= LocalTerm ->
    State1 = reset_election_timer(follower, State),
    State2 = State1#node_data{term = LeaderTerm, leader = LeaderRef},
    case log:commited(Log) of
        {ReplIndex, _, ReplTerm} when LogIndex =:= ReplIndex andalso LogTerm =:= ReplTerm ->
            State3 = State2#node_data{log = log:push(Log, Data)},
            send_answer(State3, ok),
            {next_state, follower, State3};
        _ ->
            send_answer(State2, error),
            {next_state, follower, State2}
    end.

log_actuality(Log, OtherLogIndex, OtherLogTerm) ->
    case log:commited(Log) of
        {_, _, LogTerm} when LogTerm < OtherLogTerm ->
            less;
        {_, _, LogTerm} when LogTerm > OtherLogTerm ->
            greater;
        {LogIndex, _, _} when LogIndex < OtherLogIndex ->
            less;
        {LogIndex, _, _} when LogIndex > OtherLogIndex ->
            greater;
        {_, _, _} ->
            equal
    end.

send_answer(#node_data{log = Log, leader = Leader}, Ok) ->
    {Index, _, Term} = log:commited(Log),
    gen_fsm:send_event(Leader, {Ok, Index, Term}).

reset_election_timer(MachineState, State) ->
    Time = 149 + rand:uniform(151),
    cancel_timer(State),
    Timer = erlang:send_after(Time, self(), {tick, MachineState}),
    State#node_data{timer = Timer}.

cancel_timer(State) ->
    case State#node_data.timer of
        none -> ok;
        TimerRef -> erlang:cancel_timer(TimerRef)
    end.

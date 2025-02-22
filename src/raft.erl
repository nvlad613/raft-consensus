-module(raft).

-include("log.hrl").

-behaviour(gen_fsm).

-export([handle_event/3, handle_sync_event/4, init/1, idle/2, follower/2, candidate/2, leader/2]).

-type timer_ref() :: reference().

-type node_address() :: {identifier(), timer_ref() | none()}.

-record(node_data, {
    log :: #log{},
    term :: integer(),
    nodes :: [node_address()],
    leader :: identifier() | none(),
    timer :: timer_ref() | none(),
    broadcast_timer :: timer_ref() | none(),
    votes :: integer(),
    % Replicated last log entry nodes
    replicated_list :: [identifier()]
}).

init([Log, InitialState]) ->
    {_, _, Term} = log:commited(Log),
    {ok, InitialState, #node_data{
        log = Log,
        term = Term,
        nodes = [],
        leader = none,
        timer = none,
        broadcast_timer = none,
        votes = 0,
        replicated_list = []
    }}.

handle_event(_Req, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Req, _From, StateName, State) ->
    {noreply, StateName, State}.

idle({add_node, node_name}, State) ->
    Nodes = State#node_data.nodes,
    {next_state, idle, State#node_data{nodes = [{node_name, none} | Nodes]}};
idle(start, State) ->
    State1 = reset_election_timer(follower, State),
    {next_state, follower, State1}.

% Election logic:
% Election timer expired, so become a candidate
follower({tick, follower}, State) ->
    {next_state, candidate, State, 0};
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
    {request_vote, {LogIndex, LogTerm}, LeaderTerm, LeaderRef, Label},
    State = #node_data{log = Log}
) ->
    case log_actuality(Log, LogIndex, LogTerm) of
        LessOrEq when LessOrEq == less orelse LessOrEq == equal ->
            State1 = reset_election_timer(follower, State),
            State2 = State1#node_data{term = LeaderTerm, leader = LeaderRef},
            request_vote_response(State2, Label),
            {next_state, follower, State2};
        _ ->
            {next_state, follower, State}
    end;
%
% Log Replication logic:
% Handle empty <AppendEntries>
follower(
    {append_entries, {LogIndex, LogTerm}, LeaderTerm, LeaderRef, FollowerRef},
    State = #node_data{log = Log, term = LocalTerm}
) when LeaderTerm >= LocalTerm ->
    State1 = reset_election_timer(follower, State),
    State2 = State1#node_data{term = LeaderTerm, leader = LeaderRef},
    UncommitedIndex = log:size(Log) - 1,
    case log:commited(Log) of
        % logIndex == replIndex, logTerm == replTerm
        {ReplIndex, _, ReplTerm} when LogIndex =:= ReplIndex andalso LogTerm =:= ReplTerm ->
            append_entries_response(State2, ok, FollowerRef),
            {next_state, follower, State2};
        % logIndex == replIndex, logTerm /= replTerm
        {ReplIndex, _, _} when LogIndex =:= ReplIndex ->
            append_entries_response(State2, error, FollowerRef),
            {next_state, follower, State2};
        % logIndex > replIndex (leader want to commit)
        {ReplIndex, _, ReplTerm} when LogIndex > ReplIndex andalso LogIndex =< UncommitedIndex ->
            case log:at(Log, ReplIndex) of
                {_, TermToCommit} when TermToCommit =:= ReplTerm ->
                    State3 = State2#node_data{log = log:commit(Log, LogIndex)},
                    append_entries_response(State3, ok, FollowerRef),
                    {next_state, follower, State3};
                _ ->
                    append_entries_response(State2, error, FollowerRef),
                    {next_state, follower, State2}
            end;
        {ReplIndex, _, _} when LogIndex > ReplIndex ->
            append_entries_response(State2, error, FollowerRef),
            {next_state, follower, State2};
        % clear the log; return OK
        {_, _, _} when LogIndex < 0 ->
            Log1 = log:pop(Log, log:size(Log)),
            append_entries_response(State2, ok, FollowerRef),
            {next_state, follower, State2#node_data{log = Log1}};
        % logIndex < replIndex (leader want to cut the log)
        {_, _, _} ->
            Log1 = log:cut(Log, LogIndex),
            State3 = State2#node_data{log = Log1},
            {_, _, ReplTerm} = log:commited(Log1),

            case ReplTerm of
                LogTerm -> append_entries_response(State3, ok, FollowerRef);
                _ -> append_entries_response(State3, error, FollowerRef)
            end,
            {next_state, follower, State3}
    end;
%
% Log Replication logic:
% Handle <AppendEntries> with data
follower(
    {append_entries_data, {LogIndex, LogTerm}, {Data, DataTerm}, LeaderTerm, LeaderRef,
        FollowerRef},
    State = #node_data{log = Log, term = LocalTerm}
) when LeaderTerm >= LocalTerm ->
    State1 = reset_election_timer(follower, State),
    State2 = State1#node_data{term = LeaderTerm, leader = LeaderRef},
    case log:commited(Log) of
        {ReplIndex, _, ReplTerm} when LogIndex =:= ReplIndex andalso LogTerm =:= ReplTerm ->
            State3 = State2#node_data{log = log:push(Log, Data, DataTerm)},
            append_entries_response(State3, ok, FollowerRef),
            {next_state, follower, State3};
        _ ->
            append_entries_response(State2, error, FollowerRef),
            {next_state, follower, State2}
    end.

% Election logic:
% Candidate sends RequestVote to other nodes
% On enter state
candidate(timeout, State) ->
    broadcast_request_votes(State),
    State1 = reset_election_timer(candidate, State),
    State2 = reset_broadcast_timer(candidate, State1),
    {next_state, candidate, State2#node_data{votes = 1, leader = none}};
%
% On broadcast timer
candidate({broadcast_tick, candidate}, State) ->
    broadcast_request_votes(State),
    State1 = reset_election_timer(candidate, State),
    State2 = reset_broadcast_timer(candidate, State1),
    {next_state, candidate, State2};
%
% On election timer
candidate({tick, candidate}, State = #node_data{term = Term}) ->
    State1 = State#node_data{term = Term + 1},
    broadcast_request_votes(State1),
    State2 = reset_election_timer(candidate, State1),
    State3 = reset_broadcast_timer(candidate, State2),
    {next_state, candidate, State3#node_data{votes = 1}};
%
% On response vote
candidate({request_vote_response, candidate}, State = #node_data{votes = Votes, nodes = Nodes}) ->
    case Votes + 1 of
        IncVotes when IncVotes >= (length(Nodes) + 2) div 2 ->
            {next_state, leader, State#node_data{votes = 0}, 0};
        IncVotes ->
            {next_state, candidate, State#node_data{votes = IncVotes}}
    end.

% Replecation logic:
leader(timeout, State = #node_data{nodes = Nodes, log = Log, term = Term}) ->
    {LogIndex, _, LogTerm} = log:commited(Log),
    % make heartbeat requests and set up timers for next ones
    Nodes1 = lists:map(
        fun({NodeRef, Timer}) ->
            cancel_timer(Timer),
            Timer1 = erlang:send_after(30, self(), {heartbeat_tick, NodeRef}),
            NodeRef ! {append_entries, {LogIndex, LogTerm}, Term, self(), NodeRef},
            {NodeRef, Timer1}
        end,
        Nodes
    ),
    {next_state, candidate, State#node_data{nodes = Nodes1}};
%
% On broadcast timer
leader({heartbeat_tick, NodeRef}, State = #node_data{nodes = Nodes, log = Log, term = Term}) ->
    Nodes1 = update_node_timer(NodeRef, Nodes),
    {LogIndex, _, LogTerm} = log:commited(Log),
    NodeRef ! {append_entries, {LogIndex, LogTerm}, Term, self(), NodeRef},
    {next_state, leader, State#node_data{nodes = Nodes1}};
%
% On OK heartbeat response
leader(
    {append_entries_response, ok, FollowerIndex, FollowerTerm, FollowerLogSz, BackRef},
    State = #node_data{log = Log, nodes = Nodes, replicated_list = ReplicatedList, term = Term}
) ->
    {Index, _, _} = log:commited(Log),
    case log:size(Log) of
        % replicate not commited
        LogSz when FollowerIndex =:= Index andalso LogSz > FollowerLogSz ->
            Nodes1 = update_node_timer(BackRef, Nodes),
            State1 = State#node_data{nodes = Nodes1},
            {Data, DataTerm} = log:at(Log, FollowerLogSz),
            BackRef !
                {append_entries, {FollowerIndex, FollowerTerm}, {Data, DataTerm}, Term, self(),
                    BackRef},
            {next_state, leader, State1};
        % add to replicated list; check if can be commited
        LogSz when FollowerIndex =:= Index andalso LogSz =:= FollowerLogSz ->
            ReplicatedList1 = util:append_if_not_present(ReplicatedList, BackRef),
            case length(ReplicatedList1) of
                Length when Length >= (length(Nodes) + 2) div 2 ->
                    Log1 = log:commit(Log, LogSz - 1),
                    {next_state, leader, State#node_data{replicated_list = [], log = Log1}}
            end;
        % attempt to move follower index on
        _ when FollowerIndex < Index ->
            Nodes1 = update_node_timer(BackRef, Nodes),
            State1 = State#node_data{nodes = Nodes1},
            {_, DataTerm} = log:at(Log, FollowerIndex + 1),
            BackRef ! {append_entries, {FollowerIndex + 1, DataTerm}, Term, self(), BackRef},
            {next_state, leader, State1};
        _ ->
            {next_state, leader, State}
    end;
%
% On ERROR heartbeat response
leader(
    {append_entries_response, error, FollowerIndex, FollowerTerm, _, BackRef},
    State = #node_data{log = Log, nodes = Nodes, term = Term}
) ->
    {Index, _, LogTerm} = log:commited(Log),
    Nodes1 = update_node_timer(BackRef, Nodes),
    State1 = State#node_data{nodes = Nodes1},
    case FollowerIndex > Index of
        true ->
            BackRef ! {append_entries, {Index, LogTerm}, Term, self(), BackRef},
            {next_state, leader, State1};
        false ->
            case log:at(Log, FollowerIndex) of
                % replicate commited
                {_, TermAtFollowerIndex} when
                    FollowerIndex < Index andalso TermAtFollowerIndex =:= FollowerTerm
                ->
                    DataAndTerm = log:at(Log, FollowerIndex + 1),
                    BackRef !
                        {
                            append_entries_data,
                            {FollowerIndex, FollowerTerm},
                            DataAndTerm,
                            Term,
                            self(),
                            BackRef
                        },
                    {next_state, leader, State1};
                % move follower index back
                {_, TermAtFollowerIndex} when TermAtFollowerIndex /= FollowerTerm ->
                    BackRef !
                        {
                            append_entries,
                            {FollowerIndex - 1, FollowerTerm},
                            Term,
                            self(),
                            BackRef
                        },
                    {next_state, leader, State1}
            end
    end;
% Client add new data
leader({client_add_data, Data}, State = #node_data{log = Log, term = Term}) ->
    Log1 = log:push(Log, Data, Term),
    {next_state, leader, State#node_data{log = Log1, replicated_list = []}}.

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

update_node_timer(NodeRef, Nodes) ->
    lists:filtermap(
        fun
            ({CurrentNodeRef, Timer}) when CurrentNodeRef =:= NodeRef ->
                cancel_timer(Timer),
                Timer1 = erlang:send_after(30, self(), {heartbeat_tick, NodeRef}),
                {true, {CurrentNodeRef, Timer1}};
            (NodeAddress) ->
                {false, NodeAddress}
        end,
        Nodes
    ).

broadcast_request_votes(#node_data{log = Log, term = Term, nodes = Nodes}) ->
    {LogIndex, _, LogTerm} = log:commited(Log),
    lists:foreach(
        fun({NodeRef, _}) ->
            NodeRef ! {request_vote, {LogIndex, LogTerm}, Term, self(), NodeRef}
        end,
        Nodes
    ).

append_entries_response(#node_data{log = Log, leader = Leader}, Status, Label) ->
    {Index, _, Term} = log:commited(Log),
    gen_fsm:send_event(
        Leader,
        {append_entries_response, Status, Index, Term, log:size(Log), Label}
    ).

request_vote_response(#node_data{leader = Leader}, Label) ->
    gen_fsm:send_event(Leader, {request_vote_response, Label}).

reset_election_timer(MachineState, State) ->
    Time = 149 + rand:uniform(151),
    cancel_timer(State#node_data.timer),
    Timer = erlang:send_after(Time, self(), {tick, MachineState}),
    State#node_data{timer = Timer}.

reset_broadcast_timer(MachineState, State) ->
    cancel_timer(State#node_data.broadcast_timer),
    Timer = erlang:send_after(30, self(), {broadcast_tick, MachineState}),
    State#node_data{broadcast_timer = Timer}.

cancel_timer(Timer) ->
    case Timer of
        none -> ok;
        TimerRef -> erlang:cancel_timer(TimerRef)
    end.

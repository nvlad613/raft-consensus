-module(raft).

-compile([
    {nowarn_deprecated_function, [
        {gen_fsm, send_event, 2},
        {gen_fsm, sync_send_event, 2},
        {gen_fsm, send_event_after, 2},
        {gen_fsm, cancel_timer, 1}
    ]}
]).

-behaviour(gen_fsm).

-export([handle_event/3, handle_sync_event/4, init/1, idle/2, follower/2, candidate/2, leader/2]).

-include("log.hrl").
-type timer_ref() :: reference().
-type node_address() :: {identifier(), timer_ref() | none()}.

-record(node_data, {
    % DEBUG ONLY
    self_name :: atom(),

    log :: #log{},
    snapshot_creator :: identifier(),
    term :: integer(),
    nodes :: [node_address()],
    leader :: identifier() | none(),
    timer :: timer_ref() | none(),
    broadcast_timer :: timer_ref() | none(),
    votes :: integer(),
    % Replicated last log entry nodes
    replicated_list :: [identifier()]
}).

init([Log, InitialState, SnapshotCreatorRef]) ->
    {_, _, Term} = log:commited(Log),
    {ok, InitialState, #node_data{
        log = Log,
        snapshot_creator = SnapshotCreatorRef,
        term = Term,
        nodes = [],
        leader = none,
        timer = none,
        broadcast_timer = none,
        votes = 0,
        replicated_list = []
    }}.

idle({add_node, NodeRef}, State) ->
    Nodes = State#node_data.nodes,
    {next_state, idle, State#node_data{nodes = [{NodeRef, none} | Nodes]}};
idle(start, State) ->
    SelfName =
        case process_info(self(), registered_name) of
            {registered_name, Name} -> Name;
            _ -> undefined
        end,
    logger:debug("[~p] Started on ~p", [SelfName, self()]),
    State1 = reset_election_timer(follower, State),
    {next_state, follower, State1#node_data{self_name = SelfName}}.

% Election logic:
% Election timer expired, so become a candidate
follower({tick, follower}, State) ->
    {next_state, candidate, State, 0};
%
% Election logic:
% Handle <RequestVote>
follower(
    {request_vote, _, LeaderTerm, _, _},
    State = #node_data{term = LocalTerm}
) when LeaderTerm =< LocalTerm ->
    logger:debug("[~p] ignoring request vote with term ~p", [State#node_data.self_name, LeaderTerm]),
    {next_state, follower, State};
follower(
    {request_vote, {LogIndex, LogTerm}, LeaderTerm, LeaderRef, FollowerRef},
    State = #node_data{log = Log}
) ->
    logger:debug("[~p] get request vote from {~p}", [FollowerRef, LeaderRef]),
    case log_actuality(Log, LogIndex, LogTerm) of
        LessOrEq when LessOrEq == less orelse LessOrEq == equal ->
            logger:debug("[~p] {~p} now is a leader", [FollowerRef, LeaderRef]),
            State1 = reset_election_timer(follower, State),
            State2 = State1#node_data{term = LeaderTerm, leader = LeaderRef},
            request_vote_response(State2, FollowerRef),
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
    logger:debug("[~p] {~p, ~p} append entries from [~p]", [
        FollowerRef, LogIndex, LogTerm, LeaderRef
    ]),
    logger:debug("[~p] STATE : ~p", [FollowerRef, log:debug(Log)]),
    State1 = reset_election_timer(follower, State),
    State2 = State1#node_data{term = LeaderTerm, leader = LeaderRef},
    UncommitedIndex = log:size(Log) - 1,
    case log:commited(Log) of
        % logIndex == replIndex, logTerm == replTerm
        {ReplIndex, _, ReplTerm} when LogIndex =:= ReplIndex andalso LogTerm =:= ReplTerm ->
            logger:debug("[~p] OK: logIndex = replIndex, logTerm = replTerm", [
                State#node_data.self_name
            ]),
            append_entries_response(State2, ok, FollowerRef),
            {next_state, follower, State2};
        % logIndex == replIndex, logTerm /= replTerm
        {ReplIndex, _, _} when LogIndex =:= ReplIndex ->
            logger:debug("[~p] ERROR: logIndex = replIndex, logTerm /= replTerm", [
                State#node_data.self_name
            ]),
            append_entries_response(State2, error, FollowerRef),
            {next_state, follower, State2};
        % logIndex > replIndex (leader want to commit)
        {ReplIndex, _, ReplTerm} when LogIndex > ReplIndex andalso LogIndex =< UncommitedIndex ->
            case log:at(Log, ReplIndex) of
                {_, TermToCommit} when TermToCommit =:= ReplTerm ->
                    logger:debug("[~p] OK: logIndex > replIndex, logTerm = replTerm", [
                        State#node_data.self_name
                    ]),
                    {ok, Log1} = log:commit(Log, LogIndex),
                    State3 = State2#node_data{log = Log1},
                    append_entries_response(State3, ok, FollowerRef),
                    {next_state, follower, State3};
                none ->
                    logger:debug("[~p] OK: commit to an empty replicated log", [
                        State#node_data.self_name
                    ]),
                    {ok, Log1} = log:commit(Log, LogIndex),
                    State3 = State2#node_data{log = Log1},
                    append_entries_response(State3, ok, FollowerRef),
                    {next_state, follower, State3};
                {_, TermToCommit} ->
                    logger:debug(
                        "[~p] ERROR: logIndex > replIndex, logTerm {~p} /= replTerm {~p}", [
                            State#node_data.self_name, TermToCommit, ReplTerm
                        ]
                    ),
                    append_entries_response(State2, error, FollowerRef),
                    {next_state, follower, State2}
            end;
        {ReplIndex, _, _} when LogIndex > ReplIndex ->
            logger:debug("[~p] ERROR: logIndex > replIndex, nothing to commit", [
                State#node_data.self_name
            ]),
            append_entries_response(State2, error, FollowerRef),
            {next_state, follower, State2};
        % clear the log; return OK
        {_, _, _} when LogIndex < 0 ->
            logger:debug("[~p] OK: clear the log", [State#node_data.self_name]),
            Log1 = log:pop(Log, log:size(Log)),
            append_entries_response(State2, ok, FollowerRef),
            {next_state, follower, State2#node_data{log = Log1}};
        % logIndex < replIndex (leader want to cut the log)
        {_, _, _} ->
            Log1 = log:cut(Log, LogIndex),
            State3 = State2#node_data{log = Log1},
            {_, _, ReplTerm} = log:commited(Log1),

            case ReplTerm of
                LogTerm ->
                    logger:debug("[~p] OK: cut the log to index {~p}", [
                        State#node_data.self_name, LogIndex
                    ]),
                    append_entries_response(State3, ok, FollowerRef);
                _ ->
                    logger:debug("[~p] ERROR: cut the log to index {~p}, wrong term number", [
                        State#node_data.self_name, LogIndex
                    ]),
                    append_entries_response(State3, error, FollowerRef)
            end,
            {next_state, follower, State3}
    end;
%
% Log Replication logic:
% Handle <AppendEntries> with data
follower(
    {append_entries, {LogIndex, LogTerm}, {Data, DataTerm}, LeaderTerm, LeaderRef, FollowerRef},
    State = #node_data{log = Log, term = LocalTerm}
) when LeaderTerm >= LocalTerm ->
    logger:debug("[~p] filled append entries from {~p}", [FollowerRef, LeaderRef]),
    logger:debug("[~p] STATE : ~p", [FollowerRef, log:debug(Log)]),
    State1 = reset_election_timer(follower, State),
    State2 = State1#node_data{term = LeaderTerm, leader = LeaderRef},
    case log:commited(Log) of
        {ReplIndex, _, ReplTerm} when LogIndex =:= ReplIndex andalso LogTerm =:= ReplTerm ->
            logger:debug("[~p] OK: {~p, ~p} pushed to log", [FollowerRef, Data, DataTerm]),
            State3 = State2#node_data{log = log:push(Log, Data, DataTerm)},
            append_entries_response(State3, ok, FollowerRef),
            {next_state, follower, State3};
        {ReplIndex, _, ReplTerm} ->
            logger:debug("[~p] ERROR: {~p, ~p} != {~p, ~p}", [
                FollowerRef, LogIndex, LocalTerm, ReplIndex, ReplTerm
            ]),
            append_entries_response(State2, error, FollowerRef),
            {next_state, follower, State2}
    end;
%
% Ignore timers started before entering current state
follower({tick, _}, State) ->
    {next_state, follower, State};
follower({broadcast_tick, _}, State) ->
    {next_state, follower, State};
follower({heartbeat_tick, _}, State) ->
    {next_state, follower, State}.

% Election logic:
% Candidate sends RequestVote to other nodes
% On enter state
candidate(timeout, State = #node_data{term = Term}) ->
    logger:debug("[~p] now is a candidate; term=~p", [State#node_data.self_name, Term + 1]),
    State1 = State#node_data{term = Term + 1, votes = 1, leader = none},
    broadcast_request_votes(State1),
    State2 = reset_election_timer(candidate, State1),
    State3 = reset_broadcast_timer(candidate, State2),
    {next_state, candidate, State3};
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
    logger:debug("[~p] now is a candidate again; term=~p", [State#node_data.self_name, Term + 1]),
    State1 = State#node_data{term = Term + 1, votes = 1, leader = none},
    broadcast_request_votes(State1),
    State2 = reset_election_timer(candidate, State1),
    State3 = reset_broadcast_timer(candidate, State2),
    {next_state, candidate, State3};
%
% On response vote
candidate({request_vote_response, _}, State = #node_data{votes = Votes, nodes = Nodes}) ->
    case Votes + 1 of
        IncVotes when IncVotes >= (length(Nodes) + 2) div 2 ->
            {next_state, leader, State#node_data{votes = 0}, 0};
        IncVotes ->
            {next_state, candidate, State#node_data{votes = IncVotes}}
    end;
%
% Ignore request vote
candidate({request_vote, _, _, _, _}, State) ->
    {next_state, candidate, State};
%
% Ignore timers started before entering current state
candidate({tick, _}, State) ->
    {next_state, candidate, State};
candidate({broadcast_tick, _}, State) ->
    {next_state, candidate, State};
candidate({heartbeat_tick, _}, State) ->
    {next_state, candidate, State}.

% Replecation logic:
% On enter leader state
leader(timeout, State = #node_data{nodes = Nodes, log = Log, term = Term}) ->
    {LogIndex, _, LogTerm} = log:commited(Log),
    logger:debug(
        "[~p] become a leader term=~p index=~p", [State#node_data.self_name, Term, LogIndex]
    ),
    % make heartbeat requests and set up timers for next ones
    Nodes1 = lists:map(
        fun({NodeRef, Timer}) ->
            cancel_timer(Timer),
            Timer1 = gen_fsm:send_event_after(30, {heartbeat_tick, NodeRef}),
            gen_fsm:send_event(
                NodeRef,
                {append_entries, {LogIndex, LogTerm}, Term, self(), NodeRef}
            ),
            logger:debug("[~p] append entries request for {~p}", [
                State#node_data.self_name, NodeRef
            ]),
            {NodeRef, Timer1}
        end,
        Nodes
    ),
    {next_state, leader, State#node_data{nodes = Nodes1}};
%
% On broadcast timer
leader({heartbeat_tick, NodeRef}, State = #node_data{nodes = Nodes, log = Log, term = Term}) ->
    Nodes1 = update_node_timer(NodeRef, Nodes),
    {LogIndex, _, LogTerm} = log:commited(Log),
    gen_fsm:send_event(
        NodeRef,
        {append_entries, {LogIndex, LogTerm}, Term, self(), NodeRef}
    ),
    {next_state, leader, State#node_data{nodes = Nodes1}};
%
% On OK heartbeat response
leader(
    {append_entries_response, ok, FollowerIndex, FollowerTerm, FollowerLogSz, BackRef},
    State = #node_data{log = Log, nodes = Nodes, replicated_list = ReplicatedList, term = Term}
) ->
    {Index, _, _} = log:commited(Log),
    logger:debug("[~p] get OK heartbeat response from {~p}", [State#node_data.self_name, BackRef]),
    case log:size(Log) of
        % replicate not commited
        LogSz when FollowerIndex =:= Index andalso LogSz > FollowerLogSz ->
            logger:debug("[~p] replicate not commited", [State#node_data.self_name]),
            Nodes1 = update_node_timer(BackRef, Nodes),
            State1 = State#node_data{nodes = Nodes1},
            {Data, DataTerm} = log:at(Log, FollowerLogSz),
            gen_fsm:send_event(
                BackRef,
                {append_entries, {FollowerIndex, FollowerTerm}, {Data, DataTerm}, Term, self(),
                    BackRef}
            ),
            {next_state, leader, State1};
        % count replications, commit if necessary
        LogSz when
            Index < LogSz - 1 andalso FollowerIndex =:= Index andalso LogSz =:= FollowerLogSz
        ->
            ReplicatedList1 = util:append_if_not_present(ReplicatedList, BackRef),
            case length(ReplicatedList1) of
                Length when Length >= (length(Nodes) + 2) div 2 ->
                    {ok, Log1} = log:commit(Log, LogSz - 1),
                    logger:debug("[~p] commit at index ~p; LOG: ~p", [
                        State#node_data.self_name, LogSz - 1, log:debug(Log1)
                    ]),
                    {next_state, leader, State#node_data{replicated_list = [], log = Log1}}
            end;
        % attempt to move follower index on
        _ when FollowerIndex < Index ->
            logger:debug("[~p] move follower index on {~p} -> {~p}", [
                State#node_data.self_name, FollowerIndex, FollowerIndex + 1
            ]),
            Nodes1 = update_node_timer(BackRef, Nodes),
            State1 = State#node_data{nodes = Nodes1},
            {_, DataTerm} = log:at(Log, FollowerIndex + 1),
            gen_fsm:send_event(
                BackRef,
                {append_entries, {FollowerIndex + 1, DataTerm}, Term, self(), BackRef}
            ),
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
            % cut follower log
            logger:debug("[~p] cut follower log", [State#node_data.self_name]),
            gen_fsm:send_event(
                BackRef,
                {append_entries, {Index, LogTerm}, Term, self(), BackRef}
            ),
            {next_state, leader, State1};
        false ->
            case log:at(Log, FollowerIndex) of
                % replicate commited
                {_, TermAtFollowerIndex} when
                    FollowerIndex < Index andalso TermAtFollowerIndex =:= FollowerTerm
                ->
                    logger:debug("[~p] replicate commited", [State#node_data.self_name]),
                    DataAndTerm = log:at(Log, FollowerIndex + 1),
                    gen_fsm:send_event(
                        BackRef,
                        {
                            append_entries,
                            {FollowerIndex, FollowerTerm},
                            DataAndTerm,
                            Term,
                            self(),
                            BackRef
                        }
                    ),
                    {next_state, leader, State1};
                % move follower index back
                {_, TermAtFollowerIndex} when TermAtFollowerIndex /= FollowerTerm ->
                    logger:debug("[~p] move follower index back", [State#node_data.self_name]),
                    gen_fsm:send_event(
                        BackRef,
                        {
                            append_entries,
                            {FollowerIndex - 1, FollowerTerm},
                            Term,
                            self(),
                            BackRef
                        }
                    ),
                    {next_state, leader, State1}
            end
    end;
%
% Client add new data
leader({add_data, Data}, State = #node_data{log = Log, term = Term}) ->
    Log1 = log:push(Log, Data, Term),
    {next_state, leader, State#node_data{log = Log1, replicated_list = []}};
%
% Ignore timers started before entering current state
leader({tick, _}, State) ->
    {next_state, leader, State};
leader({broadcast_tick, _}, State) ->
    {next_state, leader, State}.

% response with log snapshot
handle_sync_event(
    log_snapshot,
    _From,
    StateName,
    State = #node_data{snapshot_creator = SnapshotCreatorRef, log = Log}
) ->
    Snapshot = gen_server:call(SnapshotCreatorRef, {make_snapshot, Log, log:size(Log) - 1}),
    {reply, Snapshot, StateName, State}.

handle_event({client_add_data, Data}, StateName, State = #node_data{leader = Leader}) ->
    case Leader of
        none ->
            gen_fsm:send_event(self(), {add_data, Data});
        _ ->
            gen_fsm:send_event(Leader, {add_data, Data})
    end,
    {next_state, StateName, State}.

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
                Timer1 = gen_fsm:send_event_after(30, {heartbeat_tick, NodeRef}),
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
            gen_fsm:send_event(
                NodeRef,
                {request_vote, {LogIndex, LogTerm}, Term, self(), NodeRef}
            )
        end,
        Nodes
    ).

append_entries_response(#node_data{log = Log, leader = Leader}, Status, Label) ->
    {Index, _, Term} = log:commited(Log),
    gen_fsm:send_event(
        Leader,
        {
            append_entries_response,
            Status,
            Index,
            Term,
            log:size(Log),
            Label
        }
    ).

request_vote_response(#node_data{leader = Leader}, Label) ->
    gen_fsm:send_event(Leader, {request_vote_response, Label}).

reset_election_timer(MachineState, State) ->
    Time = 149 + rand:uniform(151),
    cancel_timer(State#node_data.timer),
    Timer = gen_fsm:send_event_after(Time, {tick, MachineState}),
    State#node_data{timer = Timer}.

reset_broadcast_timer(MachineState, State) ->
    cancel_timer(State#node_data.broadcast_timer),
    Timer = gen_fsm:send_event_after(30, {broadcast_tick, MachineState}),
    State#node_data{broadcast_timer = Timer}.

cancel_timer(Timer) ->
    case Timer of
        none -> ok;
        TimerRef -> gen_fsm:cancel_timer(TimerRef)
    end.

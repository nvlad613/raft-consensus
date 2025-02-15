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

% Election logic
%
% Election timer expired, so become a candidate
follower({tick, follower}, State) ->
    {next_state, candidate, State};
follower({tick, _}, State) ->
    {next_state, follower, State};
%
% Log Replication logic
%
% Handle empty AppendEntries, return to leader whether log is consistent on the node
follower(
    {append_entries, LeaderIndex, LeaderTerm},
    State = #node_data{log = Log, leader = Leader, term = Term}
) ->
    State1 = reset_election_timer(follower, State),
    IsLogConsistent = is_log_consistent(LeaderIndex, LeaderTerm, Log),
    gen_fsm:send_event(Leader, {IsLogConsistent, log:index(Log), Term}),
    {next_state, follower, State1#node_data{term = LeaderTerm}}.
%
% Handle AppendEntries with data
follower(
    {append_entries, LeaderIndex, LeaderTerm, Data},
    State = #node_data{log = Log, leader = Leader, term = Term}
) ->
    State1 = reset_election_timer(follower, State),
    case is_log_consistent(LeaderIndex, LeaderTerm, Log) of 
        ok -> 
            State2 = State1#node_data{log = log:push(Log, Data, Term)},
            gen_fsm:send_event(Leader, {ok, log:index(Log), Term}),
            {next_state, follower, State2#node_data{term = LeaderTerm}};
        error ->


is_log_consistent(LeaderIndex, _LeaderTerm, Log) when Log#log.size =/= LeaderIndex + 1 ->
    error;
is_log_consistent(_LeaderIndex, LeaderTerm, Log) ->
    {_, Term} = log:last(Log),
    case Term of
        LeaderTerm -> ok;
        _ -> error
    end.

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

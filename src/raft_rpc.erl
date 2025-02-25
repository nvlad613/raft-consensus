-module(raft_rpc).

-compile([
    {nowarn_deprecated_function, [
        {gen_fsm, send_event, 2},
        {gen_fsm, send_all_state_event, 2},
        {gen_fsm, sync_send_all_state_event, 2},
        {gen_fsm, start_link, 4}
    ]}
]).

-export([
    add_node/2,
    start_follower/1,
    send_data/2,
    start_node/2,
    init_cluster/1, init_cluster/2,
    make_snapshot/1,
    make_snapshot/2
]).

start_node(NodeName, SnapshotServerRef) ->
    case whereis(NodeName) of
        undefined ->
            gen_fsm:start_link(
                {local, NodeName}, raft, [log:new(), idle, SnapshotServerRef], []
            );
        Pid ->
            {error, {already_started, Pid}}
    end.
add_node(TargetNode, NodeToAdd) ->
    gen_fsm:send_event(TargetNode, {add_node, NodeToAdd}).

start_follower(NodeRef) ->
    gen_fsm:send_event(NodeRef, start).

send_data(NodeRef, Data) ->
    gen_fsm:send_all_state_event(NodeRef, {client_add_data, Data}).

make_snapshot(NodeRef) ->
    gen_fsm:sync_send_all_state_event(NodeRef, log_snapshot).

make_snapshot(NodeRef, Provider) ->
    gen_fsm:sync_send_all_state_event(NodeRef, {log_snapshot, Provider}).

init_cluster(Nodes) ->
    init_cluster(Nodes, concat_str_provider).
init_cluster(Nodes, SnapshotProdiver) ->
    util:set_up_logger(),
    gen_server:start_link({local, SnapshotProdiver}, SnapshotProdiver, [], []),

    lists:foreach(fun(Node) -> start_node(Node, SnapshotProdiver) end, Nodes),
    lists:foreach(
        fun(TargetNode) ->
            Others = lists:delete(TargetNode, Nodes),
            lists:foreach(
                fun(NodeToAdd) ->
                    add_node(TargetNode, NodeToAdd)
                end,
                Others
            )
        end,
        Nodes
    ),

    lists:foreach(fun(Node) -> start_follower(Node) end, Nodes).

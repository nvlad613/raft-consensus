-module(raft_rpc).
-export([add_node/2, start_follower/1, send_data/2, start_node/1, init_cluster/1]).

start_node(NodeName) ->
    case whereis(NodeName) of
        undefined ->
            gen_fsm:start_link({local, NodeName}, raft, [log:new(), idle], []);
        Pid ->
            {error, {already_started, Pid}}
    end.
add_node(TargetNode, NodeToAdd) ->
    gen_fsm:send_event(TargetNode, {add_node, NodeToAdd}).

start_follower(NodeRef) ->
    gen_fsm:send_event(NodeRef, start).

send_data(NodeRef, Data) ->
    gen_fsm:send_event(NodeRef, {client_add_data, Data}).

init_cluster(Nodes) ->
    util:set_up_logger(),
    lists:foreach(fun(Node) -> start_node(Node) end, Nodes),

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

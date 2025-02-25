-include("log.hrl").

-define(ELECTION_TIMEOUT, 150).
-define(HEARTBEAT_PERIOD, 30).

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

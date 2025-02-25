-module(tests).

-include_lib("eunit/include/eunit.hrl").

concat_str_test() ->
    raft_rpc:init_cluster([concat0, concat1], concat_str_provider),
    timer:sleep(300),

    raft_rpc:send_data(concat0, "Hello"),
    timer:sleep(100),
    raft_rpc:send_data(concat1, " "),
    timer:sleep(100),
    raft_rpc:send_data(concat1, "world"),
    timer:sleep(100),
    raft_rpc:send_data(concat0, "!"),
    timer:sleep(100),

    {Status0, Snapshot0} = raft_rpc:make_snapshot(concat0),
    ?assertEqual(ok, Status0),
    ?assertEqual("Hello world!", Snapshot0),

    {Status1, Snapshot1} = raft_rpc:make_snapshot(concat1),
    ?assertEqual(ok, Status1),
    ?assertEqual("Hello world!", Snapshot1).

counter_test() ->
    raft_rpc:init_cluster([counter0, counter1], counter_provider),
    timer:sleep(300),

    raft_rpc:send_data(counter0, inc),
    raft_rpc:send_data(counter1, inc),
    raft_rpc:send_data(counter1, inc),
    raft_rpc:send_data(counter0, dec),
    timer:sleep(100),

    {Status0, Snapshot0} = raft_rpc:make_snapshot(counter0),
    ?assertEqual(ok, Status0),
    ?assertEqual(2, Snapshot0),

    {Status1, Snapshot1} = raft_rpc:make_snapshot(counter1),
    ?assertEqual(ok, Status1),
    ?assertEqual(2, Snapshot1).

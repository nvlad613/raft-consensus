-module(util).

-export([append_if_not_present/2, set_up_logger/0]).

append_if_not_present(List, Item) ->
    case lists:member(Item, List) of
        true -> List;
        false -> [Item, List]
    end.

set_up_logger() ->
    Config = #{config => #{file => "logs/app.log"}, level => debug},
    logger:add_handler(to_file_handler, logger_std_h, Config).

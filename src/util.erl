-module(util).

-export([append_if_not_present/2, set_up_logger/0]).

append_if_not_present(List, Item) ->
    case lists:member(Item, List) of
        true -> List;
        false -> [Item, List]
    end.

set_up_logger() ->
    logger:remove_handler(default),
    logger:set_primary_config(level, debug),
    logger:add_handler(console_handler, logger_std_h, #{
        level => debug,
        formatter => {logger_formatter, #{
            template => [time, " ", level, ": ", msg, "\n"]
        }},
        config => #{type => standard_io}
    }),
    logger:add_handler(file_handler, logger_std_h, #{
        level => debug,
        formatter => {logger_formatter, #{
            template => [time, " ", level, ": ", msg, "\n"]
        }},
        config => #{type => file, file => "logs/app.log"}
    }).

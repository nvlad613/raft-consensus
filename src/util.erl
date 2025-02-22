-module(util).

-export([append_if_not_present/2]).

append_if_not_present(List, Item) ->
    case lists:member(Item, List) of
        true -> List;
        false -> [Item, List]
    end.

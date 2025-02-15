-module(log).

-include("log.hrl").

-export([new/0, size/1, last/1, at/2, pop/2, push/3, commited/1, commit/2]).

new() -> #log{commited = ?COMMITED_NONE, items = []}.

size(#log{items = Items}) -> length(Items).

last(#log{items = []}) -> none;
last(#log{items = [Last | _]}) -> Last.

at(#log{items = Items}, Index) when Index >= 0 andalso Index < length(Items) ->
    lists:nth(length(Items) - Index, Items);
at(_, _) ->
    none.

pop(#log{commited = Commited, items = Items}, Count) ->
    #log{commited = Commited, items = lists:nthtail(Count, Items)}.

push(#log{commited = Commited, items = Items}, Data, Term) ->
    #log{commited = Commited, items = [{Data, Term} | Items]}.

commited(#log{commited = Commited, items = Items}) ->
    case at(#log{items = Items}, Commited) of
        none -> none;
        {Data, Term} -> {Commited, Data, Term}
    end.

commit(#log{items = Items}, Index) when Index >= 0 andalso Index < length(Items) ->
    {ok, #log{commited = Index, items = Items}};
commit(Log, _) ->
    {err, Log}.

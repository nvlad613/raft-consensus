-module(log).

-include("log.hrl").

-export([new/0, size/1, last/1, pop/2]).

new() -> #log{}.

size(Log) -> 0.

last(Log) -> {data, term}.

at(Log, Index) -> {data, term}.

pop(Log, _Count) -> Log.

push(Log, Data, Term) -> Log.

commited(Log) -> {index, data, term}.

commit(Log, _Index) -> Log.

-module(log).

-include("log.hrl").

-export([new/0, size/1, last/1, pop/2, commited/1, commit/2, cut/2, at/2, push/3]).

new() -> #log{}.

size(Log) -> 0.

last(Log) -> {data, term}.

at(Log, Index) -> {data, term}.

pop(Log, _Count) -> Log.

cut(Log, _Index) -> Log.

push(Log, Data, Term) -> Log.

commited(Log) -> {index, data, term}.

commit(Log, _Index) -> Log.

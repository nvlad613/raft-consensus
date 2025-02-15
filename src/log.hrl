-record(log, {
    commited :: integer(),
    items = [] :: [{tuple(), integer()}]
}).

-define(COMMITED_NONE, -1).

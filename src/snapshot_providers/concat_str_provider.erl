-module(concat_str_provider).
-behaviour(gen_server).

-include("./log.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% API Functions
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% gen_server Implementation
init([]) ->
    {ok, #{}}.

handle_call({make_snapshot, Log, Index}, _From, State) ->
    MaxIndex = log:size(Log) - 1,
    Response =
        if
            Index < 0 orelse Index > MaxIndex ->
                {error, invalid_index};
            true ->
                Log1 = log:cut(Log, Index),
                Result = lists:foldr(
                    fun(Elem, Acc) ->
                        case Elem of
                            {Data, _Term} when is_list(Data) -> Acc ++ Data;
                            _ -> Acc
                        end
                    end,
                    "",
                    Log1#log.items
                ),
                {ok, Result}
        end,
    {reply, Response, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

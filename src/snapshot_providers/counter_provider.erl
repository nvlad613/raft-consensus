-module(counter_provider).
-behaviour(gen_server).

-include("./log.hrl").

%% API
-export([start_link/0]).

%% gen_server callbacks
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
                %% Собираем элементы до указанного индекса
                Log1 = log:cut(Log, Index),
                %% Фильтруем и конкатенируем строки
                Result = lists:foldr(
                    fun(Elem, Acc) ->
                        logger:debug("~p ~i", [Elem, Acc]),
                        case Elem of
                            {Data, _Term} ->
                                case Data of
                                    inc -> Acc + 1;
                                    dec -> Acc - 1;
                                    _ -> Acc
                                end;
                            _ -> Acc
                        end
                    end,
                    0,
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

-module(ekaf_server_lib).

%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-include("ekaf_definitions.hrl").

%%--------------------------------------------------------------------
%% External exports
%%--------------------------------------------------------------------
-export([
         handle_connected/2,
         handle_metadata_during_bootstrapping/2
        ]).

-export([
         handle_pick/3,
         reconnect_attempt/0,
         save_messages/3,
         send_messages/3,
         reply_to_prepares/2
        ]).
%%====================================================================
%% State Changes
%%====================================================================

handle_connected(#ekaf_server{ topic = Topic } = State, Socket)->
    CorId = State#ekaf_server.cor_id + 1,
    Self = self(),
    Request = ekaf_protocol:encode_metadata_request(CorId, "ekaf", [Topic]),
    ekaf_socket:fork(Self, Socket, {send, metadata, Request}).

handle_metadata_during_bootstrapping({metadata, resp, Metadata}, #ekaf_server{ topic = Topic} = State)->
    Self = self(),
    BrokersDict = lists:foldl(fun(Broker,Dict)->
                                       dict:append(Broker#broker.node_id,
                                                  Broker,
                                                  Dict)
                              end, dict:new(), Metadata#metadata_response.brokers),
    _Started = lists:foldl(
                fun(#topic{ name = undefined }, TopicsAcc)->
                        %% topic is undefined, so maybe new topic? try again
                        gen_fsm:send_event(Self, connect),
                        TopicsAcc;
                   (#topic{ name = CurrTopicName } = CurrTopic,TopicsAcc) when CurrTopicName =:= Topic ->
                        %% topic will have CurrTopic#topic.partitions
                        TempStarted =
                            [ begin
                                  Leader = Partition#partition.leader,
                                  PartitionId = Partition#partition.id,
                                  case dict:find(Leader, BrokersDict) of
                                      {ok,[Broker]} ->
                                          ekaf_lib:start_child(Metadata, Broker, CurrTopic, Leader, PartitionId);
                                      _ ->
                                          ?INFO_MSG("cant find broker ~p in metadata for partition ~p",[Leader, Partition]),
                                          ok
                                  end
                              end
                              || Partition <- CurrTopic#topic.partitions ],
                        [TempStarted|TopicsAcc];
                    (TopicsAcc,_OtherTopic)->
                        TopicsAcc
                end, [], Metadata#metadata_response.topics),
    State#ekaf_server{ metadata = Metadata }.

%%--------------------------------------------------------------------
%%% External functions
%%--------------------------------------------------------------------

handle_pick({pick, Topic, _Callback}, _From, State)->
    case ekaf_picker:pick(Topic, undefined, sync, State#ekaf_server.strategy) of
        Pid when is_pid(Pid)->
            {Pid, State#ekaf_server{ worker = Pid}};
        {error, {no_such_group,_}} ->
            ekaf:prepare(Topic),
            {{error, picking}, State};
        _E ->
            ?INFO_MSG("error handle_pick error: ~p", [_E]),
            {{error, bootstrapping}, State}
    end;
handle_pick(Pick, _From, State) ->
    Error = {error, {handle_pick_error,Pick}},
    {Error, State}.

reconnect_attempt()->
    gen_fsm:start_timer(1000,<<"reconnect">>).

save_messages(StateName, #ekaf_server{ messages = OfflineMessages, worker = Worker,
                                       max_downtime_buffer_size = MaxDowntimeBufferSize } = State, Messages)->
    case StateName of
        ready when Worker =/= self() ->
            send_messages(StateName, State, Messages),
            fsm_next_state(StateName, State);
        _ ->
            Self = self(),
            case ekaf_callbacks:find(?EKAF_CALLBACK_DOWNTIME_SAVED) of
                {Mod,Func} ->
                    Mod:Func(?EKAF_CALLBACK_DOWNTIME_SAVED, Self, StateName, State, {ok, Messages});
                _ ->
                    ok
            end,
            NextMessages = save_messages_until(StateName, State, Messages, OfflineMessages, MaxDowntimeBufferSize),
            fsm_next_state(StateName, State#ekaf_server{ messages = NextMessages } )
    end.

send_messages(StateName, #ekaf_server{ topic = Topic } = State, Messages)->
    case Messages of
        [] ->
            ok;
        _ ->
            Self = self(),
            case ekaf_callbacks:find(?EKAF_CALLBACK_DOWNTIME_REPLAYED) of
                {Mod,Func} ->
                    Mod:Func(?EKAF_CALLBACK_DOWNTIME_REPLAYED, Self, StateName, State, {ok, Messages });
                _ ->
                    ok
            end,
            ekaf:produce_async_batched( Topic, Messages)
    end.

save_messages_until(_, _, Messages, OfflineMessages, undefined)->
    case Messages of
        SomeList when is_list(SomeList)->
            Messages ++ OfflineMessages;
        _ ->
            [Messages|OfflineMessages]
    end;
save_messages_until(StateName, State, TempMessages, OfflineMessages, MaxDowntimeBufferSize)->
    %% if kafka is down, we save to a queue in-memory
    %% by default this is un-bounded assuming kafka comes up quickly
    %% optionally, bound this to some max oldest in the queue
    %% see _demo.erl for the ekaf_max_downtime_buffer_size option
    Messages = case TempMessages of
                SomeMList when is_list(SomeMList)->
                    TempMessages;
                _ ->
                    [TempMessages]
            end,
    Combined = Messages ++ OfflineMessages,
    case length(Combined) of
        BigLen when BigLen > MaxDowntimeBufferSize ->
            case ekaf_callbacks:find(?EKAF_CALLBACK_MAX_DOWNTIME_BUFFER_REACHED) of
                {Mod,Func} ->
                    Self = self(),
                    Mod:Func(?EKAF_CALLBACK_MAX_DOWNTIME_BUFFER_REACHED, Self, StateName, State, {ok, OfflineMessages});
                _ ->
                    ok
            end,
            %%TODO: optimize
            %% on the other hand, this is an edge case
            lists:sublist(Messages,MaxDowntimeBufferSize);
        _ ->
            Combined
    end.


%% many pids that called prepare or pick or publish
%% maybe waiting to get metadata, before continuing
reply_to_prepares(WorkerUp, #ekaf_server{ kv = KV } = State)->
    case dict:find(prepare, KV) of
        error ->
           State;
        {ok, Pids} ->
            [ gen_fsm:reply(From, {ok,WorkerUp}) || From <- Pids ],
            State#ekaf_server{ kv = dict:erase(prepare, KV) }
    end.

fsm_next_state(StateName, StateData)->
    {next_state, StateName, StateData}.

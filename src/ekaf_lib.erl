-module(ekaf_lib).

-include("ekaf_definitions.hrl").

-export([
         %% API
         prepare/1, prepare/2,
         common_async/3,
         common_sync/3, common_sync/4,
         start_child/5, stop_child/1,

         %% read configs per topic
         get_bootstrap_broker/0, get_bootstrap_topics/0, get_max_buffer_size/1,
         get_max_downtime_buffer_size/1,
         get_concurrency_opts/1, get_buffer_ttl/1, get_default/3,
         get_pool_name/1,

         %% fsm handling
         handle_reply_when_not_ready/4,
         handle_metadata_during_ready/2,
         handle_async_as_batch/3, handle_sync_as_batch/4,
         handle_inactivity_timeout/1, fsm_next_state/2, fsm_next_state/3,

         %% manipulating state
         cursor/3,flush/1,

         %% helpers
         data_to_message_sets/1, data_to_message_set/1,response_to_proplist/1,
         add_message_to_buffer/2, pop_messages_from_buffer/2, add_messages_to_sent/2,
         flush_messages_callback/1, flushed_messages_replied_callback/2
]).

prepare(Topic)->
    ekaf_sup:start_child(ekaf_sup,
                         {Topic, {ekaf_server, start_link, [[Topic]]},
                          transient, infinity, worker, []}
                        ),
    Pid = (catch gproc:where({n,l,Topic})),
    case Pid of
        SomePid when is_pid(SomePid)->
            gen_fsm:sync_send_event(Pid, prepare);
        _E ->
            {error, _E}
    end.

prepare(Topic, Callback)->
    %% Each topic must first get metadata of the partitions
    %% For each topic+partition combination, then starts `ekaf_partition_workers` workers
    %% This process will bootup the partition workers, then die
    %% Hence is not supervised
    Prepared = prepare(Topic),
    case Prepared of
        {ok, Pid} when Callback =:= undefined ->
            Pid;
        {ok, Pid} ->
            Callback(Pid);
        _ ->
            ok
    end.

%% Pick a worker, and pass it into the callback
%% usually, callback(worker) blocks on the worker
%% but ekaf:pick creates a new process that blocks instead
%% this way the worker doesnt wait for the action to finish
common_async(Event, Topic, Data)->
    ekaf:pick(Topic, fun(Worker)->
                             case Worker of
                                 {error,{retry,_N}} ->
                                     common_async(Event, Topic, Data);
                                 {error,_}=E ->
                                     E;
                                 _ ->
                                     gen_fsm:send_event(Worker, {Event, Data})
                             end
                     end),
    ok.

common_sync(Event, Topic, Data)->
    common_sync(Event, Topic, Data, ?EKAF_SYNC_TIMEOUT).
common_sync(Event, Topic, Data, Timeout)->
    Worker = ekaf:pick(Topic),
    case Worker of
        {error,{retry,_N}} ->
            common_sync(Event, Topic, Data);
        {error,_}=E ->
            E;
        _ ->
            gen_fsm:sync_send_event(Worker, {Event, Data}, Timeout)
    end.

cursor(_,[], State)->
    {[], State};
cursor(BatchEnabled,Messages,#ekaf_fsm{ to_buffer = _ToBuffer}=State)->
    case BatchEnabled of
        true ->
            %% only timeout sends messages every BufferTTL ms
            ekaf_lib:add_message_to_buffer(Messages,State);
        _ ->
            Callback = ekaf_callbacks:find(?EKAF_CALLBACK_MASSAGE_BUFFER),
            MessageSets = case Callback of
                              {CallbackModule,CallbackFunction} ->
                                  CallbackModule:CallbackFunction
                                    (?EKAF_CALLBACK_MASSAGE_BUFFER,
                                     self(),
                                     ready,
                                     State,
                                     lists:reverse(Messages));
                              undefined ->
                                  ekaf_lib:data_to_message_sets(Messages)
                          end,
            {MessageSets,
             State#ekaf_fsm{ cor_id =  State#ekaf_fsm.cor_id+length(MessageSets)}}
    end.

handle_reply_when_not_ready(During,  Event, _From, State)->
    {reply, {error, {During,not_ready_for, Event}}, During, State}.

handle_inactivity_timeout(State)->
    ?MODULE:flush(State).

flush(#ekaf_fsm{ buffer = []} = State)->
    State;
flush(PrevState)->
    {Messages,State} = ekaf_lib:pop_messages_from_buffer([],PrevState),
    NextState = spawn_inactivity_timeout(Messages,State),
    flush_messages_callback(NextState),
    NextState#ekaf_fsm{ last_known_size = 0 }.

spawn_inactivity_timeout([],State)->
    State;
spawn_inactivity_timeout(_,#ekaf_fsm{socket = undefined} = State)->
    State;
spawn_inactivity_timeout(Messages, #ekaf_fsm{cor_id = CorId, client_id = ClientId, socket = Socket, topic_packet = DefTopicPacket, partition_packet = DefPartitionPacket, produce_packet = DefProducePacket} = State)->
    Self = self(),
    spawn(
      fun()->
              Callback = ekaf_callbacks:find(?EKAF_CALLBACK_MASSAGE_BUFFER),
              MessageSets = case Callback of
                                {CallbackModule,CallbackFunction} ->
                                    CallbackModule:CallbackFunction
                                      (?EKAF_CALLBACK_MASSAGE_BUFFER,
                                       self(),
                                       ready,
                                       State,
                                       lists:reverse(Messages));
                                undefined ->
                                    ekaf_lib:data_to_message_sets(Messages)
                            end,
              case MessageSets of
                  [] ->
                      ok;
                  _ ->
                      %% we buffer many messages
                      %% then send them as one sync call
                      %% TODO: retry for failed batches
                      ProducePacket = DefProducePacket#produce_request{
                                        required_acks=1,
                                        timeout=100,
                                        topics= [ DefTopicPacket#topic{
                                                    partitions =
                                                    [DefPartitionPacket#partition{
                                                       message_sets_size = length(MessageSets),
                                                       message_sets = MessageSets}]
                                                   }]
                                       },
                      Request = ekaf_protocol:encode_sync(CorId, ClientId, ProducePacket),
                      ekaf_socket:fork(Self, Socket, {send, produce_sync, Request})
              end
      end),
    State.

handle_async_as_batch(BatchEnabled, {_, Messages}, PrevState)->
    %% usually when we buffer we add to front, then while converting to messagesets it gets reversed
    %% but in sync batch, since it comes in order, reverse it for the expected behvaviour
    FinalMessages = case Messages of Bin when is_binary(Bin)-> Bin; _ -> lists:reverse(Messages) end,
    {MessageSets,State} = ekaf_lib:cursor(BatchEnabled, FinalMessages, PrevState),
    spawn_async_as_batch(BatchEnabled,MessageSets, State),
    fsm_next_state(ready, State, State#ekaf_fsm.buffer_ttl).

spawn_async_as_batch(_,[],_)->
    ok;
spawn_async_as_batch(_,_, #ekaf_fsm{ socket = undefined })->
    ok;
spawn_async_as_batch(BatchEnabled,MessageSets, #ekaf_fsm{ cor_id = CorId, socket = Socket, client_id = ClientId, to_buffer = ToBuffer, topic_packet = DefTopicPacket, partition_packet = DefPartitionPacket, produce_packet = DefProducePacket } = State)->
    Self = self(),
    spawn(fun()->
                  case (BatchEnabled and ToBuffer) of
                      true ->
                          Response = {buffered, State#ekaf_fsm.partition, length(State#ekaf_fsm.buffer) },
                          {reply, Response, ready, State, State#ekaf_fsm.buffer_ttl};

                      _ ->

                          %% we buffer many messages
                          %% then send them as one sync call
                          %% TODO: retry for failed batches
                          ProducePacket = DefProducePacket#produce_request{
                                            required_acks=1,
                                            timeout=100,
                                            topics= [ DefTopicPacket#topic{
                                                        partitions =
                                                        [DefPartitionPacket#partition{
                                                           message_sets_size = length(MessageSets),
                                                           message_sets = MessageSets}]
                                                       }]
                                           },
                          Request = ekaf_protocol:encode_sync(CorId, ClientId, ProducePacket),
                          ekaf_socket:fork(Self, Socket, {send, produce_sync, Request})
                  end
          end).

%% if BatchEnabled, then there are bufferent and sent only when reaching max_buffer_size
handle_sync_as_batch(BatchEnabled, {_, Messages}, From, #ekaf_fsm{ to_buffer = ToBuffer} = PrevState)->
    %% usually when we buffer we add to front, then while converting to messagesets it gets reversed
    %% but in sync batch, since it comes in order, reverse it for the expected behvaviour
    FinalMessages = case Messages of Bin when is_binary(Bin)-> Bin; _ -> lists:reverse(Messages) end,
    {MessageSets,State} = ekaf_lib:cursor(BatchEnabled, FinalMessages,PrevState),
    case (BatchEnabled and ToBuffer) of
        true ->
            Response = {buffered, State#ekaf_fsm.partition, length(State#ekaf_fsm.buffer) },
            {reply, Response, ready, State, State#ekaf_fsm.buffer_ttl};
        _ ->
            spawn_sync_as_batch(MessageSets,State),
            NextState = State#ekaf_fsm{kv = dict:append({cor_id, State#ekaf_fsm.cor_id}, {?EKAF_PACKET_DECODE_PRODUCE,From}, State#ekaf_fsm.kv ) },
            fsm_next_state(ready, NextState, NextState#ekaf_fsm.buffer_ttl)
    end.

spawn_sync_as_batch([],_)->
    ok;
spawn_sync_as_batch(_, #ekaf_fsm{ socket = undefined })->
    ok;
spawn_sync_as_batch(MessageSets, #ekaf_fsm{ cor_id = CorId, socket = Socket, client_id = ClientId, topic_packet = DefTopicPacket, partition_packet = DefPartitionPacket, produce_packet = DefProducePacket})->
    Self = self(),
    spawn(fun()->
                  %% each messge goes in a different messageset, even for batching
                  ProducePacket = DefProducePacket#produce_request{
                                    required_acks=1,
                                    timeout=100,
                                    topics= [ DefTopicPacket#topic{
                                                partitions =
                                                [DefPartitionPacket#partition{
                                                   message_sets_size = length(MessageSets),
                                                   message_sets = MessageSets}]
                                               }]
                                   },
                  Request = ekaf_protocol:encode_sync(CorId, ClientId, ProducePacket),
                  ekaf_socket:fork(Self, Socket, {send, produce_sync, Request})
          end).


handle_metadata_during_ready( _From, #ekaf_fsm{ metadata = Metadata } = State)->
    %% a workers metadata is always up-to-date
    %% because if it changes, the socket closes and the worker stops
    %% a new metadata request and responses starts new workers
    {reply, Metadata, ready, State}.

add_message_to_buffer(Message, #ekaf_fsm{ buffer = Buffer } = State) when is_binary(Message)->
    {[], State#ekaf_fsm{ buffer = [Message | Buffer], cor_id = State#ekaf_fsm.cor_id+1}};
add_message_to_buffer(Messages,#ekaf_fsm{ buffer = Buffer } = State) ->
    {[], State#ekaf_fsm{ buffer = Messages ++ Buffer, cor_id = State#ekaf_fsm.cor_id+1}}.

pop_messages_from_buffer([],#ekaf_fsm{ buffer = Buffer, cor_id = CorId} = State) ->
    {Buffer, State#ekaf_fsm{ buffer = [], cor_id = CorId+1}};
pop_messages_from_buffer(Messages,#ekaf_fsm{ buffer = Buffer, cor_id = CorId} = State) when is_list(Messages)->
    {Messages++Buffer, State#ekaf_fsm{ buffer = [], cor_id = CorId+1}};
pop_messages_from_buffer(Message,#ekaf_fsm{ buffer= Buffer, cor_id = CorId }=State) ->
    {[Message|Buffer], State#ekaf_fsm{ buffer = [], cor_id = CorId+1}}.

add_messages_to_sent(Messages, #ekaf_fsm{ kv = KV, cor_id = CorId } = State)->
    State#ekaf_fsm{ kv = dict:append({cor_id,CorId}, {?EKAF_PACKET_DECODE_PRODUCE_ASYNC_BATCH, Messages}, KV)}.

flush_messages_callback(State)->
    Self = self(),
    spawn(fun()->
                  FlushCallback = ekaf_callbacks:find(?EKAF_CALLBACK_FLUSH),
                  case FlushCallback of
                      {FlushCallbackModule,FlushCallbackFunction} ->
                          FlushCallbackModule:FlushCallbackFunction(?EKAF_CALLBACK_FLUSH, Self, ready, State, undefined);
                      undefined ->
                          ok
                  end
          end).

flushed_messages_replied_callback(State, Packet)->
    Self = self(),
    spawn(fun()->
                  Reply = {{replied, State#ekaf_fsm.partition, Self},
                           ekaf_protocol:decode_produce_response(Packet)},
                  FlushCallback = ekaf_callbacks:find(?EKAF_CALLBACK_FLUSHED_REPLIED),
                  case FlushCallback of
                      {FlushCallbackModule,FlushCallbackFunction} ->
                          FlushCallbackModule:FlushCallbackFunction(?EKAF_CALLBACK_FLUSHED_REPLIED, Self, ready, State, {ok,Reply});
                      undefined ->
                          ok
                  end
          end).

response_to_proplist({{sent,_,_},Response})->
    response_to_proplist(Response);
response_to_proplist(#produce_response{topics = Topics})->
    ProduceJson =
        lists:foldl(fun(TopicRow,TAcc)->
                            TempAcc = [ begin
                                            case PartitionRow#partition.error_code of
                                                0 ->
                                                    {ok,true};
                                                ErrorCode ->
                                                    {error,ErrorCode}
                                            end
                                        end || PartitionRow <- TopicRow#topic.partitions ],
                            [TempAcc|TAcc]
                    end, [], Topics),
    lists:reverse(ProduceJson);
response_to_proplist(_) ->
    [].

data_to_message_sets(Data)->
    data_to_message_sets(Data,[]).
data_to_message_sets([],Messages)->
    Messages;
data_to_message_sets([Message|Rest], Messages) when is_record(Message,message)->
    data_to_message_sets(Rest, [Message|Messages]);
data_to_message_sets([{Key,Value}|Rest], Messages ) ->
    data_to_message_sets(Rest, [#message_set{ size=1, messages=[#message{key = Key, value = Value }]}|Messages]);
data_to_message_sets([Value|Rest], Messages) ->
    data_to_message_sets(Rest, [#message_set{ size = 1, messages = [#message{ value = Value }]}|Messages]);
data_to_message_sets(Value, Messages) ->
    [#message_set{ size = 1, messages = [#message{value = Value}]}| Messages].

data_to_message_set(Data)->
    data_to_message_set(Data,#message_set{size = 0, messages= []}).
data_to_message_set([],#message_set{ messages = Messages} )->
    #message_set{ size = length(Messages), messages = Messages };
data_to_message_set([Message|Rest], #message_set{ messages = Messages }) when is_record(Message,message)->
    data_to_message_set(Rest, #message_set{ messages = [Message|Messages]});
data_to_message_set([{Key,Value}|Rest], #message_set{ messages = Messages }) ->
    data_to_message_set(Rest, #message_set{ messages = [#message{ key = Key, value = Value }|Messages]});
data_to_message_set([Value|Rest], #message_set{ messages = Messages }) ->
    data_to_message_set(Rest, #message_set{ messages = [#message{ value = Value }|Messages]});
data_to_message_set(Value, #message_set{ size = Size, messages = Messages }) ->
    #message_set{ size = Size + 1, messages = [#message{value = Value}| Messages] }.

start_child(Metadata, Broker, Topic, Leader, PartitionId)->
    TopicName = Topic#topic.name,
    SizeArgs = ?MODULE:get_concurrency_opts(TopicName),
    NextPoolName = ?MODULE:get_pool_name({TopicName, Broker, PartitionId, Leader }),

    WorkerArgs = [NextPoolName, Metadata, {Broker#broker.host,Broker#broker.port}, TopicName, Leader, PartitionId],
    [
     begin
         WorkerId = erlang:phash2({WorkerArgs,X}),
         Child = ekaf_sup:start_child(ekaf_sup,
                              {WorkerId, {ekaf_fsm, start_link, [[WorkerId|WorkerArgs]]},
                              transient, infinity, worker, [ekaf_fsm]}
                             ),
         ?DEBUG_MSG("start child ~p for broker~w#~w => ~p",[WorkerId, Leader, PartitionId, Child])
     end || X<- lists:seq(1, proplists:get_value(size, SizeArgs))].

stop_child(WorkerId)->
    spawn(fun()->
                  receive
                  after 10 ->
                          supervisor:delete_child(ekaf_sup, WorkerId)
                  end
          end).

get_bootstrap_broker()->
    case application:get_env(ekaf, ekaf_bootstrap_broker) of
        {ok,{Broker,Port}}->
            {Broker,Port};
        _ ->
            {"localhost",9091}
    end.

get_bootstrap_topics()->
    case application:get_env(ekaf, ekaf_bootstrap_topics) of
        {ok,L}->
            {ok,L};
        _ ->
            {ok,[]}
    end.

get_concurrency_opts(Topic)->
    %[{size,5},{max_overflow,10}],
    Size =  get_default(Topic, ekaf_per_partition_workers, ?EKAF_DEFAULT_PER_PARTITION_WORKERS),
    TempMaxSize =  get_default(Topic, ekaf_per_partition_workers_max, ?EKAF_DEFAULT_PER_PARTITION_WORKERS_MAX),
    MaxSize = case TempMaxSize of
                  0 ->
                      0;
                  Big when Big > Size ->
                      Big;
                  _ ->
                      Size*2
              end,
    [{size,Size}, {max_overflow, MaxSize}].

get_max_buffer_size(Topic)->
    get_default(Topic, ekaf_max_buffer_size,?EKAF_DEFAULT_MAX_BUFFER_SIZE).

get_max_downtime_buffer_size(Topic)->
    get_default(Topic, ekaf_max_downtime_buffer_size, ?EKAF_DEFAULT_MAX_DOWNTIME_BUFFER_SIZE).

get_buffer_ttl(Topic)->
    get_default(Topic, ekaf_buffer_ttl, ?EKAF_DEFAULT_BUFFER_TTL).

get_default(Topic, Key, Default)->
    case application:get_env(ekaf,Key) of
        {ok,L} when is_list(L)->
            case proplists:get_value(Topic, L) of
                TopicAtom when is_atom(TopicAtom), TopicAtom =/= undefined->
                    TopicAtom;
                TopicMax when is_integer(TopicMax) ->
                    TopicMax;
                _ ->
                    case proplists:get_value(Key, L) of
                        TopicAtom when is_atom(TopicAtom), TopicAtom =/= undefined->
                            TopicAtom;
                        TopicMax when is_integer(TopicMax) ->
                            TopicMax;
                        _ ->
                           Default
                    end
            end;
        {ok,TopicAtom} when is_atom(TopicAtom), TopicAtom =/= undefined->
            TopicAtom;
        {ok,Max} when is_integer(Max)->
            Max;
        _ ->
            Default
    end.

get_pool_name(State) when is_record(State,ekaf_fsm)->
    PoolName = State#ekaf_fsm.pool, Topic = State#ekaf_fsm.topic, Broker = State#ekaf_fsm.broker, PartitionId = State#ekaf_fsm.partition, Leader = State#ekaf_fsm.leader,
    get_pool_name({PoolName, Topic, Broker, PartitionId, Leader });
get_pool_name({PoolName, Topic, Broker, PartitionId, Leader })->
    NextPoolName = {PoolName, Topic, Broker, PartitionId, Leader },
    ekaf_utils:btoatom(ekaf_utils:itob(erlang:phash2(NextPoolName)));
get_pool_name({Topic, Broker, PartitionId, Leader })->
    NextPoolName = {Topic, Broker, PartitionId, Leader },
    ekaf_utils:btoatom(ekaf_utils:itob(erlang:phash2(NextPoolName))).

fsm_next_state(StateName,State)->
    {next_state, StateName, State}.
fsm_next_state(StateName, State, Timeout)->
    {next_state, StateName, State, Timeout}.

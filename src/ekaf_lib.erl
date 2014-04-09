-module(ekaf_lib).

-include("ekaf_definitions.hrl").

-export([
         %% API
         prepare/1,
         common_async/3,
         common_sync/3, common_sync/4,
         start_child/4,

         %% read configs per topic
         get_bootstrap_broker/0, get_bootstrap_topics/0, get_max_buffer_size/1, get_concurrency_opts/1, get_buffer_ttl/1, get_default/3,
         get_pool_name/1,

         %% networking
         open_socket/1, close_socket/1,

         %% fsm handling
         handle_reply_when_not_ready/3, handle_continue_when_not_ready/3,
         handle_connected/2,
         handle_metadata_during_bootstrapping/2, handle_metadata_during_ready/3,
         handle_async_as_batch/3, handle_sync_as_batch/4,
         handle_inactivity_timeout/1,

         %% manipulating state
         cursor/3, fsm_next_state/2, fsm_next_state/3,

         %% helpers
         data_to_message_sets/1, data_to_message_set/1,response_to_proplist/1,
         add_message_to_buffer/2, pop_messages_from_buffer/2
]).

prepare(Topic)->
    %% Each topic must first get metadata of the partitions
    %% For each topic+partition combination, then starts `ekaf_partition_workers` workers
    %% This process will bootup the partition workers, then die
    %% Hence is not supervised
    ekaf_fsm:start_link([
                         self(),
                         ekaf_lib:get_bootstrap_broker(),Topic]).

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
                     end).

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

cursor(BatchEnabled,Messages,#ekaf_fsm{ cor_id = PrevCorId, max_buffer_size = MaxBufferSize, buffer = Buffer}=State)->
    Len = length(Buffer),
    Rem = Len rem MaxBufferSize,
    ToBuffer = case Rem of
                   0 when Len =:= 0 ->
                       true;
                   0 ->
                       false;
                   _ ->
                       true
               end,
    {MessageSets,NextState} =
        case BatchEnabled of
            true ->
                case ToBuffer of
                    true ->
                        {NewMessages,NewState} = ekaf_lib:add_message_to_buffer(Messages,State),
                        {NewMessages,NewState};
                    _ ->
                        {NewMessages,NewState1} = ekaf_lib:pop_messages_from_buffer(Messages,State),
                        {ekaf_lib:data_to_message_sets(NewMessages),
                         NewState1
                        }
                end;
            _ ->
                {ekaf_lib:data_to_message_sets(Messages), State}
        end,
    %io:format("~n cor_id ~p prev state is ~p, next state was ~p < ~p",[PrevCorId, length(Buffer), length(NextState#ekaf_fsm.buffer), MaxBufferSize]),
    {PrevCorId, ToBuffer, MessageSets, NextState}.

handle_continue_when_not_ready(StateName,_Event, State)->
    %io:format("~n continue ~p since not ready when got ~p",[StateName,_Event]),
    fsm_next_state(StateName, State).

handle_reply_when_not_ready(_Event, _From, State)->
    {reply, {error, {not_ready_for,_Event}}, State}.

handle_inactivity_timeout(#ekaf_fsm{cor_id = PrevCorId} = PrevState)->
    {Messages,State} = ekaf_lib:pop_messages_from_buffer([],PrevState),
    MessageSets = ekaf_lib:data_to_message_sets(Messages),
    CorrelationId = PrevCorId+1,
    Topic = State#ekaf_fsm.topic, Partition=State#ekaf_fsm.partition, Leader = State#ekaf_fsm.leader, Socket=State#ekaf_fsm.socket, ClientId = State#ekaf_fsm.client_id,

    case length(MessageSets) of
        0 ->
            State;
        _ ->
            %io:format("~n send remaining messages ~p remaining in buffer: ~w",[length(MessageSets),length(State#ekaf_fsm.buffer)]),
            TopicPacket = #topic{
              name = Topic,
              partitions =
              [#partition{id = Partition, leader = Leader,
                          %% each messge goes in a different messageset, even for batching
                          message_sets_size = length(MessageSets), message_sets = MessageSets}]},
            ProducePacket = #produce_request{
              timeout=100, topics= [TopicPacket]
             },
            Request = ekaf_protocol:encode_async(CorrelationId, ClientId, ProducePacket),
            spawn(fun()-> gen_tcp:send(Socket, Request)  end),
            State
    end.

handle_async_as_batch(BatchEnabled, {_, Messages}, PrevState)->
    {CorrelationId,ToBuffer,MessageSets,State} = ekaf_lib:cursor(BatchEnabled,Messages,PrevState),
    Topic = State#ekaf_fsm.topic, Partition=State#ekaf_fsm.partition, Leader = State#ekaf_fsm.leader, Socket=State#ekaf_fsm.socket, ClientId = State#ekaf_fsm.client_id,
    %io:format("~n To buffer ? ~p, ~nbatch: ~p, ~nMs:~p",[ToBuffer,BatchEnabled, MessageSets]),
    case (BatchEnabled and ToBuffer) of
        true ->
            fsm_next_state(ready, State, State#ekaf_fsm.buffer_ttl);
        _ ->
            %io:format("~n sending async batch of ~w",[length(MessageSets)]),
            TopicPacket = #topic{
              name = Topic,
              partitions =
              [#partition{id = Partition, leader = Leader,
                          %% each messge goes in a different messageset, even for batching
                          message_sets_size = length(MessageSets), message_sets = MessageSets}]},
            ProducePacket = #produce_request{
              timeout=100, topics= [TopicPacket]
             },
            Request = ekaf_protocol:encode_async(CorrelationId,ClientId, ProducePacket),
            spawn(fun()-> gen_tcp:send(Socket, Request)  end),
            NewState = State#ekaf_fsm{cor_id = CorrelationId + 1 },
            fsm_next_state(ready, NewState, State#ekaf_fsm.buffer_ttl)
    end.

%% if BatchEnabled, then there are bufferent and sent only when reaching max_buffer_size
handle_sync_as_batch(BatchEnabled, {_, Messages}, From, PrevState)->
    {CorrelationId,ToBuffer,MessageSets,State} = ekaf_lib:cursor(BatchEnabled,Messages,PrevState),
    Topic = State#ekaf_fsm.topic, Partition=State#ekaf_fsm.partition, Leader = State#ekaf_fsm.leader, Socket=State#ekaf_fsm.socket, ClientId = State#ekaf_fsm.client_id,
    %io:format("~n To buffer ? ~p, ~nbatch: ~p, ~nMs:~p",[ToBuffer,BatchEnabled, MessageSets]),

    case (BatchEnabled and ToBuffer) of
        true ->
            BufferIndex = length(State#ekaf_fsm.buffer) rem (State#ekaf_fsm.max_buffer_size),
            Response = {buffered, State#ekaf_fsm.partition, BufferIndex },
            {reply, Response, ready, State, State#ekaf_fsm.buffer_ttl};
        _ ->
            %io:format("~n To buffer ? ~p, ~nbatch: ~p, ~nMs:~p",[ToBuffer,BatchEnabled, MessageSets]),
            %io:format("~n sending async batch of ~w",[length(MessageSets)]),
            TopicPacket = #topic{
              name = Topic,
              partitions =
              [#partition{id = Partition,
                          leader = Leader,
                          %% each messge goes in a different messageset, even for batching
                          message_sets_size = length(MessageSets),
                          message_sets = MessageSets}]},
            ProducePacket = #produce_request{
              required_acks=1,
              timeout=100,
              topics= [TopicPacket]
             },
            Request = ekaf_protocol:encode_sync(CorrelationId,ClientId, ProducePacket),
            case gen_tcp:send(Socket, Request) of
                ok ->
                    NewState = State#ekaf_fsm{cor_id = CorrelationId + 1,
                                              kv = dict:append({cor_id, CorrelationId}, {?EKAF_PACKET_DECODE_PRODUCE,From}, State#ekaf_fsm.kv )},
                    % {reply, Response, ready, NewState};
                    fsm_next_state(ready, NewState, State#ekaf_fsm.buffer_ttl);
                {error, Reason} ->
                    ?ERROR_MSG("~p",[Reason]),
                    {stop, Reason, State}
            end
    end.

handle_connected({metadata, Topic}, State)->
    CorrelationId = State#ekaf_fsm.cor_id,
    Request = ekaf_protocol:encode_metadata_request(CorrelationId, "", [Topic]),
    case gen_tcp:send(State#ekaf_fsm.socket, Request) of
        ok ->
            Metadata =
                receive
                    {tcp, _Port, <<_CorrelationId:32, _/binary>> = Packet} ->
                        ekaf_protocol:decode_metadata_response(Packet);
                    _E ->
                        {error,_E}
                end,
            NewState = State#ekaf_fsm{cor_id = CorrelationId + 1},
            gen_fsm:send_event(self(), {metadata,Metadata}),
            fsm_next_state(bootstrapping, NewState);
        {error, Reason} ->
            {stop, Reason, State}
    end.

handle_metadata_during_bootstrapping({metadata,Metadata}, #ekaf_fsm{ topic = Topic } = State)->
    BrokersDict = lists:foldl(fun(Broker,Dict)->
                                       dict:append(Broker#broker.node_id,
                                                  Broker,
                                                  Dict)
                              end, dict:new(), Metadata#metadata_response.brokers),
    pg2:create(Topic),
    Started = lists:foldl(
                fun(#topic{ name = CurrTopicName } = CurrTopic,TopicsAcc) when CurrTopicName =:= Topic ->
                        TempStarted =
                            [ begin
                                  Leader = Partition#partition.leader,
                                  PartitionId = Partition#partition.id,
                                  {ok,[Broker]} = dict:find(Leader, BrokersDict),
                                  ekaf_lib:start_child(Broker, CurrTopic, Leader, PartitionId )

                              end
                              || Partition <- CurrTopic#topic.partitions ],
                        [TempStarted|TopicsAcc];
                    (TopicsAcc,_OtherTopic)->
                        io:format("~n ignore ~p",[_OtherTopic]),
                        TopicsAcc
                end, [], Metadata#metadata_response.topics),
    State#ekaf_fsm.reply_to ! {ready,Started},
    {stop, normal, State#ekaf_fsm{metadata = Metadata}}.

handle_metadata_during_ready({metadata, Topic}, _From, State)->
    CorrelationId = State#ekaf_fsm.cor_id+1,
    ClientId = State#ekaf_fsm.client_id,
    Request = ekaf_protocol:encode_metadata_request(CorrelationId,ClientId, [Topic]),
    case gen_tcp:send(State#ekaf_fsm.socket, Request) of
        ok ->
            Response =
                receive
                    {tcp, _Port, <<CorrelationId:32, _/binary>> = Packet} ->
                        ekaf_protocol:decode_metadata_response(Packet);
                    Packet ->
                        {error,Packet}
                end,
            NewState = State#ekaf_fsm{cor_id = CorrelationId + 1},
            {reply, Response, ready, NewState};
        {error, Reason} ->
            ?ERROR_MSG("~p",[Reason]),
            {stop, Reason, State}
    end.

add_message_to_buffer(Messages,State) when is_list(Messages)->
    {[],State#ekaf_fsm{ buffer = Messages ++ State#ekaf_fsm.buffer }};
add_message_to_buffer(Message, State) ->
    add_message_to_buffer([Message], State).

pop_messages_from_buffer(Messages,State) when is_list(Messages)->
    {Messages ++ State#ekaf_fsm.buffer, State#ekaf_fsm{ buffer = []}};
pop_messages_from_buffer(Message, State) ->
    pop_messages_from_buffer([Message], State).

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
                            TempAcc++TAcc
                    end, [], Topics),
    lists:reverse(ProduceJson);
response_to_proplist(_) ->
    [].

data_to_message_sets(Data)->
    data_to_message_sets(Data,[]).
data_to_message_sets([],Messages)->
    lists:reverse(Messages);
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

start_child(Broker, Topic, Leader, PartitionId)->
    TopicName = Topic#topic.name,
    SizeArgs = ?MODULE:get_concurrency_opts(Topic),
    NextPoolName = ?MODULE:get_pool_name({TopicName, Broker, PartitionId, Leader }),

    WorkerArgs = [NextPoolName, {Broker#broker.host,Broker#broker.port}, TopicName, Leader, PartitionId],

    %Poolboy
    %ChildPoolName = ?MODULE:get_pool_name({NextPoolName, TopicName, Broker, PartitionId, Leader }),
    %PoolArgs = [{name, {local, ChildPoolName }},{worker_module, ekaf_fsm}] ++ SizeArgs,
    % ekaf_sup:start_child(ekaf_sup,poolboy:child_spec(NextPoolName, PoolArgs, WorkerArgs))

    [
      begin
          ekaf_sup:start_child(ekaf_sup,
                               {{WorkerArgs,X}, {ekaf_fsm, start_link, [WorkerArgs]},
                                permanent, infinity, worker, [ekaf_fsm]}
                              )
      end || X<- lists:seq(1, proplists:get_value(size, SizeArgs))].

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

open_socket({Host,Port}) when is_binary(Host)->
    open_socket({ ekaf_utils:btoa(Host),Port});

open_socket({Host,Port})->
    gen_tcp:connect(Host, Port, [{packet, 4}, binary]).

close_socket(undefined)->
    ok;
close_socket(Socket) ->
    gen_tcp:close(Socket).

fsm_next_state(StateName, StateData)->
    {next_state, StateName, StateData}.

fsm_next_state(StateName, StateData, Timeout)->
    {next_state, StateName, StateData, Timeout}.

-module(ekaf_lib).

-include("ekaf_definitions.hrl").

-export([start_child/4, get_bootstrap_broker/0, get_max_buffer_size/1, pool_name/1]).
-export([data_to_message_sets/1, data_to_message_set/1,
         add_message_to_buffer/2, pop_messages_from_buffer/2]).
-export([response_to_proplist/1]).

add_message_to_buffer(Messages,State) when is_list(Messages)->
    {[],State#ekaf_fsm{ buffer = Messages ++ State#ekaf_fsm.buffer}};
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

%%
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
    %PoolName =
    TopicName = Topic#topic.name,
    SizeArgs = get_concurrency_opts(Topic),
    NextPoolName = ?MODULE:pool_name({TopicName, Broker, PartitionId, Leader }),
    ChildPoolName = ?MODULE:pool_name({NextPoolName, TopicName, Broker, PartitionId, Leader }),

    PoolArgs = [{name, {local, ChildPoolName }},
                {worker_module, ekaf_fsm}] ++ SizeArgs,
    WorkerArgs = [NextPoolName, {Broker#broker.host,Broker#broker.port}, TopicName, Leader, PartitionId],
    %%io:format("~nAsk kafboy_sup to start ~p child with workerargs:~p =>~n ~p",[NextPoolName, WorkerArgs, poolboy:child_spec(NextPoolName, PoolArgs, WorkerArgs)]),

    ekaf_sup:start_child(ekaf_sup, poolboy:child_spec(NextPoolName, PoolArgs, WorkerArgs)).

get_bootstrap_broker()->
    case application:get_env(ekaf, ekaf_bootstrap_broker) of
        {ok,{Broker,Port}}->
            {Broker,Port};
        _ ->
            {"localhost",9091}
    end.

get_concurrency_opts(Topic)->
    %[{size,5},{max_overflow,10}],
    Size = case application:get_env(ekaf,ekaf_per_partition_workers) of
               {ok,L} when is_list(L)->
                   case proplists:get_value(Topic, L) of
                       TopicSize when is_integer(TopicSize)->
                           TopicSize;
                       _ ->
                           case proplists:get_value(ekaf_per_partition_workers, L) of
                               DefaultSize when is_integer(DefaultSize)->
                                   DefaultSize;
                               _ ->
                                   ?EKAF_DEFAULT_PER_PARTITION_WORKERS
                           end
                   end;
               {ok,_Size} when is_integer(_Size) ->
                   _Size;
               _ ->
                   ?EKAF_DEFAULT_PER_PARTITION_WORKERS
           end,
   TempMaxSize = case application:get_env(ekaf,ekaf_per_partition_workers_max) of
                     {ok,LMax} when is_list(LMax)->
                         case proplists:get_value(Topic, LMax) of
                             TopicMaxSize when is_integer(TopicMaxSize)->
                                 TopicMaxSize;
                             _ ->
                                 case proplists:get_value(ekaf_per_partition_workers_max, LMax) of
                                     DefaultMaxSize when is_integer(DefaultMaxSize)->
                                         DefaultMaxSize;
                                     _ ->
                                         ?EKAF_DEFAULT_PER_PARTITION_WORKERS_MAX
                                 end
                         end;
                     {ok,_MaxSize} when is_integer(_MaxSize) ->
                         _MaxSize;
                     _ ->
                         ?EKAF_DEFAULT_PER_PARTITION_WORKERS_MAX
              end,
    MaxSize = case TempMaxSize of
                  Big when Big > Size ->
                      Big;
                  _ ->
                      Size*2
              end,
    [{size,Size}, {max_overflow, MaxSize}].

get_max_buffer_size(Topic)->
    case application:get_env(ekaf,ekaf_max_buffer_size) of
        {ok,L} when is_list(L)->
            case proplists:get_value(Topic, L) of
                TopicMax when is_integer(TopicMax) ->
                    TopicMax;
                _ ->
                    case proplists:get_value(ekaf_max_buffer_size, L) of
                        TopicMax when is_integer(TopicMax) ->
                            TopicMax;
                        _ ->
                            ?EKAF_DEFAULT_MAX_BUFFER_SIZE
                    end
            end;
        {ok,Max} when is_integer(Max)->
            Max;
        _ ->
            ?EKAF_DEFAULT_MAX_BUFFER_SIZE
    end.

pool_name(State) when is_record(State,ekaf_fsm)->
    PoolName = State#ekaf_fsm.pool, Topic = State#ekaf_fsm.topic, Broker = State#ekaf_fsm.broker, PartitionId = State#ekaf_fsm.partition, Leader = State#ekaf_fsm.leader,
    pool_name({PoolName, Topic, Broker, PartitionId, Leader });
pool_name({PoolName, Topic, Broker, PartitionId, Leader })->
    NextPoolName = {PoolName, Topic, Broker, PartitionId, Leader },
    ekaf_utils:btoatom(ekaf_utils:itob(erlang:phash2(NextPoolName)));
pool_name({Topic, Broker, PartitionId, Leader })->
    NextPoolName = {Topic, Broker, PartitionId, Leader },
    ekaf_utils:btoatom(ekaf_utils:itob(erlang:phash2(NextPoolName))).

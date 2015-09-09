-module(ekaf_tests).

-ifdef(TEST).
-export([callback/5]).
-define(TEST_TOPIC,<<"ekaf">>).
-include("ekaf_definitions.hrl").
-include_lib("kafkamocker/include/kafkamocker.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

callback(Event, _From, _StateName, _State, _Extra)->
    kafka_consumer ! {produce, [Event]}.

metadata1()->
    Topics = [?TEST_TOPIC],
    #kafkamocker_metadata{
               brokers = [ #kafkamocker_broker{ id = 1, host = "localhost", port = 9907 }
                          ],
               topics =  [ #kafkamocker_topic { name = Topic,
                                                partitions = [ #kafkamocker_partition {id = 0, leader = 1,
                                                                                       replicas = [#kafkamocker_replica{ id = 1 }],
                                                                                       isrs = [#kafkamocker_isr{ id = 1 }]
                                                                                      }
                                                              ]
                                               }
                           || Topic <- Topics]
              }.

metadata2()->
    Topics = [?TEST_TOPIC],
    #kafkamocker_metadata{
               brokers = [ #kafkamocker_broker{ id = 1, host = "localhost", port = 9907 },
                           #kafkamocker_broker{ id = 2, host = "localhost", port = 9908 },
                           #kafkamocker_broker{ id = 3, host = "localhost", port = 9909 }
                          ],
               topics =  [ #kafkamocker_topic { name = Topic,
                                                partitions = [ #kafkamocker_partition {id = 0, leader = 1,
                                                                                       replicas = [#kafkamocker_replica{ id = 1 }],
                                                                                       isrs = [#kafkamocker_isr{ id = 1 }]
                                                                                      },
                                                               #kafkamocker_partition { id = 1, leader = 2,
                                                                                        replicas = [#kafkamocker_replica{ id = 2 }],
                                                                                        isrs = [#kafkamocker_isr{ id = 2 }]
                                                                                       },
                                                               #kafkamocker_partition { id = 3, leader = 3,
                                                                                        replicas = [#kafkamocker_replica{ id = 3 }],
                                                                                        isrs = [#kafkamocker_isr{ id = 3 }]
                                                                                       }
                                                              ]
                                               }
                           || Topic <- Topics]
              }.

pick_test_() ->
    {timeout, 15000, {setup,
     fun() ->
             Topic = ?TEST_TOPIC,

             Pid = spawn( fun() -> kafka_consumer_loop([],{self(),0}) end),
             erlang:register(kafka_consumer, Pid),
             [application:load(X) ||X<- [kafkamocker, ekaf] ],

             % start a kafka broker on 9908
             application:set_env(kafkamocker, kafkamocker_callback, kafka_consumer),
             application:set_env(kafkamocker, kafkamocker_bootstrap_topics, [Topic]),
             application:set_env(kafkamocker, kafkamocker_bootstrap_broker, {"localhost",9907}),

             application:set_env(ekaf, ?EKAF_CALLBACK_WORKER_UP_ATOM, {?MODULE,callback}),
             application:set_env(ekaf, ?EKAF_CALLBACK_WORKER_DOWN_ATOM, {?MODULE,callback}),
             application:set_env(ekaf, ?EKAF_CALLBACK_MAX_DOWNTIME_BUFFER_REACHED_ATOM, {?MODULE, callback}),

             application:set_env(ekaf, ekaf_per_partition_workers, 1),
             application:set_env(ekaf, ekaf_bootstrap_broker, {"localhost",9907}),
             application:set_env(ekaf, ekaf_buffer_ttl, 10),
             application:set_env(ekaf, ekaf_max_downtime_buffer_size, 5),

             [ application:start(App) || App <- [gproc, ranch, kafkamocker]],
             kafkamocker_fsm:start_link({metadata, metadata1()}),
             [ application:start(App) || App <- [ekaf]],

             ok
     end,
     fun(_) ->
             ok
     end,
     [
       {timeout, 5, ?_test(?debugVal(t_pick_from_new_pool()))}
      , ?_test(t_is_clean())
      , {spawn, ?_test(?debugVal(t_request_metadata()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_request_worker_state()))}
      , ?_test(t_is_clean())

      ,{spawn, ?_test(?debugVal(t_produce_sync_to_topic()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_produce_sync_multi_to_topic()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_produce_sync_in_batch_to_topic()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_produce_sync_multi_in_batch_to_topic()))}
      , ?_test(t_is_clean())

      ,{spawn, ?_test(?debugVal(t_produce_async_to_topic()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_produce_async_multi_to_topic()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_produce_async_in_batch_to_topic()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_produce_async_multi_in_batch_to_topic()))}
      , ?_test(t_is_clean())

      , {spawn, ?_test(?debugVal(t_max_messages_to_save_during_kafka_downtime()))}
      , ?_test(t_is_clean())
      , {spawn, ?_test(?debugVal(t_restart_kafka_broker()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_change_kafka_config()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_massage_buffer_encode_messages_as_one_large_message()))}
      , ?_test(t_is_clean())
      ]}}.

t_pick_from_new_pool()->
    Topic = ?TEST_TOPIC,
    ?assertMatch({error,_},pg2:get_closest_pid(Topic)),
    ?assertMatch({ok,_}, ekaf:prepare(Topic)),
    ekaf:pick(?TEST_TOPIC,fun(Worker)->
                                  ?assertNotEqual( Worker, undefined),
                                  case Worker of
                                      Pid when is_pid(Pid)->
                                          ?assertEqual( erlang:is_process_alive(Pid), true );
                                      _ ->
                                          ok
                                  end
                          end),

    kafka_consumer ! {flush, 1, self()},
    receive
        {flush, X}->
            ?assertEqual(X, [?EKAF_CALLBACK_WORKER_UP])
    end,

    ok.

t_request_metadata()->
    Metadata1 = ekaf:metadata(?TEST_TOPIC),
    Metadata1 = ekaf:metadata(?TEST_TOPIC),
    ?assertMatch(#metadata_response{}, Metadata1),
    ?assertEqual( length(Metadata1#metadata_response.brokers), 1),
    ok.

t_request_worker_state()->
    ?assertMatch(#ekaf_fsm{}, ekaf:info(?TEST_TOPIC)),
    ok.

t_produce_sync_to_topic()->
    kafka_consumer ! {flush, 1, self()},
    Sent = <<"1.sync">>,
    Response  = ekaf:produce_sync(?TEST_TOPIC, Sent),
    ?assertMatch({{sent,_,_},
                  #produce_response{ topics = [
                                               #topic{ partitions = [
                                                                     #partition{ error_code = 0 }
                                                                    ]
                                                      }
                                              ]
                                    }
                 },
                 Response),
    timer:sleep(100),
    receive
        {flush, [X]}->
            ?assertEqual(Sent, X)
    end,
    ok.

t_produce_sync_multi_to_topic()->
    kafka_consumer ! {flush, 3, self()},
    Sent = [ <<"2.sync multi1">>, <<"3.sync multi2">>, <<"4.sync multi3">> ],
    Response  = ekaf:produce_sync(?TEST_TOPIC, Sent),
    ?assertMatch({{sent,_,_},
                  #produce_response{ topics = [
                                               #topic{ partitions = [
                                                                     #partition{ error_code = 0 }
                                                                    ]
                                                      }
                                              ]
                                    }
                 },
                 Response),
    timer:sleep(100),
    receive
        {flush, X}->
            ?assertEqual(Sent, X)
    end,
    ok.

t_produce_async_to_topic()->
    kafka_consumer ! {flush, 1, self()},
    Sent = <<"5.async1">>,
    Response  = ekaf:produce_async(?TEST_TOPIC, Sent),
    ?assertMatch(ok,Response),
    timer:sleep(100),
    receive
        {flush, [X]}->
            ?assertEqual(Sent, X)
    end,
    ok.

t_produce_async_multi_to_topic()->
    kafka_consumer ! {flush, 3, self()},
    Sent = [ <<"6.async_multi1">>, <<"7.async_multi2">>, <<"8.async_multi3">> ],
    Response  = ekaf:produce_async(?TEST_TOPIC, Sent),
    ?assertMatch(ok,Response),
    timer:sleep(100),
    receive
        {flush, X}->
            ?assertEqual(Sent, X)
    end,
    ok.

t_produce_async_multi_in_batch_to_topic()->
    kafka_consumer ! {flush, 11, self()},
    Sent = [ <<(ekaf_utils:itob(X))/binary,".async multi batch">> || X<- lists:seq(9,19)],
    Response  = ekaf:produce_async_batched(?TEST_TOPIC, Sent ),
    ?assertMatch(ok,Response),
    timer:sleep(100),
    receive
        {flush, X}->
            ?assertEqual(Sent, X)
    end,
    ok.

t_produce_async_in_batch_to_topic()->
    kafka_consumer ! {flush, 1, self()},
    Sent = <<"20.async in batch">>,
    Response  = ekaf:produce_async_batched(?TEST_TOPIC, Sent),
    ?assertMatch(ok, Response),
    timer:sleep(100),
    receive
        {flush, [X]}->
            ?assertEqual(Sent, X)
    end,    ok.

t_produce_sync_multi_in_batch_to_topic()->
    kafka_consumer ! {flush, 11, self()},
    Sent = [ <<(ekaf_utils:itob(X))/binary,".sync multi batch">> || X<- lists:seq(21,31)],
    Response  = ekaf:produce_sync_batched(?TEST_TOPIC, Sent),
    ?assertMatch({buffered,_,_},
                 Response),
    timer:sleep(100),
    receive
        {flush, X}->
            ?assertEqual(Sent, X)
    end,
    ok.

t_produce_sync_in_batch_to_topic()->
    kafka_consumer ! {flush, 1, self()},
    Sent = <<"32.sync in batch">>,
    Response  = ekaf:produce_sync_batched(?TEST_TOPIC, Sent),
    ?assertMatch({buffered,_,_},
                 Response),
    timer:sleep(100),
    receive
        {flush, [X]}->
            ?assertEqual(Sent, X)
    end,
    ok.

t_restart_kafka_broker()->
    kafka_consumer ! {flush, 2, self()},
    gen_fsm:send_event(kafkamocker_fsm, {broker, stop, #kafkamocker_broker{ id = 1, host = "localhost", port = 9907 }}),
    gen_fsm:send_event(kafkamocker_fsm, {broker, start, #kafkamocker_broker{ id = 1, host = "localhost", port = 9907 }}),
    timer:sleep(100),
    receive
        {flush, X}->
            ?assertEqual([?EKAF_CALLBACK_WORKER_DOWN, ?EKAF_CALLBACK_WORKER_UP],X)
    end,
    ok.

t_change_kafka_config()->
    % add two more brokers to a running kafkamocker
    gen_fsm:sync_send_event(kafkamocker_fsm, {metadata, metadata2()}),
    timer:sleep(1000),
    Metadata2 = ekaf:metadata(?TEST_TOPIC),
    ?assertEqual( length(Metadata2#metadata_response.brokers), 3),
    ok.

t_max_messages_to_save_during_kafka_downtime()->
    kafka_consumer ! {flush, 1, self()},
    gen_fsm:send_event(kafkamocker_fsm, {broker, stop, #kafkamocker_broker{ id = 1, host = "localhost", port = 9907 }}),
    receive
        {flush, X1}->
            ?assertEqual([?EKAF_CALLBACK_WORKER_DOWN],X1)
    end,

    % %% sent 10 messages when broker was down
    Sent = [ <<(ekaf_utils:itob(X))/binary,". max downtime messages">> || X<- lists:seq(32,41)],
    ekaf:produce_async_batched(?TEST_TOPIC, Sent),


    %% start broker, get up message
    gen_fsm:send_event(kafkamocker_fsm, {broker, start, #kafkamocker_broker{ id = 1, host = "localhost", port = 9907 }}),
    kafka_consumer ! {flush, 2, self()},
    receive
        {flush, X2}->
            ?assertEqual([?EKAF_CALLBACK_MAX_DOWNTIME_BUFFER_REACHED,?EKAF_CALLBACK_WORKER_UP],X2)
    end,

    %% broker should get only max_downtime_buffer_size messages
    kafka_consumer ! {flush, 5, self()},
    receive
        {flush, X3}->
            ?assertEqual(5,length(X3))
    end,
    ok.

t_massage_buffer_encode_messages_as_one_large_message()->
    application:set_env(ekaf, ?EKAF_CALLBACK_MASSAGE_BUFFER_ATOM, {ekaf_callbacks,encode_messages_as_one_large_message}),
    kafka_consumer ! {flush, 1, self()},
    Event1 = [{<<"id">>, 1}],
    Event2 = [{<<"id">>, 2}],
    Response1 = ekaf:produce_async_batched(?TEST_TOPIC, [Event1]),
    Response2 = ekaf:produce_async_batched(?TEST_TOPIC, [Event2]),
    ?assertMatch(ok,Response1),
    ?assertMatch(ok,Response2),
    timer:sleep(100),
    receive
        {flush, [X]}->
            ?assertEqual([[{<<"id">>, 1}], [{<<"id">>, 2}]], lists:usort(binary_to_term(X)))
    end,
    application:set_env(ekaf, ?EKAF_CALLBACK_MASSAGE_BUFFER_ATOM, undefined),
    ok.

t_is_clean()->
    ok.

kafka_consumer_loop(Acc,{From,Stop}=Acc2)->
    receive
        {stop, N}->
            kafka_consumer_loop(Acc, {From,N});
        stop ->
            ok;
        {flush, NewFrom} ->
            %?debugFmt("asked to flush when consumer got ~p items",[Acc]),
            % kafka_consumer_loop should flush
            NewFrom ! {flush, Acc },
            kafka_consumer_loop([], {NewFrom,0});
        {flush, NewStop, NewFrom} ->
            %?debugFmt("~p asked to flush when consumer ~p items length is ~p",[NewFrom, Acc, NewStop]),
            NextAcc =
                case length(Acc) of
                    NewStop ->
                        %?debugFmt("time to reply to ~p since ~p",[NewFrom, NewStop]),
                       NewFrom ! {flush, Acc },
                       [];
                    _NotNow ->
                        %?debugFmt("not time to reply to ~p since ~p != ~p",[NewFrom, NewStop, _NotNow]),
                        Acc
            end,
            kafka_consumer_loop(NextAcc, {NewFrom,NewStop});

        {info, From} ->
            %kafka_consumer_loop should reply
            From ! {info, Acc },
            kafka_consumer_loop(Acc, Acc2);
        {produce,X} ->
            %?debugFmt("~p kafka_consumer_loop INCOMING ~p, stop at ~p",[ekaf_utils:epoch(), X, Stop]),
            Next = Acc++X,
            Next2 =
                case length(Next) of
                    Stop ->
                        %?debugFmt("reply to ~p since ~p",[From, length(Next)]),
                        From ! {flush, Next },
                        [];
                    _ ->
                        Next
                end,
            kafka_consumer_loop(Next2, Acc2);
        _E ->
            ?debugFmt("kafka_consumer_loop unexp: ~p",[_E]),
            kafka_consumer_loop(Acc, Acc2)
    end.

-endif.

-module(ekaf_tests).

-ifdef(TEST).
-define(TEST_TOPIC,<<"ekaf">>).
-include("ekaf_definitions.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

pick_test_() ->
    {setup,
     fun() ->
             application:start(gproc),
             application:load(ekaf),
             %% after 100ms of inactivity, send remaining messages in buffer
             application:set_env(ekaf, ekaf_sticky_partition_buffer_size, 200),
             application:set_env(ekaf, ekaf_buffer_ttl, 100),
             application:start(ekaf)
     end,
     fun(_) ->
             ok
     end,
     [
      {spawn, ?_test(?debugVal(t_pick_from_new_pool()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_request_metadata()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_request_info()))}
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
      ]}.

t_pick_from_new_pool()->
    Topic = ?TEST_TOPIC,
    ?assertMatch({error,_},pg2:get_closest_pid(Topic)),
    ekaf:prepare(Topic),
    timer:sleep(100),
    ekaf:pick(?TEST_TOPIC,fun(Worker)->
                                  ?assertNotEqual( Worker, undefined),
                                  case catch Worker of
                                      {Result,_}->
                                          ?assertNotEqual( Result, error );
                                      _ ->
                                          ok
                                  end
                          end),
    ok.

t_request_metadata()->
    ?assertMatch(#metadata_response{}, ekaf:metadata(?TEST_TOPIC)),
    ok.

t_request_info()->
    ?assertMatch(#ekaf_fsm{}, ekaf:info(?TEST_TOPIC)),
    ok.

t_produce_sync_to_topic()->
    Response  = ekaf:produce_sync(?TEST_TOPIC, <<"1.sync">>),
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
    ok.

t_produce_sync_multi_to_topic()->
    Response  = ekaf:produce_sync(?TEST_TOPIC,[ <<"2.sync multi1">>, <<"3.sync multi2">>, <<"4.sync multi3">> ]),
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
    ok.

t_produce_async_to_topic()->
    Response  = ekaf:produce_async(?TEST_TOPIC, <<"5.async1">>),
    ?assertMatch(ok,Response),
    ok.

t_produce_async_multi_to_topic()->
    Response  = ekaf:produce_async(?TEST_TOPIC,[ <<"6.async_multi1">>, <<"7.async_multi2">>, <<"8.async_multi3">> ]),
    ?assertMatch(ok,Response),
    ok.

t_produce_async_multi_in_batch_to_topic()->
    Response  = ekaf:produce_async_batched(?TEST_TOPIC, [ <<(ekaf_utils:itob(X))/binary,".async multi batch">> || X<- lists:seq(9,19)] ),
    ?assertMatch(ok,Response),

    %% to give time for all batches to flush
    timer:sleep(1000),
    ok.

t_produce_async_in_batch_to_topic()->
    Response  = ekaf:produce_async_batched(?TEST_TOPIC, <<"20.async in batch">>),
    ?assertMatch(ok,
                 Response),
    ok.

t_produce_sync_multi_in_batch_to_topic()->
    Response  = ekaf:produce_sync_batched(?TEST_TOPIC, [ <<(ekaf_utils:itob(X))/binary,".sync multi batch">> || X<- lists:seq(21,31)]),
    ?assertMatch({buffered,_,_},
                 Response),
    ok.

t_produce_sync_in_batch_to_topic()->
    Response  = ekaf:produce_sync_batched(?TEST_TOPIC, <<"32.sync in batch">>),
    ?assertMatch({buffered,_,_},
                 Response),
    ok.


t_is_clean()->
    ok.

-endif.

-module(ekaf_tests).

-ifdef(TEST).
-define(TEST_TOPIC,<<"ekaf">>).
-include("ekaf_definitions.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

-define(T_NAME, {n, l, {?MODULE, ?LINE, erlang:now()}}).

pick_test_() ->
    {setup,
     fun() ->
             application:start(poolboy),
             application:load(ekaf),
             %% after 100ms of inactivity, send remaining messages in buffer
             application:set_env(ekaf, ekaf_sticky_partition_buffer_size, 100),
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

      ,{spawn, ?_test(?debugVal(t_publish_sync_to_topic()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_publish_sync_multi_to_topic()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_publish_sync_in_batch_to_topic()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_publish_sync_multi_in_batch_to_topic()))}
      , ?_test(t_is_clean())

      ,{spawn, ?_test(?debugVal(t_publish_async_to_topic()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_publish_async_multi_to_topic()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_publish_async_in_batch_to_topic()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_publish_async_multi_in_batch_to_topic()))}
      , ?_test(t_is_clean())
      ]}.

t_pick_from_new_pool()->
    Topic = ?TEST_TOPIC,
    ekaf:prepare(Topic),
    ?assertMatch({error,_},pg2:get_closest_pid(Topic)),
    ekaf:pick(?TEST_TOPIC,fun(Worker)->
                                  ?assertNotEqual( Worker, {error, try_again})
                          end),
    ok.

t_request_metadata()->
    ?assertMatch(#metadata_response{}, ekaf:metadata(?TEST_TOPIC)),
    ok.

t_publish_sync_to_topic()->
    Response  = ekaf:produce_sync(?TEST_TOPIC, <<"sync1">>),
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

t_publish_sync_multi_to_topic()->
    Response  = ekaf:produce_sync(?TEST_TOPIC,[ <<"multi1">>, <<"multi2">>, <<"multi3">> ]),
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

t_publish_sync_in_batch_to_topic()->
    Response  = ekaf:produce_sync_batched(?TEST_TOPIC, <<"sync in batch">>),
    ?assertMatch({buffered,_,_},
                 Response),
    ok.

t_publish_sync_multi_in_batch_to_topic()->
    Response  = ekaf:produce_sync_batched(?TEST_TOPIC, [ ekaf_utils:itob(X) || X<- lists:seq(1,101)]),
    ?assertMatch({buffered,_,_},
                 Response),
    ok.

t_publish_async_to_topic()->
    Response  = ekaf:produce_async(?TEST_TOPIC, <<"async1">>),
    ?assertMatch(ok,Response),
    ok.

t_publish_async_multi_to_topic()->
    Response  = ekaf:produce_async(?TEST_TOPIC,[ <<"async_multi1">>, <<"async_multi2">>, <<"async_multi3">> ]),
    ?assertMatch(ok,Response),
    ok.

t_publish_async_in_batch_to_topic()->
    Response  = ekaf:produce_async_batched(?TEST_TOPIC, <<"async in batch">>),
    ?assertMatch(ok,
                 Response),
    ok.

t_publish_async_multi_in_batch_to_topic()->
    Response  = ekaf:produce_async_batched(?TEST_TOPIC, [ ekaf_utils:itob(X) || X<- lists:seq(1,101)] ),
    ?assertMatch(ok,Response),

    %% to give time for all batches to flush
    timer:sleep(3000),
    ok.

t_is_clean()->
    ok.

-endif.

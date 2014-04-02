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
             application:set_env(ekaf, ekaf_bootstrap_broker,{"localhost",9091}),
             application:start(ekaf)
     end,
     fun(_) ->
             %application:stop(ekaf),
             %application:stop(poolboy)
             ok
     end,
     [
      {spawn, ?_test(?debugVal(t_pick_from_new_pool()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_request_metadata()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_publish_to_topic()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_publish_many_to_topic()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_publish_async_to_topic()))}
      , ?_test(t_is_clean())
      ,{spawn, ?_test(?debugVal(t_publish_many_async_to_topic()))}
      , ?_test(t_is_clean())
      ]}.

t_pick_from_new_pool()->
    Topic = ?TEST_TOPIC,
    ?assertEqual( pg2:get_closest_pid(Topic), {error,{no_such_group,?TEST_TOPIC}}),
    ?assertEqual( ekaf:pick(?TEST_TOPIC), {error, try_again}),
    ok.

t_request_metadata()->
    Response  = ekaf:metadata(?TEST_TOPIC),
    ok.

t_publish_to_topic()->
    Response  = ekaf:produce_sync(?TEST_TOPIC, <<"sync1">>),
    ?assertMatch(#produce_response{ topics = [
                                              #topic{ partitions = [
                                                                    #partition{ error_code = 0 }
                                                                   ]
                                                     }
                                             ]
                                   },
                 Response),
    ok.

t_publish_many_to_topic()->
    Response  = ekaf:produce_sync(?TEST_TOPIC,[ <<"multi1">>, <<"multi2">>, <<"multi3">> ]),
    ?assertMatch(#produce_response{ topics = [
                                              #topic{ partitions = [
                                                                    #partition{ error_code = 0 }
                                                                   ]
                                                     }
                                             ]
                                   },
                 Response),
    ok.

t_publish_async_to_topic()->
    Response  = ekaf:produce_async(?TEST_TOPIC, <<"async1">>),
    ?assertMatch(ok,Response),
    ok.

t_publish_many_async_to_topic()->
    Response  = ekaf:produce_async(?TEST_TOPIC,[ <<"async_multi1">>, <<"async_multi2">>, <<"async_multi3">> ]),
    ?assertMatch(ok,Response),
    ok.

t_is_clean()->
    ok.

-endif.

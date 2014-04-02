-module(ekaf_tests).

-ifdef(TEST).

-include("ekaf_definitions.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

-define(T_NAME, {n, l, {?MODULE, ?LINE, erlang:now()}}).

pick_test_() ->
    {setup,
     fun() ->
             application:start(poolboy),
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
      ]}.

t_pick_from_new_pool()->
    Topic = <<"test8">>,
    ?assertEqual( pg2:get_closest_pid(Topic), {error,{no_such_group,<<"test8">>}}),
    ?assertEqual( ekaf:pick(<<"test8">>), {error, try_again}),
    ok.

t_request_metadata()->
    Response  = ekaf:metadata(<<"test8">>),
    ok.

t_publish_to_topic()->
    Response  = ekaf:produce_sync(<<"test8">>, <<"foo">>),
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
    Response  = ekaf:produce_sync(<<"test8">>,[ <<"multi1">>, <<"multi2">>, <<"multi3">> ]),
    ?assertMatch(#produce_response{ topics = [
                                              #topic{ partitions = [
                                                                    #partition{ error_code = 0 }
                                                                   ]
                                                     }
                                             ]
                                   },
                 Response),
    ok.

t_is_clean()->
    ok.

-endif.

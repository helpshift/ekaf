-module(ekaf_tests).

-ifdef(TEST).
-define(TEST_TOPIC,<<"ekaf">>).
-include("ekaf_definitions.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

pick_test_() ->
    {timeout, 15000, {setup,
     fun() ->
             Pid = spawn( fun() -> kafka_consumer_loop([],{self(),0}) end),
             erlang:register(kafka_consumer, Pid),
             [application:load(X) ||X<- [kafkamocker, ekaf] ],

             % start a kafka broker on 9908
             application:set_env(kafkamocker, kafkamocker_callback, kafka_consumer),
             application:set_env(kafkamocker, kafkamocker_bootstrap_topics, [?TEST_TOPIC]),
             application:set_env(kafkamocker, kafkamocker_bootstrap_broker, {"localhost",9908}),

             application:set_env(ekaf, ekaf_per_partition_workers_max, 1),
             application:set_env(ekaf, ekaf_bootstrap_broker, {"localhost",9908}),
             application:set_env(ekaf, ekaf_buffer_ttl, 10),
             [ application:start(App) || App <- [gproc, ranch, kafkamocker]],
             kafkamocker_fsm:start_link(),
             [ application:start(App) || App <- [ekaf]]
     end,
     fun(_) ->
             ok
     end,
     [
      {timeout, 15, ?_test(?debugVal(t_pick_from_new_pool()))}
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
      ]}}.

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
    kafka_consumer ! {flush, 1, self()},
    receive
        {flush, [X]}->
            ?assertEqual(Sent, X)
    end,
    ok.

t_produce_sync_multi_to_topic()->
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
    kafka_consumer ! {flush, 3, self()},
    receive
        {flush, X}->
            ?assertEqual(Sent, X)
    end,
    ok.

t_produce_async_to_topic()->
    Sent = <<"5.async1">>,
    Response  = ekaf:produce_async(?TEST_TOPIC, Sent),
    ?assertMatch(ok,Response),
    kafka_consumer ! {flush, 1, self()},
    receive
        {flush, [X]}->
            ?assertEqual(Sent, X)
    end,
    ok.

t_produce_async_multi_to_topic()->
    Sent = [ <<"6.async_multi1">>, <<"7.async_multi2">>, <<"8.async_multi3">> ],
    Response  = ekaf:produce_async(?TEST_TOPIC, Sent),
    ?assertMatch(ok,Response),
    kafka_consumer ! {flush, 3, self()},
    receive
        {flush, X}->
            ?assertEqual(Sent, X)
    end,
    ok.

t_produce_async_multi_in_batch_to_topic()->
    Sent = [ <<(ekaf_utils:itob(X))/binary,".async multi batch">> || X<- lists:seq(9,19)],
    Response  = ekaf:produce_async_batched(?TEST_TOPIC, Sent ),
    ?assertMatch(ok,Response),

    %% to give time for all batches to flush
    timer:sleep(100),
    kafka_consumer ! {flush, 11, self()},
    receive
        {flush, X}->
            ?assertEqual(Sent, X)
    end,
    ok.

t_produce_async_in_batch_to_topic()->
    Sent = <<"20.async in batch">>,
    Response  = ekaf:produce_async_batched(?TEST_TOPIC, Sent),
    ?assertMatch(ok,
                 Response),
    kafka_consumer ! {flush, 1, self()},
    receive
        {flush, [X]}->
            ?assertEqual(Sent, X)
    end,    ok.

t_produce_sync_multi_in_batch_to_topic()->
    Sent = [ <<(ekaf_utils:itob(X))/binary,".sync multi batch">> || X<- lists:seq(21,31)],
    Response  = ekaf:produce_sync_batched(?TEST_TOPIC, Sent),
    ?assertMatch({buffered,_,_},
                 Response),
    kafka_consumer ! {flush, 11, self()},
    receive
        {flush, X}->
            ?assertEqual(Sent, X)
    end,
    ok.

t_produce_sync_in_batch_to_topic()->
    Sent = <<"32.sync in batch">>,
    Response  = ekaf:produce_sync_batched(?TEST_TOPIC, Sent),
    ?assertMatch({buffered,_,_},
                 Response),
    kafka_consumer ! {flush, 1, self()},
    receive
        {flush, [X]}->
            ?assertEqual(Sent, X)
    end,
    ok.


t_is_clean()->
    ok.


kafka_consumer_loop(Acc,{From,Stop}=Acc2)->
    receive
        {stop, N}->
            kafka_consumer_loop(Acc, {From,N});
        stop ->
            ?debugFmt("kafka_consumer_loop stopping",[]),
            ok;
        {flush, NewFrom} ->
            %?debugFmt("asked to flush when consumer got ~w items",[length(Acc)]),
            % kafka_consumer_loop should flush
            NewFrom ! {flush, Acc },
            kafka_consumer_loop([], {NewFrom,0});
        {flush, NewStop, NewFrom} ->
            % ?debugFmt("~p asked to flush when consumer got ~w items",[ekaf_utils:epoch(), length(Acc)]),
            % kafka_consumer_loop should flush
            NextAcc =
                case length(Acc) of
                    NewStop ->
                        NewFrom ! {flush, Acc },
                        [];
                    _ ->
                        Acc
            end,
            kafka_consumer_loop(NextAcc, {NewFrom,NewStop});
        {info, From} ->
            %kafka_consumer_loop should reply
            From ! {info, Acc },
            kafka_consumer_loop(Acc, Acc2);
        {produce,X} ->
            ?debugFmt("~p kafka_consumer_loop INCOMING ~p",[ekaf_utils:epoch(), length(X)]),
            Next = Acc++X,
            Next2 =
                case length(Next) of
                    Stop ->
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

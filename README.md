# ekaf #

An advanced but simple to use, Kafka client written in Erlang

### Highlights of v0.1 ###
* A minimal implementation of a `Producer` as per 0.8 Kafka wire protocol
* Produce data to a Topic syncronously and asynchronously
* Option to batche messages and customize the concurrency
* Several lazy handling of both metadata requests as well as connection pool creation

ekaf also powers `kafboy`, the webserver based on `Cowboy` capable of publishing to ekaf via simple HTTP endpoints.

Add a feature request at https://github.com/bosky101/ekaf or check the ekaf web server at https://github.com/bosky101/ekafboy

##Features##
###Simple API###

Topic is a binary. and the payload can be a list, a binary, a key-value tuple, or a combination of all the above.

    %%------------------------
    %% Start
    %%------------------------
    application:start(ekaf),

    %% ekaf needs atleast 1 broker to connect.
    %% To eliminate a SPOF, use an IP to a load balancer to a bunch of brokers
    application:set_env(ekaf, {ekaf_bootstrap_broker,{"localhost",9091}}),

    Topic = <<"ekaf">>,

    %%------------------------
    %% Send data to a message
    %%------------------------

    %% sync
    ekaf:produce_sync(Topic, <<"foo">>).

    %% async
    ok = ekaf:produce_async(Topic, jsx:encode(PropList) ).

    %%------------------------
    %% Send in batches
    %%------------------------

    %% send many messages in 1 packet by sending a list to represent a payload
    {sent, Partition, _} =
        ekaf:produce_sync(
            Topic,
            [<<"foo">>, {<<"key">>, <<"value">>}, <<"back_to_binary">> ]
        ).

    %%------------------------
    %% Send in batches
    %%------------------------

    %% sync
    {buffered, _Partition, _BufferIndex} =
        ekaf:produce_sync_batched(
            Topic,
            [ekaf_utils:itob(X) || X<- lists:seq(1,1000) ]
        ).

    %% async
    {buffered, _Partition, _BufferIndex} =
        ekaf:produce_async_batched(
            Topic,
            [<<"foo">>, {<<"key">>, <<"value">>}, <<"back_to_binary">> ]
        ).

    %%------------------------
    %% Other helpers that are used internally
    %%------------------------
    %% reads ekaf_bootstrap_topics. Will also start their workers
    ekaf:prepare(Topic).
    %% metadata
    %% if you don't want to start workers on app start, make sure to
    %%               first get metadata before any produce/publish
    ekaf:metadata(Topic).
    %% pick a worker, and directly communicate with it
    ekaf:pick(Topic,Callback).
    %% see the tests for a complete API, and `ekaf.erl` and `ekaf_lib` for more

See `test/ekaf_tests.erl` for more

### Batch writes ###

You can batch async and sync calls until they reach 1000 for a partition, or 5 seconds of inactivity. Which ever comes first.

    %% default is 1
    {ekaf,[
        {ekaf_max_buffer_size, 100},
        {ekaf_buffer_ttl     , 5000}
    ]}.


You can also set different batch sizes for different topics.

    {ekaf,[{ekaf_max_buffer_size, [
       {<<"topic1">>, 500},            % topic specific
       {<<"topic2">>, 10000},
       {ekaf_max_buffer_size, 1000}    % for remaining topics
    ]}]}.

You can also change the default buffer flush on inactivity from 1 second. Topic specific is again possible.

    {ekaf,[
        {ekaf_partition_strategy, 1000}  % if a buffer exists after 1 sec on inactivity, send them
    ]}.

### Partition choosing strategy

* Random Strategy ( Faster, since order not maintained among partition workers )

![ordered_round_robin](/benchmarks/n30000_c100_strategy_random.png)

Will deliver to kafka ordered within the same partition worker, but produced messages can go to any random partition worker. As a result, ordering finally at the kafka broker cannot be ensured.

* Ordered Round Robin

![ordered_round_robin](/benchmarks/n30000_c100_strategy_sticky_batch.png)

Will attempt to deliver to kafka in the same order than was published by sharding 1000 writes at a time to the same partition worker before switching to another worker. The same partition worker order need not be chosen when run over long durations, but whichever partition worker is picked, its writes will be in order.


### Tunable Concurrency ###
Each worker represents a connection to a broker + topic + partition combination.
You can decide how many workers to start for each partition

    %% a single topic with 3 partitions will have 15 workers per node
    %% all able to batch writes
    {ekaf,[{ekaf_per_partition_workers, 5},
           {ekaf_per_partition_workers_max, 10}]}.

You can also set different concurrency strategies for different topics


    {ekaf,[{ekaf_per_partition_workers, [
       {<<"topic1">>, 5},              % lazily start a connection pool of 5 workers
       {<<"topic2">>, 1},              % per partition
       {ekaf_max_buffer_size, 1000}    % for remaining topics
    ]}]}.

By lazily starting a connection pool, these workers only are spawned on receipt of the first produce message received by that worker. More on this below.

### No need for Zookeeper ###
Does not need a connection to Zookeeper for broker info, etc. This adopts the pattern encouraged from the 0.8 protocol

### No linked drivers, No NIF's   ###
Deals with the protcol completely in erlang. Pattern matching FTW, see the blogpost that inspired this project ( also see https://coderwall.com/p/1lyfxg ).

### Optimized for using on a cluster ###
* Works well if embedding into other OTP/rebar style apps ( eg: tested with `kafboy`)
* Only gets metadata for the topic being published. ekaf does not start workers until a produce is called, hence easily horizontally scalable - can be added to a load balancer without worrying about creating holding up valuable connections to a broker on bootup. Queries metadata directly from a `{ekaf_bootstrap_bhroker,Broker}` during the first produce to a topic, and then caches this data for that topic.
* Extensive use of records for `O(1)` lookup
* By using binary as the preferred format for Topic, etc, - lists are avoided in all places except the {BrokerHost,_Port}.

### Concurrency when publishing to multiple topics via process groups ###
Each Topic, will have a pg2 process group, You can pick a random partition worker for a topic via

    ekaf:pick(<<"topic_foo">>, fun(Worker) ->
        case Worker of
            {error, try_again}->
                %% need to get some metadata first for this topic
                %% see how `ekaf:produce_sync/2` does this
            SomeWorkerPid ->
                %% gen_fsm:sync_send_event(SomeWorker, pool_name)
                %% SomeWorker ! info
        end
    end).

    %% pick/1 and pick/2 also exist for synchronously choosing a worker

### State Machines ###
Each worker is a finite state machine powered by OTP's gen_fsm as opposed to gen_server which is more of a client-server model. Which makes it easy to handle connections breaking, and adding more features in the future. In fact every new topic spawns a worker that first starts in a bootstrapping state until metadata is retrieved. This is a blocking call.

### Powering kafboy, a HTTP gateway to ekaf ###

`kafboy` ( https://github.com/bosky101/kafboy ) sits on top of ekaf and is powered by `Cowboy` to send JSON posted to it directly to kafka

    curl -d a=apple -d b=ball http://localhost:8000/sync/topic1

    POST /sync/topic_name
    POST /async/topic_name

### Benchmarks ###
Running the test on a 2GB RAM vagrant VM, where the broker was local

*Without Batching*

* Roughly 15,000+ async calls per second
* Roughly 500     sync calls per second


*With Batching*

* Roughly 50,000+ messages per second
  ( based on time to send N events in 1 async batch call )

Batching is basically sending several messages at once. The following was calculated when sending 100,0000 messages in 1 batch, where the message was just a incrementally increasing number.

Try out for yourself with the following snippets

    %% test async requests per second
    Test_Async = fun(N) -> Seq = lists:seq(1,N), N1 = now(), [ ekaf:produce_async(<<"ekaf">>, ekaf_utils:itob(X)) || X <- Seq], N2 = now(), N/(timer:now_diff(N2,N1)/1000000) end.

    %% test sync requests per second
    Test_Sync = fun(N) -> N1 = now(), [ ekaf:produce_sync(<<"ekaf">>,ekaf_utils:itob(X)) || X <- lists:seq(1,N)]], N2 = now(), N/(timer:now_diff(N2,N1)/1000000) end.
    #Fun<erl_eval.6.80484245>

    %% test batched async requests per second
    Test_Async_Batched = fun(N) ->N1 = now(), [ ekaf:produce_async_batched(<<"ekaf">>, ekaf_utils:itob(X)) || X <- lists:seq(1,N)], N2 = now(), N/(timer:now_diff(N2,N1)/1000000) end.
    #Fun<erl_eval.6.80484245>

    %% test batched sync requests per second
    Test_Sync_Batched = fun(N) ->N1 = now(), [ ekaf:produce_sync_batched(<<"ekaf">>, ekaf_utils:itob(X)) || X <- lists:seq(1,N) ], N2 = now(), N/(timer:now_diff(N2,N1)/1000000) end.
    #Fun<erl_eval.6.80484245>

    %% test async batched multi requests per second
    Test_Async_Multi_Batched = fun(N) ->N1 = now(),  ekaf:produce_async_batched(<<"ekaf">>, [ ekaf_utils:itob(X) || X <- lists:seq(1,N)]), N2 = now(), N/(timer:now_diff(N2,N1)/1000000) end.
    #Fun<erl_eval.6.80484245>

    (node@127.0.0.1)8> Test_Async_Multi_Batched(1000).
    77954.47458684129
    (node@127.0.0.1)9> Test_Async_Multi_Batched(10000).
    80857.08510208207
    (node@127.0.0.1)10> Test_Async_Multi_Batched(100000).
    471695.88822694233

### Tests ###

    rebar compile eunit skip_deps=true

The tests assume you have a topic `test`. Create it as instructed on the Kafka Quickstart at `http://kafka.apache.org/08/quickstart.html`

    bin/kafka-create-topic.sh --zookeeper localhost:2181 --replica 1 --partition 1 --topic ekaf

    $ rebar compile eunit skip_deps=true

    test/ekaf_tests.erl:24:<0.245.0>: t_pick_from_new_pool ( ) = ok
    test/ekaf_tests.erl:26:<0.265.0>: t_request_metadata ( ) = ok
    test/ekaf_tests.erl:29:<0.269.0>: t_publish_sync_to_topic ( ) = ok
    test/ekaf_tests.erl:31:<0.273.0>: t_publish_sync_multi_to_topic ( ) = ok
    test/ekaf_tests.erl:33:<0.277.0>: t_publish_sync_in_batch_to_topic ( ) = ok
    test/ekaf_tests.erl:95:<0.281.0>: t_publish_many_sync_in_batch_to_topic: {buffered,1,2}
    test/ekaf_tests.erl:35:<0.281.0>: t_publish_sync_multi_in_batch_to_topic ( ) = ok
    test/ekaf_tests.erl:38:<0.285.0>: t_publish_async_to_topic ( ) = ok
    test/ekaf_tests.erl:40:<0.289.0>: t_publish_async_multi_to_topic ( ) = ok
    test/ekaf_tests.erl:112:<0.293.0>: t_publish_async_in_batch_to_topic: ok
    test/ekaf_tests.erl:42:<0.293.0>: t_publish_async_in_batch_to_topic ( ) = ok
    test/ekaf_tests.erl:119:<0.297.0>: t_publish_many_async_in_batch_to_topic: ok
    test/ekaf_tests.erl:44:<0.297.0>: t_publish_async_multi_in_batch_to_topic ( ) = ok
    All 20 tests passed.
    Cover analysis: /data/repos/ekaf/.eunit/index.html

    Code Coverage:
    ekaf                   : 66%
    ekaf_fsm               : 70%
    ekaf_lib               : 34%
    ekaf_protocol          : 70%
    ekaf_protocol_metadata : 78%
    ekaf_protocol_produce  : 50%
    ekaf_sup               : 30%
    ekaf_utils             : 14%
    ekafka                 :  0%
    ekafka_app             :  0%
    ekafka_connection      :  0%
    ekafka_connection_sup  :  0%
    ekafka_protocol        :  0%
    ekafka_sup             :  0%

    Total                  : 37%

# License

```
Copyright 2014, Helpshift, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

### Goals for v0.2 ###
* Explicit Partition choosing strategies ( eg: round robin, hash, leader under low load, etc )
* Compression when publishing
* Add a feature request at https://github.com/bosky101/ekaf or check the ekaf web server at https://github.com/bosky101/ekafboy

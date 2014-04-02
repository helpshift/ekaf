==ekaf==

A Kafka client written in erlang.

* Conforms to the 0.8 Kafka wire protocol
* Produce data to a Topic syncronously and asynchronously
* Publish in batches
* Query for a Topic's metadata from a bootstrapped {Broker, Port}

==Features==
===Simple API===

    application:start(ekaf),
    application:set_env(ekaf, {ekaf_bootstrap_broker,{"localhost",9091}}),

    %% send 1 message
    ekaf:publish_sync(<<"test">>, <<"foo">>).
    %% send a message asynchronously
    ekaf:publish_async(<<"test">>, <<"{\"a\":1}">>).

    %% send many Messages
    %% -include_lib("ekaf/include/ekaf_definitions.hrl").
    ekaf:publish_sync(<<"test">>, [<<"foo">>, {<<"key">>, <<"value">>}, <<"back_to_binary">> ]).

See `test/ekaf_tests.erl` for more

===HTTP to Kafka gateway===

[https://github.com/bosky101/ekafboy https://github.com/bosky101/ekafboy] sits on top of ekaf and is powered by `Cowboy` to send JSON posted to it directly to kafka

    curl -d a=apple -d b=ball http://localhost:8000/sync/topic1

    POST /sync/topic_name
    POST /async/topic_name

===No need for Zookeeper===
Does not need a connection to Zookeeper for broker info, etc. This adopts the pattern encouraged from the 0.8 protocol

===No linked drivers, No NIF's  ===
Deals with the protcol completely in erlang. Pattern matching FTW, ( also see [https://coderwall.com/p/1lyfxg https://coderwall.com/p/1lyfxg] )

===Optimized for using on a cluster===
Only gets metadata for the topic being published. ekaf does not start workers until a produce is called, hence easily horizontally scalable - can be added to a load balancer without worrying about creating holding up valuable connections to a broker on bootup. Queries metadata directly from a `{ekaf_bootstrap_bhroker,Broker}` during the first produce to a topic, and then caches this data for that topic.

===Concurrency when publishing to the same Topic via Connection Pool===
Each worker represents a connection to a broker + topic + partition combination.
Uses `poolboy` to implement a connection pool for each partition for a topic. Workers are brought back up via poolboy

===Concurrency when publishing to multiple topics via process groups===
Each Topic, will have a pg2 process group, You can pick a random partition worker for a topic via

    case ekaf:pick(<<"topic_foo">>) of
        {error, try_again}->
            %% need to get some metadata first for this topic
            %% see how `ekaf:produce_sync/2` does this
        SomeWorkerPid ->
            %% gen_fsm:sync_send_event(SomeWorker, pool_name)
            %% SomeWorker ! info
    end

===State Machines===
Each worker is a finite state machine powered by OTP's gen_fsm as opposed to gen_server which is more of a client-server model. Which makes it easy to handle connections breaking, and adding more features in the future. In fact every new topic spawns a worker that first starts in a bootstrapping state until metadata is retrieved. This is a blocking call.

===Tests===

    rebar compile eunit skip_deps=true

The tests assume you have a topic `test`. Create it as instructed on the Kafka Quickstart at `http://kafka.apache.org/08/quickstart.html`

    bin/kafka-create-topic.sh --zookeeper localhost:2181 --replica 1 --partition 1 --topic test

===Benchmarks===
Against a local broker

* Roughly 15,000+ async calls per second
* Roughly 500     sync calls per second

Here's how you can test it out yourself

**Async**

    (node@127.0.0.1)28> Rps = fun(N) ->N1 = now(),[ ekaf:publish(<<"test8">>,<<"a">>) || _X <- lists:seq(1,N)], N2 = now(), N/(timer:now_diff(N2,N1)/1000000) end.
    #Fun<erl_eval.6.80484245>
    (node@127.0.0.1)32> Rps(100).
    16371.971185330714
    (node@127.0.0.1)31> Rps(1000).
    13715.72782509704
    (node@127.0.0.1)30> Rps(10000).
    16077.816632501308
    (node@127.0.0.1)29> Rps(100000).
    16720.88692934957

**Sync**

(node@127.0.0.1)33> SRps = fun(N) ->N1 = now(),[ ekaf:produce_sync(<<"test8">>,<<"a">>) || _X <- lists:seq(1,N)], N2 = now(), N/(timer:now_diff(N2,N1)/1000000) end.
    #Fun<erl_eval.6.80484245>
    (node@127.0.0.1)34> SRps(100).
    485.1707558475206
    (node@127.0.0.1)35> SRps(1000).
    473.421411808929
    (node@127.0.0.1)36> SRps(10000).
    491.2887383012484
    (node@127.0.0.1)37> SRps(100000).
    532.82974528447

==Coming in 0.2==
* Explicit Partition choosing strategies ( eg: round robin, hash, leader under low load, etc )

==Coming in 0.3==
* Compression when publishing

Add a feature request at https://github.com/bosky101/ekaf or check the ekaf web server at https://github.com/bosky101/ekafboy

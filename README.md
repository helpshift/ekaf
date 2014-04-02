==ekaf==

A Kafka 0.8 client written in erlang.

* Query for a Topic's metadata from a bootstrapped {Broker, Port}
* Produce data to a Topic

==Features==
===Simple API===

    application:start(ekaf),
    application:set_env(ekaf, {ekaf_bootstrap_broker,{"localhost",9091}}),
    %% send 1 message
    ekaf:publish_sync(<<"test8">>, <<"foo">>).
    %% send many Messages
    %% -include_lib("ekaf/include/ekaf_definitions.hrl").
    ekaf:publish_sync(<<"test8">>, [<<"foo">>, <<"bar">>]).

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
The tests assume you have a topic `test`. Create it as instructed on the Kafka Quickstart at `http://kafka.apache.org/08/quickstart.html`

    bin/kafka-create-topic.sh --zookeeper localhost:2181 --replica 1 --partition 1 --topic test

==Coming in 0.2==
* Batching ( currently a publish only sends 1 message)
* Explicit Partition choosing strategies ( eg: round robin, hash, leader under low load, etc )

==Coming in 0.3==
* Compression when publishing

%%
%% ekaf_demo
%%
%% useful for testing with a running kafkamocker. first start a kafka broker
%% terminal1 : cd ekaf/deps/kafkamocker
%% rebar compile skip_deps=true && erl -pa ebin -pa deps/*/ebin -s kafkamocker_demo
%%
%% then lets connect to it
%% terminal2 : cd ekaf
%% rebar compile skip_deps=true && erl -pa ebin -pa deps/*/ebin -s ekaf_demo
%%
-module(ekaf_demo).

%% NOTE: You may use include_lib("ekaf/include/ekaf_definitions.hrl").
%% for your callbacks
-include("ekaf_definitions.hrl").

-export([start/0, test/0, demo/0, demo_callback/5, tuple_to_statsd_key/1]).

%% instrumenting functions arent used, but you may use as you feel fit
-export([get_workers/1, get_dead_workers/1, get_info/1, get_info/2, get_ekaf_server_message_queue_size/1, instrument_topics/1]).

-define(MAX_TEST_MESSAGES,20).
-define(TIME_BETWEEN_MESSAGE, 1000).

start()->
    %% setup the config, and start ekaf
    demo(),

    %% send a few messages
    test().

test()->
    [ slow_loop1(X) || X <- lists:seq(1,?MAX_TEST_MESSAGES) ].

slow_loop1(X)->
    io:format("~n request ~p",[X]),
    ekaf:produce_async_batched(<<"ekaf">>, ekaf_utils:itob(X) ),
    timer:sleep(?TIME_BETWEEN_MESSAGE),
    ok.

demo()->
    %% SET CALLBACKS
    %% here are the various callbacks that you can listen to
    %% more info in the include/ekaf_definitions.hrl file
    application:set_env(ekaf, ?EKAF_CALLBACK_FLUSH,  {ekaf_demo, demo_callback}),
    application:set_env(ekaf, ?EKAF_CALLBACK_FLUSHED_REPLIED, {ekaf_demo, demo_callback}),
    application:set_env(ekaf, ?EKAF_CALLBACK_WORKER_DOWN, {ekaf_demo, demo_callback}),
    application:set_env(ekaf, ?EKAF_CALLBACK_WORKER_UP, {ekaf_demo, demo_callback}),
    application:set_env(ekaf, ?EKAF_CALLBACK_DOWNTIME_SAVED, {ekaf_demo, demo_callback}),
    application:set_env(ekaf, ?EKAF_CALLBACK_DOWNTIME_REPLAYED, {ekaf_demo, demo_callback}),
    application:set_env(ekaf, ?EKAF_CALLBACK_TIME_TO_CONNECT, {ekaf_demo, demo_callback}),
    application:set_env(ekaf, ?EKAF_CALLBACK_TIME_DOWN, {ekaf_demo, demo_callback}),

    %% SET STRATEGY AND CONCURRENCY OPTIONS
    application:set_env(ekaf, ekaf_partition_strategy, strict_round_robin),
    application:set_env(ekaf, ekaf_per_partition_workers, 2),
    application:set_env(ekaf, ekaf_per_partition_workers_max, 10),

    % POINT TO KAFKA

    % point to kafkamocker
    application:set_env(ekaf, ekaf_bootstrap_broker, {"localhost", 9907}),

    % uncomment to point to an actual kafka broker, eg: on port 9091
    % application:set_env(ekaf, ekaf_bootstrap_broker, {"localhost", 9091}),

    application:set_env(ekaf, ekaf_buffer_ttl, 1000),
    application:ensure_started(kafkamocker),
    [ application:start(App) || App <- [gproc, ranch, ekaf] ].

demo_callback(Event, _From, _StateName,
                #ekaf_fsm{ topic = Topic, broker = _Broker, partition = PartitionId, last_known_size = BufferLength, cor_id = CorId, leader = Leader},
                Extra)->
    Stat = <<Topic/binary,".",  Event/binary, ".broker", (ekaf_utils:itob(Leader))/binary, ".", (ekaf_utils:itob(PartitionId))/binary>>,
    case Event of
        ?EKAF_CALLBACK_FLUSH ->
            io:format("~n ~s ~w",[Stat, BufferLength]),
            %io:format("~n ~p flush broker~w#~p when size was ~p corid ~p via:~p",[Topic, Leader, PartitionId, BufferLength, CorId, _From]);
            ok;
        ?EKAF_CALLBACK_FLUSHED_REPLIED ->
            case Extra of
                {ok, {{replied, _, _}, #produce_response{ cor_id = ReplyCorId }} }->
                    Diff = case (CorId - ReplyCorId  ) of Neg when Neg < 0 -> 0; SomeDiff -> SomeDiff end,
                    FinalStat = <<Stat/binary,".diff">>,
                    io:format("~n~s ~w",[FinalStat, Diff]);
                _ ->
                    ?INFO_MSG("ekaf_fsm callback got ~p some:~p ~nextra:~p",[Event, Extra])
            end;
        ?EKAF_CALLBACK_WORKER_UP ->
            io:format("~n ~s 1",[Stat]),
            ok;
        ?EKAF_CALLBACK_WORKER_DOWN ->
            io:format("~n ~s 1",[Stat]),
            ok;
        ?EKAF_CALLBACK_TIME_TO_CONNECT ->
            case Extra of
                {ok, Micros}->
                    io:format("~n ~s => ~p",[Stat, ekaf_utils:ceiling(Micros/1000)]);
                _ ->
                    ok
            end;
        _ ->
            ?INFO_MSG("ekaf_fsm callback got ~p ~p",[Event, Extra])
    end;

demo_callback(Event, _From, StateName,
                #ekaf_server{ topic = Topic },
                Extra)->
    Stat = <<Topic/binary,".",  Event/binary>>,
    case Event of
        ?EKAF_CALLBACK_DOWNTIME_SAVED ->
            io:format("~n ~s => 1",[Stat]),
            ok;
        ?EKAF_CALLBACK_DOWNTIME_REPLAYED ->
            io:format("~n ~s => 1 during ~p",[Stat, StateName]),
            ok;
        ?EKAF_CALLBACK_TIME_DOWN ->
            case Extra of
                {ok, Micros}->
                    io:format("~n ~s => ~p",[Stat, ekaf_utils:ceiling(Micros/1000)]);
                _ ->
                    ok
            end;
        ?EKAF_CALLBACK_WORKER_DOWN ->
            FinalStat = <<Topic/binary,".mainbroker_unreachable">>,
            io:format("~n ~s => 1",[FinalStat]),
            ok;
        _ ->
            ?INFO_MSG("ekaf_server callback got ~p ~p",[Event, Extra])
    end.

%%====================================================================
%% Internal helper functions
%%====================================================================
% loop1(X)->
%     io:format("~n request ~p",[X]),
%     ekaf:produce_async_batched(<<"ekaf">>, ekaf_utils:itob(X) ),
%     ok.

% loop2(X)->
%     timer:sleep(1000),
%     [begin
%          timer:sleep(1000),
%          spawn(fun()->
%                        loop1(X*Y)
%                end)
%      end
%      || Y <- lists:seq(1,?MAX)].

tuple_to_statsd_key({X,Y})->
    <<(tuple_to_statsd_key(X))/binary,".", (tuple_to_statsd_key(Y))/binary>>;
tuple_to_statsd_key(X) when is_atom(X)->
    tuple_to_statsd_key(atom_to_list(X));
tuple_to_statsd_key(X) when is_list(X)->
    tuple_to_statsd_key(ekaf_utils:atob(X));
tuple_to_statsd_key(X) when is_integer(X)->
    tuple_to_statsd_key(ekaf_utils:itob(X));
tuple_to_statsd_key(X) when is_binary(X) ->
    X.


%%====================================================================
%% Public helper functions
%%====================================================================
get_workers(Topic)->
    pg2l:get_local_members(Topic).

get_dead_workers(Topic) when is_binary(Topic)->
    Workers = get_workers(Topic),
    ?MODULE:get_dead_workers(Workers);
get_dead_workers(Workers)->
    lists:foldl(fun(Worker,Acc)->
                        case erlang:is_process_alive(Worker) of
                            true ->
                               Acc;
                            _ ->
                                Acc+1
                        end
                end, 0, Workers).

get_info(Topic) when is_binary(Topic)->
    Workers = get_workers(Topic),
    ?MODULE:get_info(Workers,info).
get_info(Topic,Field) when is_binary(Topic)->
    Workers = get_workers(Topic),
    ?MODULE:get_info(Workers,Field);
get_info(Workers,info=Field) ->
    lists:foldl(fun(Worker,Acc)->
                        State = gen_fsm:sync_send_event(Worker, info),
                        [get_info_from_state(State,Field)| Acc]
                end, [], Workers);
get_info(Workers,Field)->
    lists:foldl(fun(Worker,Acc)->
                        State = gen_fsm:sync_send_event(Worker, info),
                        Topic = State#ekaf_fsm.topic, Partition = State#ekaf_fsm.partition,
                        [{{<<"ekaf_sup">>, <<Topic/binary,".",(ekaf_utils:itob(Partition))/binary,".",Field/binary>>},
                          get_info_from_state(State,Field)}|Acc]
                end, [], Workers).

get_info_from_state(State, <<"buffer.size">>)->
    State#ekaf_fsm.last_known_size;
get_info_from_state(State, <<"buffer.max">>) ->
    State#ekaf_fsm.max_buffer_size;
get_info_from_state(State, _)->
    State.

get_ekaf_server_message_queue_size(Topic)->
    TopicPid = gproc:where({n,l,Topic}),
    case TopicPid of
        undefined ->
            -1;
        _ ->
            {_,N} = erlang:process_info(TopicPid, message_queue_len),
            N
    end.

instrument_topics(Topics) ->
    lists:foldl(fun(Topic,Acc)->
                        Workers = ekaf_stats:get_workers(Topic),
                        StatsPL = supervisor:count_children(ekaf_sup),
                        [
                         {<<"ekaf_sup/workers">>, proplists:get_value(workers, StatsPL)},
                         {<<"ekaf_sup/children">>, length(Workers)},
                         {<<"ekaf_sup/workers.dead">>, ekaf_stats:get_dead_workers(Workers)},
                         {<<"ekaf_sup/",Topic/binary,".message_queue">>, ekaf_stats:get_ekaf_server_message_queue_size(Topic)}
                        ]
                        ++ Acc
                end, [], Topics).

-module(ekaf_stats).

%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-include("ekaf_definitions.hrl").

-export([
         get_workers/1, get_dead_workers/1, get_info/1, get_info/2, get_ekaf_server_message_queue_size/1,

         instrument_topics/1
        ]).

get_workers(Topic)->
    pg2:get_local_members(Topic).

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

-module(ekaf_fsm).

-behaviour(gen_fsm).
%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-include("ekaf_definitions.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").
-endif.

-define(HIBERNATE_TIMEOUT, undefined).
-define(KEEPALIVE_INTERVAL, 60*1000).
%%--------------------------------------------------------------------
%% External exports
-export([start_link/1, init/1]).

%% gen_fsm callbacks
-export([
         handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).
%% states
-export([init/2, init/3,
         downtime/2, downtime/3,
         connected/2, connected/3,       %% Connection with the broker established
         bootstrapping/2, bootstrapping/3,  %% Asked for metadata and waiting
         ready/2, ready/3,    %% Got metadata and ready to send
         draining/2, draining/3,
         fsm_next_state/2, fsm_next_state/3
         ]).

%%====================================================================
%% External functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link/0
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Args)->
    gen_fsm:start_link(?MODULE,Args,
                       []
                       %[{debug, [trace,statistics]}]
       ).

%%====================================================================
%% Server functions
%%====================================================================
%%--------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok, StateName, StateData}          |
%%          {ok, StateName, StateData, Timeout} |
%%          ignore                              |
%%          {stop, StopReason}
%%--------------------------------------------------------------------
init([ReplyTo, Broker, Topic]) ->
    ?INFO_MSG("init/1 3",[]),
    State = #ekaf_fsm{
      topic = Topic,
      broker = Broker,
      reply_to = ReplyTo
     },

    ekaf_sup:start_child(ekaf_sup,
                         {Topic, {ekaf_server, start_link, [[Topic]]},
                          permanent, infinity, worker, []}
                        ),

    gen_fsm:send_event(self(), connect),
    Created = pg2:create(Topic),
    Joined = pg2:join(Topic,self()),
    gproc:send({n,l,Topic}, {set, worker, self()}),
    ?INFO_MSG("i am alive, reply to ~p, created: ~p, joined: ~p",[ReplyTo, Created, Joined]),
    State#ekaf_fsm.reply_to ! {ready,[]},
    {ok, init, State};

init([PoolName, Broker, Topic, Leader, Partition, From]=_Args) ->
    ?INFO_MSG("init/1 6 ~p Broker ~p for leader ~p partition ~p",[Topic,Broker,Leader,Partition]),
    case ekaf_lib:open_socket(Broker) of
        {ok,Socket} ->
            PartitionPacket = #partition{
              id = Partition,
              leader = Leader
              %% each messge goes in a different messageset, even for batching
             },
            TopicPacket = #topic{
              name = Topic,
              partitions =
              [PartitionPacket]},
            ProducePacket = #produce_request{
              timeout=100,
              topics= [TopicPacket]
             },
            BufferTTL = ekaf_lib:get_buffer_ttl(Topic),
            gen_fsm:start_timer(BufferTTL,<<"refresh">>),
            State = #ekaf_fsm{
              pool = PoolName,
              topic = Topic,
              broker = Broker,
              leader = Leader,
              partition = Partition,
              socket = Socket,
              max_buffer_size = ekaf_lib:get_max_buffer_size(Topic),
              buffer_ttl = BufferTTL,
              kv = dict:new(),
              partition_packet = PartitionPacket,
              topic_packet = TopicPacket,
              produce_packet = ProducePacket,
              flush_callback = ekaf_lib:get_callbacks(flush),
              drain = From
             },
            ?INFO_MSG("move to ready",[]),
            gen_fsm:send_event(self(), ping),
            {ok, ready, State};
        {error, Reason} ->
            ?ERROR_MSG("~p",[Reason]),
            {stop, Reason}
    end.

%%--------------------------------------------------------------------
%% Func: StateName/2
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%%--------------------------------------------------------------------
init(connect, #ekaf_fsm{ broker = Broker } = State)->
    case ekaf_lib:open_socket(Broker) of
        {ok,Socket} ->
            ?INFO_MSG("connection good, ask for metadata",[]),
            gen_fsm:send_event(self(), {metadata, State#ekaf_fsm.topic}),
            fsm_next_state(connected, State#ekaf_fsm{ socket = Socket });
        {error, _Reason} ->
            gen_fsm:start_timer(1000,<<"reconnect">>),
            fsm_next_state(downtime, State)
    end.

connected({metadata,_Topic}=Event, State) ->
    ekaf_lib:handle_connected(Event, State);

connected(_Event, State)->
    ekaf_lib:handle_continue_when_not_ready(connected,_Event,State).

downtime({produce_async, Messages}, #ekaf_fsm{ messages = OfflineMessages } = State)->
    ?INFO_MSG("add ~p to offline q ~p",[Messages, OfflineMessages]),
    fsm_next_state(downtime, State#ekaf_fsm{ messages = [Messages|OfflineMessages] } );

downtime({timeout, Timer, <<"reconnect">>}, State)->
    gen_fsm:cancel_timer(Timer),
    gen_fsm:send_event(self(), connect),
    fsm_next_state(init, State);

downtime(Event, State) ->
    ?INFO_MSG("downtime cant handle ~p",[Event]),
    fsm_next_state(downtime, State).

% {metadata,0,
%       [{broker,2,<<"vagrant-ubuntu-precise-64">>,9092},
%        {broker,1,<<"vagrant-ubuntu-precise-64">>,9091},
%        {broker,3,<<"vagrant-ubuntu-precise-64">>,9093}],
%       [{topic,<<"a3">>,undefined,
%               [{partition,1,0,1,
%                           [{replica,2},{replica,1}],
%                           [{isr,2},{isr,1}]},
%                {partition,0,0,3,
%                           [{replica,1},{replica,3}],
%                           [{isr,1},{isr,3}]}]},
%        {topic,<<"a2">>,undefined,
%               [{partition,1,0,2,[{replica,2}],[{isr,2}]},
%                {partition,0,0,1,[{replica,1}],[{isr,1}]}]},
%        {topic,<<"a1">>,undefined,
%               [{partition,1,0,1,[{replica,1}],[{isr,1}]},
%                {partition,0,0,3,[{replica,3}],[{isr,3}]}]}]}
bootstrapping({metadata,Metadata}, State)->
    ekaf_lib:handle_metadata_during_bootstrapping({metadata,Metadata}, State);
bootstrapping(_Event, State)->
    ekaf_lib:handle_continue_when_not_ready(bootstrapping,_Event,State).

ready({produce_async, _Messages} = Async, PrevState)->
    ?INFO_MSG("in ready, send ~p",[_Messages]),
    ekaf_lib:handle_async_as_batch(false, Async, PrevState);
ready({produce_async_batched, _Messages}= Async, PrevState)->
    ekaf_lib:handle_async_as_batch(true, Async, PrevState);
ready(ping, #ekaf_fsm{ topic = Topic, drain = DrainPid } = State)->
    pg2:join(Topic,self()),
    gproc:send({n,l,Topic}, {set, worker, self()}),
    ?INFO_MSG("drainpid: ~p",[DrainPid]),
    % OfflineMessages = case (catch gen_fsm:sync_send_event(DrainPid, messages)) of
    %                       {'EXIT',_}->[];
    %                       _OffMsgs -> _OffMsgs
    %                   end,
    % ?INFO_MSG("offline: ~p",[OfflineMessages]),
    % case OfflineMessages of
    %     [] ->
    %         ok;
    %     _ ->
    %         ?INFO_MSG("drain ~p messages ",[OfflineMessages]),
    %         ekaf:publish(Topic, OfflineMessages)
    % end,
    fsm_next_state(ready,State);
ready({timeout, Timer, <<"refresh">>}, #ekaf_fsm{ buffer = Buffer, max_buffer_size = MaxBufferSize, buffer_ttl = BufferTTL, cor_id = PrevCorId, last_known_size = LastKnownSize} = PrevState)->
    Len = length(Buffer),
    %% if no activity for BufferTTL ms, then flush
    {NextTTL,ToBuffer} = case Len of
                   0 ->
                       {BufferTTL,true};
                   Curr when LastKnownSize =:= Curr, Curr =/= 0 ->
                       % tobuffer is now false since unchanged",[]),
                       {BufferTTL,false};
                   _ when Len > MaxBufferSize ->
                       % tobuffer is now false since reached batch size
                       {BufferTTL-10,false};
                   _ ->
                       {BufferTTL,true}
               end,
    CorId = case PrevCorId of Big when Big > 2000000000 -> 0;_ -> PrevCorId end,
    %% if no activity for BufferTTL ms, then flush
    State = case ToBuffer of
               false ->
                    ekaf_lib:flush(PrevState);
                _ ->
                    PrevState
            end,
    gen_fsm:cancel_timer(Timer),
    gen_fsm:start_timer(NextTTL,<<"refresh">>),
    fsm_next_state(ready, State#ekaf_fsm{ to_buffer = true, last_known_size = Len, cor_id = CorId });
ready(_Event, State)->
    fsm_next_state(ready,State).

draining(stop, State)->
    ?INFO_MSG("drain stopping",[]),
    {stop, normal, State};
draining(Event, State) ->
    ?INFO_MSG("drain cant handle ~p",[Event]),
    fsm_next_state(draining, State).
%%--------------------------------------------------------------------
%% Func: StateName/3
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
%%--------------------------------------------------------------------

init(_Event, _From, State)->
    fsm_next_state(init, State).
downtime({produce_sync, Messages}, _From, #ekaf_fsm{ messages = OfflineMessages } = State)->
    ?INFO_MSG("add ~p to offline q ~p",[Messages, OfflineMessages]),
    Next = State#ekaf_fsm{ messages = [Messages|OfflineMessages] },
    {reply, {downtime, length(State#ekaf_fsm.messages)}, downtime, Next};
downtime(_Event, _From, State)->
    fsm_next_state(downtime, State).
ready({metadata, Topic}, From, State)->
    ekaf_lib:handle_metadata_during_ready({metadata,Topic}, From, State);
ready({produce_sync, _Messages}=Sync, From, PrevState)->
    ekaf_lib:handle_sync_as_batch(false, Sync, From, PrevState);
ready({produce_sync_batched, _} = Sync, From, PrevState)->
    ekaf_lib:handle_sync_as_batch(true, Sync, From, PrevState);
ready(pool_name, _From, State) ->
    Reply = ekaf_lib:pool_name(State),
    {reply, Reply, ready, State};
ready({set,max_buffer_size,N}, _From, State) ->
    Reply = {State#ekaf_fsm.max_buffer_size, N},
    {reply, Reply, ready, State};
ready(buffer_size, _From, State) ->
    Reply = length( State#ekaf_fsm.buffer),
    {reply, Reply, ready, State};
ready(info, _From, State) ->
    Reply = State,
    {reply, Reply, ready, State};
ready(_Unknown, _From, State) ->
    Reply = ok,
    {reply, Reply, ready, State}.

draining(messages, _From, State)->
    ?INFO_MSG("asked to drain messages, so reply then stop",[]),
    Reply = State#ekaf_fsm.messages,
    gen_fsm:send_event(self(), stop),
    {reply, Reply, draining, State#ekaf_fsm{ messages = [] }}.

connected(Event, From, State)->
    ekaf_lib:handle_reply_when_not_ready(connected, Event, From, State).
bootstrapping(Event, From, State)->
    ekaf_lib:handle_reply_when_not_ready(bootstrapping, Event, From, State).
%%--------------------------------------------------------------------
%% Func: handle_event/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%%--------------------------------------------------------------------
handle_event(_Event, StateName, StateData) ->
    fsm_next_state(StateName, StateData).

%%--------------------------------------------------------------------
%% Func: handle_sync_event/4
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% Func: handle_info/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%%--------------------------------------------------------------------
handle_info({tcp, _Port, <<CorrelationId:32,_/binary>> = Packet}, ready, #ekaf_fsm{ kv = KV, messages = OfflineMessages } = State) ->
    Found = dict:find({cor_id,CorrelationId}, KV),
    Next = case Found of
               {ok,[{Type,From}|_Rest]}->
                   case Type of
                       ?EKAF_PACKET_DECODE_PRODUCE ->
                           Reply = {{sent, State#ekaf_fsm.partition, self()},
                                    ekaf_protocol:decode_produce_response(Packet)},
                           gen_fsm:reply(From,Reply);
                       _TE ->
                           ?INFO_MSG("EKAF got ~p so ignore",[_TE]),
                           ok
                   end,
                   State#ekaf_fsm{ kv = dict:erase({cor_id,CorrelationId}, KV) };
               _E->
                   ?INFO_MSG("found ~p so ignore",[_E]),
                   State
           end,

    %time to flush q
    ?INFO_MSG("flush ~p",[OfflineMessages]),
    fsm_next_state(ready, Next);
handle_info({tcp_closed,Socket}, ready, State)->
    ?INFO_MSG("tcp_closed detected",[]),
    ekaf_lib:close_socket(Socket),
    gen_fsm:start_timer(1000, <<"reconnect">>),
    fsm_next_state(downtime, State#ekaf_fsm{ socket = undefined });

handle_info(Info, StateName, State) ->
    ?INFO_MSG("got info at ~p ~p during ~p",[os:timestamp(), Info, StateName]),
    fsm_next_state(StateName, State).

%%--------------------------------------------------------------------
%% Func: terminate/3
%% Purpose: Shutdown the fsm
%% Returns: any
%%--------------------------------------------------------------------
terminate(_Reason, _StateName,  #ekaf_fsm{ topic = Topic } = State)->
    case State#ekaf_fsm.socket of
        undefined ->
            ok;
        _ ->
            ekaf_lib:close_socket(State#ekaf_fsm.socket)
    end,
    io:format("~n ~p stopping since ~p",[self(), _Reason]),
    pg2:leave(Topic,self()),
    ok.
%%--------------------------------------------------------------------
%% Func: code_change/4
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState, NewStateData}
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
fsm_next_state(StateName, StateData)->
    {next_state, StateName, StateData}.

fsm_next_state(StateName, StateData, Timeout)->
    {next_state, StateName, StateData, Timeout}.

-module(ekaf_fsm).

-behaviour(gen_fsm).
%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-include("ekaf_definitions.hrl").
-include_lib("eunit/include/eunit.hrl").

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
         connecting/2, connecting/3,
         ready/2, ready/3,    %% Got metadata and ready to send
         downtime/2, downtime/3,
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
init([WorkerId, PoolName, Metadata, Broker, Topic, Leader, Partition]) ->
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
    StatsSocket = ekaf_lib:open_socket_if_statsd_enabled(Topic),
    State = #ekaf_fsm{
      id = WorkerId,
      pool = PoolName,
      metadata = Metadata,
      topic = Topic,
      broker = Broker,
      leader = Leader,
      partition = Partition,
      max_buffer_size = ekaf_lib:get_max_buffer_size(Topic),
      buffer_ttl = BufferTTL,
      kv = dict:new(),
      partition_packet = PartitionPacket,
      topic_packet = TopicPacket,
      produce_packet = ProducePacket,
      time = os:timestamp(),
      statsd_socket = StatsSocket
     },
    reconnection_attempt(),
    {ok, connecting, State}.

%%--------------------------------------------------------------------
%% Func: StateName/2
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%%--------------------------------------------------------------------
init(_Event, State)->
    ?INFO_MSG("init/2 cant handle ~p",[_Event]),
    fsm_next_state(init, State).

connecting(connect, #ekaf_fsm{broker = Broker, buffer_ttl = BufferTTL, time = T1} = State)->
    case ekaf_socket:open(Broker) of
        {ok,Socket} ->
            T2 = os:timestamp(),
            ekaf_callbacks:call(?EKAF_CALLBACK_TIME_TO_CONNECT_ATOM,
                                ?EKAF_CALLBACK_TIME_TO_CONNECT,
                                self(), connecting, State, {ok,timer:now_diff(T2, T1)}),
            gen_fsm:send_event(self(), ping),
            gen_fsm:start_timer(BufferTTL,<<"refresh">>),
            fsm_next_state(ready, State#ekaf_fsm{ socket = Socket });
        {error, _Reason} ->
            {stop, normal, State}
            %ekaf_callbacks:worker_down_callback(self(), connecting, State, Reason),
            %gen_fsm:start_timer(1000,connect),
            %fsm_next_state(connecting, State)
    end;
connecting({timeout, Timer, connect}, State)->
    gen_fsm:cancel_timer(Timer),
    gen_fsm:start_timer(1000,connect),
    reconnection_attempt(),
    fsm_next_state(connecting, State);
connecting(Event, State) ->
    ?INFO_MSG("connecting/2 cant handle ~p",[Event]),
    fsm_next_state(connecting, State).


ready({produce_async, _Messages} = Async, PrevState)->
    ekaf_lib:handle_async_as_batch(false, Async, PrevState);
ready({produce_async_batched, _Messages}= Async, PrevState)->
    ekaf_lib:handle_async_as_batch(true, Async, PrevState);
ready(ping, #ekaf_fsm{ topic = Topic } = State)->
    PrefixedTopic = ?PREFIX_EKAF(Topic),
    pg2:join(PrefixedTopic,self()),
    gproc:send({n,l,PrefixedTopic}, {worker, up, self(), ready, State, undefined}),
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
                       {BufferTTL,false}
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
ready({stop, _Reason}, State) ->
    fsm_next_state(downtime, State);
ready(_Event, State)->
    fsm_next_state(ready,State).

downtime({timeout, Timer, <<"refresh">>}, State)->
    gen_fsm:cancel_timer(Timer),
    fsm_next_state(downtime, State);
downtime(Event, #ekaf_fsm{ topic = Topic } = State)->
    case (catch gproc:where({n,l,?PREFIX_EKAF(Topic)})) of
        Standby when is_pid(Standby)->
            ?INFO_MSG("downtime/2 cant handle ~p, send to ~p",[Event, Standby]),
            gen_fsm:send_event(Standby, Event);
        _ ->
            ok
    end,
    fsm_next_state(downtime, State).


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
    ?INFO_MSG("init/3 cant handle ~p",[_Event]),
    fsm_next_state(init, State).

connecting(Event, _From, State) ->
    ?INFO_MSG("connecting/3 cant handle ~p",[Event]),
    fsm_next_state(connecting, State).

ready(metadata, From, State)->
    ekaf_lib:handle_metadata_during_ready(From, State);
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
ready(partition, _From, State)->
    Reply = State#ekaf_fsm.partition,
    {reply, Reply, ready, State};
ready(info, _From, State) ->
    Reply = State,
    {reply, Reply, ready, State};
ready(kv, _From, State) ->
    Reply = State#ekaf_fsm.kv,
    {reply, Reply, ready, State};
ready(_Unknown, _From, State) ->
    ?INFO_MSG("ready/3 cant handle ~p",[_Unknown]),
    Reply = ok,
    {reply, Reply, ready, State}.

downtime(Event, _From, State)->
    ?INFO_MSG("downtime/3 doesnt know to handle ~p",[Event]),
    fsm_next_state(downtime, State).

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
handle_incoming(<<CorrelationId:32,_/binary>> = Packet, #ekaf_fsm{ kv = KV } = State)->
    Found = dict:find({cor_id,CorrelationId}, KV),
    case Found of
        {ok,[{Type,From}|_Rest]}->
            case Type of
                ?EKAF_PACKET_DECODE_PRODUCE ->
                    Reply = {{sent, State#ekaf_fsm.partition, self()},
                             ekaf_protocol:decode_produce_response(Packet)},
                    gen_fsm:reply(From, Reply);
                ?EKAF_PACKET_DECODE_PRODUCE_ASYNC_BATCH ->
                    ekaf_lib:flushed_messages_replied_callback(State, Packet),
                    ok;
                _TE ->
                    ok
            end,
            State#ekaf_fsm{ kv = dict:erase({cor_id,CorrelationId}, KV) };
        _E->
            State
    end.

handle_info({tcp, _Port, Packet}, ready, State) ->
    Next = handle_incoming(Packet, State),
    fsm_next_state(ready, Next);
handle_info({tcp_closed,_Socket}, ready, State)->
    %% important to stop here
    {stop, normal, State#ekaf_fsm{ time = os:timestamp() }};

handle_info({info,From}, StateName, State)->
    From ! {StateName, State},
    fsm_next_state(StateName, State);
handle_info(Info, StateName, State) ->
    ?INFO_MSG("handle_info cant handle ~p during ~p",[Info, StateName]),
    fsm_next_state(StateName, State).

%%--------------------------------------------------------------------
%% Func: terminate/3
%% Purpose: Shutdown the fsm
%% Returns: any
%%--------------------------------------------------------------------
terminate(Reason, StateName,  #ekaf_fsm{ id = WorkerId, socket = Socket, topic = Topic, buffer = Buffer } = State)->
    ekaf_socket:close(Socket),
    PrefixedTopic = ?PREFIX_EKAF(Topic),
    (catch gproc:send({n,l,PrefixedTopic}, {worker, down, self(), WorkerId, StateName, State, Reason})),

    case Buffer of
        [] ->
            ok;
        _ ->
            ?INFO_MSG("stopping since ~p when buffer had ~p items",[Reason, length(Buffer)]),
            gproc:send({n,l,PrefixedTopic}, {add, queue, Buffer})
    end,
    pg2:leave(PrefixedTopic,self()),
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

reconnection_attempt()->
    gen_fsm:send_event(self(), connect).

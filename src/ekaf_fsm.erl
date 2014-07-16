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
-export([connected/2, connected/3,       %% Connection with the broker established
         bootstrapping/2, bootstrapping/3,  %% Asked for metadata and waiting
         ready/2, ready/3,    %% Got metadata and ready to send
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
    ?DEBUG_MSG("~n open socket to ~p",[Broker]),
    case ekaf_lib:open_socket(Broker) of
        {ok,Socket} ->
            State = #ekaf_fsm{
              topic = Topic,
              broker = Broker,
              socket = Socket,
              reply_to = ReplyTo
             },
            gen_fsm:send_event(self(), {metadata, State#ekaf_fsm.topic}),
            {ok, connected, State};
        {error, Reason} ->
            ?ERROR_MSG("~p",[Reason]),
            {stop, Reason}
    end;

init([PoolName, Broker, Topic, Leader, Partition]=_Args) ->
    ?DEBUG_MSG("~n open ~p with Broker ~p for leader ~p partition ~p",[Topic,Broker,Leader,Partition]),
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
              flush_callback = ekaf_lib:get_callbacks(flush)
             },

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
connected({metadata,_Topic}=Event, State) ->
    ekaf_lib:handle_connected(Event, State);

connected(_Event, State)->
    ekaf_lib:handle_continue_when_not_ready(connected,_Event,State).

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
    ekaf_lib:handle_async_as_batch(false, Async, PrevState);
ready({produce_async_batched, _Messages}= Async, PrevState)->
    ekaf_lib:handle_async_as_batch(true, Async, PrevState);
ready(ping, #ekaf_fsm{ topic = Topic } = State)->
    pg2:join(Topic,self()),
    %gen_server:cast(erlang:whereis(ekaf_lib:get_topic_as_atom(Topic)), {set,worker,self()}),
    gproc:send({n,l,Topic}, {set, worker, self()}),
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
%%--------------------------------------------------------------------
%% Func: StateName/3
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
%%--------------------------------------------------------------------
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

connected(Event, From, State)->
     kaf_lib:handle_reply_when_not_ready(connected, Event, From, State).
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
handle_info({tcp, _Port, <<CorrelationId:32,_/binary>> = Packet}, ready, #ekaf_fsm{ kv = KV } = State) ->
    Found = dict:find({cor_id,CorrelationId}, KV),
    Next = case Found of
               {ok,[{Type,From}|_Rest]}->
                   case Type of
                       ?EKAF_PACKET_DECODE_PRODUCE ->
                           Reply = {{sent, State#ekaf_fsm.partition, self()},
                                    ekaf_protocol:decode_produce_response(Packet)},
                           gen_fsm:reply(From,Reply);
                       _TE ->
                           ?INFO_MSG("~n EKAF got ~p so ignore",[_TE]),
                           ok
                   end,
                   State#ekaf_fsm{ kv = dict:erase({cor_id,CorrelationId}, KV) };
               _E->
                   ?INFO_MSG("~n found ~p so ignore",[_E]),
                   State
           end,
    fsm_next_state(ready, Next);
handle_info({tcp_closed,Socket}, ready, State)->
    ekaf_lib:close_socket(Socket),
    fsm_next_state(ready, State#ekaf_fsm{ socket = undefined });
handle_info(Info, StateName, State) ->
    ?INFO_MSG("~n got info at ~p ~p state:~p",[os:timestamp(), Info, State]),
    fsm_next_state(StateName, State).

%%--------------------------------------------------------------------
%% Func: terminate/3
%% Purpose: Shutdown the fsm
%% Returns: any
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, State) ->
    spawn(fun()->
                  ekaf_lib:close_socket(State#ekaf_fsm.socket)
          end),
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

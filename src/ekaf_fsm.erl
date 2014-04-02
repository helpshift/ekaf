-module(ekaf_fsm).

-behaviour(gen_fsm).
%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-include("ekaf_definitions.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

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
-export([
         connected/2,
         bootstrapping/2,
         ready/2,ready/3
         ]).

%% API
-export([pool_name/1]).

-record(st, { topic::binary(), broker:: tuple(), partition::integer(), replica::integer(), leader::integer(), socket :: port(), pool::atom(), metadata, cor_id = 0 :: integer(), client_id = "ekaf", reply_to  }).

%%====================================================================
%% External functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link/0
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Args)->
    gen_fsm:start_link(?MODULE,Args, []). %%{debug, [trace,statistics]}]).

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
    case open_socket(Broker) of
        {ok,Socket} ->
            State = #st{
              topic = Topic,
              broker = Broker,
              socket = Socket,
              reply_to = ReplyTo
             },
            gen_fsm:send_event(self(), {metadata, State#st.topic}),
            {ok, connected, State};
        {error, Reason} ->
            ?ERROR_MSG("~p",[Reason]),
            {stop, Reason}
    end;

init([PoolName, Broker, Topic, Leader, Partition]=Args) ->
    case open_socket(Broker) of
        {ok,Socket} ->
            State = #st{
              pool = PoolName,
              topic = Topic,
              broker = Broker,
              leader = Leader,
              partition = Partition,
              socket = Socket
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
connected({metadata,Topic}=Event, State) ->
    CorrelationId = State#st.cor_id,
    Request = ekaf_protocol:encode_metadata_request(CorrelationId, "", [Topic]),
    case gen_tcp:send(State#st.socket, Request) of
        ok ->
            Metadata =
                receive
                    {tcp, _Port, <<_CorrelationId:32, _/binary>> = Packet} ->
                        ekaf_protocol:decode_metadata_response(Packet);
                    _E ->
                        {error,_E}
                end,
            NewState = State#st{cor_id = CorrelationId + 1},
            gen_fsm:send_event(self(), {metadata, Metadata}),
            fsm_next_state(bootstrapping, NewState);
        {error, Reason} ->
            {stop, Reason, State}
    end;

connected(Event, State)->
    fsm_next_state(connected,State).

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
    BrokersDict = lists:foldl(fun(Broker,Dict)->
                                      dict:append(Broker#broker.node_id,
                                                  Broker,
                                                  Dict)
                              end, dict:new(), Metadata#metadata_response.brokers),

    Started = lists:map(
                fun(Topic)->
                        TopicName = Topic#topic.name,
                        case State#st.topic of
                            TopicName ->
                                [ begin
                                      Leader = Partition#partition.leader,
                                      PartitionId = Partition#partition.id,
                                      {ok,[Broker]} = dict:find(Leader, BrokersDict),
                                      {ok,PoolPid} = start_child(Broker, Topic, Leader, PartitionId ),
                                      {pg2:join(TopicName,PoolPid),
                                      PoolPid}
                                  end
                                  || Partition <- Topic#topic.partitions
                                        ];
                            _ ->
                                ok
                        end
                end, Metadata#metadata_response.topics),
    State#st.reply_to ! {ready,Started},
    {stop, normal, State#st{metadata = Metadata}};
bootstrapping(Event, State)->
    %io:format("~n got: ~p~n in bootstrapping state: ~p",[Event, State]),
    fsm_next_state(bootstrapping,State).

ready({produce_async, Messages}, State)->
    CorrelationId = State#st.cor_id+1,
    Topic = State#st.topic, Partition=State#st.partition, Leader = State#st.leader, Socket=State#st.socket, ClientId = State#st.client_id,
    MessageSets = ekaf_lib:data_to_message_sets(Messages),
    TopicPacket = #topic{
      name = Topic,
      partitions =
      [#partition{id = Partition, leader = Leader,
                  %% each messge goes in a different messageset, even for batching
                  message_sets_size = length(MessageSets), message_sets = MessageSets}]},
    ProducePacket = #produce_request{
      timeout=100, topics= [TopicPacket]
     },
    Request = ekaf_protocol:encode_async(CorrelationId,ClientId, ProducePacket),
    case gen_tcp:send(Socket, Request) of
        ok ->
            NewState = State#st{cor_id = CorrelationId + 1},
            fsm_next_state(ready, NewState);
        {error, Reason} ->
            ?ERROR_MSG("~p",[Reason]),
            {stop, Reason, State}
    end;
ready(ping, State)->
    PoolName = State#st.pool,
    fsm_next_state(ready,State);
ready(timeout, State)->
    fsm_next_state(ready,State);
ready(Event, State)->
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
    CorrelationId = State#st.cor_id+1,
    ClientId = State#st.client_id,
    Request = ekaf_protocol:encode_metadata_request(CorrelationId, "", [Topic]),
    case gen_tcp:send(State#st.socket, Request) of
        ok ->
            Response =
                receive
                    {tcp, _Port, <<CorrelationId:32, _/binary>> = Packet} ->
                        ekaf_protocol:decode_produce_response(Packet);
                    Packet ->
                        {error,Packet}
                end,
            NewState = State#st{cor_id = CorrelationId + 1},
            {reply, Response, ready, NewState};
        {error, Reason} ->
            ?ERROR_MSG("~p",[Reason]),
            {stop, Reason, State}
    end;

ready({produce_sync, Messages}, From, State)->
    CorrelationId = State#st.cor_id+1,
    Topic = State#st.topic, Partition=State#st.partition, Leader = State#st.leader, Socket=State#st.socket, ClientId = State#st.client_id,
    MessageSets = ekaf_lib:data_to_message_sets(Messages),
    TopicPacket = #topic{
      name = Topic,
      partitions =
      [#partition{id = Partition,
                  leader = Leader,
                  %% each messge goes in a different messageset, even for batching
                  message_sets_size = length(MessageSets),
                  message_sets = MessageSets}]},
    ProducePacket = #produce_request{
      required_acks=1,
      timeout=100,
      topics= [TopicPacket]
     },
    Request = ekaf_protocol:encode_sync(CorrelationId,ClientId, ProducePacket),
    case gen_tcp:send(Socket, Request) of
        ok ->
            Response =
                receive
                    {tcp, _Port, <<CorrelationId:32, _/binary>> = Packet} ->
                        %?INFO_MSG("got reply ~p [~p]",[Packet,ekaf_protocol:decode_produce_response(Packet)]),
                        ekaf_protocol:decode_produce_response(Packet);
                    Packet ->
                        {error,Packet}
                end,
            NewState = State#st{cor_id = CorrelationId + 1},
            {reply, Response, ready, NewState};
        {error, Reason} ->
            ?ERROR_MSG("~p",[Reason]),
            {stop, Reason, State}
    end;
ready(pool_name, From, State) ->
    Reply = pool_name(State),
    {reply, Reply, ready, State}.

%%--------------------------------------------------------------------
%% Func: handle_event/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%%--------------------------------------------------------------------
handle_event(Event, StateName, StateData) ->
    {next_state, StateName, StateData}.

%%--------------------------------------------------------------------
%% Func: handle_sync_event/4
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
%%--------------------------------------------------------------------
handle_sync_event(Event, From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% Func: handle_info/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%%--------------------------------------------------------------------
handle_info(Info, StateName, State) ->
    Topic = State#st.topic,
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% Func: terminate/3
%% Purpose: Shutdown the fsm
%% Returns: any
%%--------------------------------------------------------------------
terminate(Reason, StateName, State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change/4
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState, NewStateData}
%%--------------------------------------------------------------------
code_change(OldVsn, StateName, StateData, Extra) ->
    {ok, StateName, StateData}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
fsm_next_state(StateName, StateData) ->
    {next_state, StateName, StateData, ?KEEPALIVE_INTERVAL}.

open_socket({Host,Port}) when is_binary(Host)->
    open_socket({ ekaf_utils:btoa(Host),Port});

open_socket({Host,Port})->
    gen_tcp:connect(Host, Port, [{packet, 4}, binary]).

pool_name(State) when is_record(State,st)->
    PoolName = State#st.pool, Topic = State#st.topic, Broker = State#st.broker, PartitionId = State#st.partition, Leader = State#st.leader,
    pool_name({PoolName, Topic, Broker, PartitionId, Leader });
pool_name({PoolName, Topic, Broker, PartitionId, Leader })->
    NextPoolName = {PoolName, Topic, Broker, PartitionId, Leader },
    ekaf_utils:btoatom(ekaf_utils:itob(erlang:phash2(NextPoolName)));
pool_name({Topic, Broker, PartitionId, Leader })->
    NextPoolName = {Topic, Broker, PartitionId, Leader },
    ekaf_utils:btoatom(ekaf_utils:itob(erlang:phash2(NextPoolName))).


start_child(Broker, Topic, Leader, PartitionId)->
    %PoolName =
    TopicName = Topic#topic.name,
    SizeArgs = [{size,5},{max_overflow,10}],
    NextPoolName = ?MODULE:pool_name({TopicName, Broker, PartitionId, Leader }),
    ChildPoolName = ?MODULE:pool_name({NextPoolName, TopicName, Broker, PartitionId, Leader }),

    PoolArgs = [{name, {local, ChildPoolName }},
                {worker_module, ekaf_fsm}] ++ SizeArgs,
    WorkerArgs = [NextPoolName, {Broker#broker.host,Broker#broker.port}, TopicName, Leader, PartitionId],
    %%io:format("~nAsk kafboy_sup to start ~p child with workerargs:~p =>~n ~p",[NextPoolName, WorkerArgs, poolboy:child_spec(NextPoolName, PoolArgs, WorkerArgs)]),

    ekaf_sup:start_child(ekaf_sup, poolboy:child_spec(NextPoolName, PoolArgs, WorkerArgs)).

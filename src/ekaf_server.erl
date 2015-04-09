-module(ekaf_server).

-behaviour(gen_fsm).
%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-include("ekaf_definitions.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% External exports
-export([start_link/0, start_link/1, start_link/2]).

%% gen_server callbacks
%% gen_fsm callbacks
-export([
         init/1,
         handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

%% states
-export([downtime/2, downtime/3,
         connected/2, connected/3,
         ready/2, ready/3]).

-define(SERVER, ?MODULE).

%%====================================================================
%% External functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link/0
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    start_link([]).
start_link(Args) ->
    gen_fsm:start_link(?MODULE, Args, []).
start_link(Name,Args) ->
    gen_fsm:start_link(Name, ?MODULE, Args,
                          %[]
                          [{debug, [trace,statistics]}]
                         ).


%%====================================================================
%% Server functions
%%====================================================================

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
%% every topic should have one of these ekaf_server running always
%% if connection to metadata is done, then create child workers
%%    and restart if all workers of a partition die
%% until then queue up tasks
init([Topic])->
    Self = self(),
    State = generic_init(Topic),
    gproc:reg({n,l,Topic},[]),
    pg2:create(Topic),
    ekaf_picker:join_group_if_not_present(Topic, self()),
    gen_fsm:send_event(self(), connect),
    {ok, downtime, State#ekaf_server{topic = Topic, worker = Self}};
init(_Args) ->
    State = generic_init(any),
    {ok, downtime, State}.

generic_init(Topic)->
    Strategy = ekaf_lib:get_default(Topic,ekaf_partition_strategy, ?EKAF_DEFAULT_PARTITION_STRATEGY),
    StickyPartitionBatchSize = ekaf_lib:get_default(Topic,ekaf_sticky_partition_buffer_size, 1000),
    BootstrapBroker = ekaf_lib:get_bootstrap_broker(),
    gen_fsm:start_timer(1000,<<"refresh">>),
    #ekaf_server{strategy = Strategy, kv = dict:new(), max_buffer_size = StickyPartitionBatchSize, broker = BootstrapBroker, time = os:timestamp() }.

%%--------------------------------------------------------------------
%% Func: StateName/2
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%%--------------------------------------------------------------------
downtime(connect, #ekaf_server{ broker = Broker, time = T1 } = State)->
    case ekaf_socket:open(Broker) of
        {ok, Socket} ->
            T2 = os:timestamp(),
            ekaf_callbacks:call(?EKAF_CALLBACK_TIME_DOWN, self(), downtime, State, {ok,timer:now_diff(T2, T1)}),
            %% connection good, ask for metadata
            gen_fsm:send_event(self(), {metadata, req, Socket}),
            fsm_next_state(downtime, State#ekaf_server{ socket = Socket });
        {error, Reason} ->
            ekaf_callbacks:call(?EKAF_CALLBACK_WORKER_DOWN, self(), downtime, State, Reason),
            ekaf_server_lib:reconnect_attempt(),
            fsm_next_state(downtime, State)
    end;
downtime({produce_async, Messages}, State)->
    ekaf_server_lib:save_messages(downtime, State, Messages);
downtime({produce_async_batched, Messages}, State)->
    ekaf_server_lib:save_messages(downtime, State, Messages);
downtime({produce_sync_batched, Messages}, State)->
    ekaf_server_lib:save_messages(downtime, State, Messages);
downtime({metadata, req, Socket}, State) ->
    ekaf_server_lib:handle_connected(State, Socket),
    fsm_next_state(connected, State);
downtime({timeout, Timer, <<"refresh">>}, State)->
    gen_fsm:cancel_timer(Timer),
    gen_fsm:start_timer(1000,<<"refresh">>),
    fsm_next_state(downtime, State);
downtime({timeout, Timer, <<"reconnect">>}, State)->
    gen_fsm:cancel_timer(Timer),
    gen_fsm:send_event(self(), connect),
    fsm_next_state(downtime, State);
downtime(_Msg, State) ->
    ?INFO_MSG("downtime/2 cant handle ~p",[_Msg]),
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
connected({metadata, resp, _} = Event, #ekaf_server{ worker = _Worker} = State)->
    Next = ekaf_server_lib:handle_metadata_during_bootstrapping(Event, State),
    RequeryMetadata = ekaf_lib:get_default(any,
                                           ?EKAF_CONSTANT_PULL_FOR_CHANGES_TIMEOUT,
                                           ?EKAF_DEFAULT_PULL_FOR_CHANGES_TIMEOUT),
    gen_fsm:start_timer(RequeryMetadata, <<"reconnect">>),
    fsm_next_state(ready, Next#ekaf_server{ ongoing_metadata = false });
connected({produce_async, Messages}, State)->
    ekaf_server_lib:save_messages(connected, State, Messages);
connected({produce_async_batched, Messages}, State)->
    ekaf_server_lib:save_messages(connected, State, Messages);
connected({produce_sync_batched, Messages}, State)->
    ekaf_server_lib:save_messages(connected, State, Messages);
connected({timeout, Timer, <<"reconnect">>}, State)->
    gen_fsm:cancel_timer(Timer),
    fsm_next_state(connected, State);
connected({tcp_closed,_}, State)->
    ?INFO_MSG("tcp_closed, reconnect",[]),
    gen_fsm:send_event(self(), connect),
    fsm_next_state(downtime, State);
connected(_Event, State)->
    ?INFO_MSG("connected/2 cant handle ~p",[_Event]),
    fsm_next_state(connected, State).

ready(disconnected, State)->
    ?INFO_MSG("ready moving to downtime",[]),
    fsm_next_state(downtime, State);
ready({produce_async, Messages}, State)->
    ekaf_server_lib:save_messages(ready, State, Messages);
ready({produce_async_batched, Messages}, State)->
    ekaf_server_lib:save_messages(ready, State, Messages);
ready({produce_sync_batched, Messages}, State)->
    ekaf_server_lib:save_messages(ready, State, Messages);
ready({set, worker, Worker}, State) ->
    {noreply, State#ekaf_server{ worker = Worker}};
ready(connect, #ekaf_server{ broker = Broker } = State)->
    case ekaf_socket:open(Broker) of
        {ok,Socket} ->
            %% connection good, ask for metadata
            gen_fsm:send_event(self(), {metadata, req, Socket});
        _ ->
            gen_fsm:start_timer(?EKAF_CONNECT_TIMEOUT, <<"reconnect">>)
    end,
    fsm_next_state(ready, State);
ready({metadata, req, Socket}, State) ->
    % asked to get metadata during ready
    ekaf_server_lib:handle_connected(State, Socket),
    fsm_next_state(ready, State);
ready({metadata, resp, _Metadata} = Event, State)->
    Next = ekaf_server_lib:handle_metadata_during_bootstrapping(Event, State),
    fsm_next_state(ready, Next#ekaf_server{ ongoing_metadata = false });
ready({timeout, Timer, <<"reconnect">> }, State)->
    gen_fsm:cancel_timer(Timer),
    gen_fsm:send_event(self(), connect),
    fsm_next_state(ready, State);
ready({timeout, Timer, <<"refresh">> = TimeoutKey}, #ekaf_server{
              strategy = Strategy,
              max_buffer_size = Max, ctr = Ctr, topic = Topic, workers = Workers} = State) ->
    gen_fsm:cancel_timer(Timer),
    gen_fsm:start_timer(1000, TimeoutKey),

    ToPick = case Strategy of
                 sticky_round_robin when Ctr > Max ->
                     true;
                 strict_round_robin ->
                     false;
                 _ ->
                     false
             end,
    Next = case ToPick of
               true ->
                   case ekaf_server_lib:handle_pick({pick, Topic, undefined}, self(), State) of
                       {error,_}->
                           State#ekaf_server{ ctr = 0 };
                       {NextWorker, NextState} when Strategy =:= strict_round_robin->
                           Members = pg2:get_members(Topic),
                           NextWorkers = case Workers of [] -> Members; _ -> case State#ekaf_server.workers -- Members of [] -> Workers; _ -> Members end end,
                           NextState#ekaf_server{ ctr = 0, worker = NextWorker, workers =  NextWorkers};
                       {NextWorker, NextState} ->
                           NextState#ekaf_server{ ctr = 0, worker = NextWorker}
                   end;
               _ ->
                   State
           end,
    fsm_next_state(ready, Next);

ready(Msg, State) ->
    ?INFO_MSG("ready/2 cant handle ~p",[Msg]),
    fsm_next_state(ready, State).

%%--------------------------------------------------------------------
%% Func: StateName/3
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
%%--------------------------------------------------------------------
ready(info, _From, State)->
    Reply = State,
    {reply, Reply, ready, State};
ready({produce_sync, Messages}, _From, State)->
    ekaf_server_lib:save_messages(ready, State, Messages);
ready(prepare, From, #ekaf_server{ worker = Worker } = State)->
	gen_fsm:reply(From, {ok, Worker}),
    fsm_next_state(ready, State);
ready(metadata, From, #ekaf_server{ kv = KV} = State)->
    % got metadata during ready
    % this means that regular workers arent ready yet,
    % lets get metadata, get a worker, then ask it
    Pid = spawn(fun()->
                        receive
                            {ok, Worker} ->
                                Reply = gen_fsm:sync_send_event(Worker, metadata),
                                gen_fsm:reply(From, Reply);
                            Reply ->
                                gen_fsm:reply(From, Reply)
                        end
                end),

    fsm_next_state(ready, State#ekaf_server{ kv = dict:append(prepare, Pid, KV)});
ready(Msg, _From, State)->
    ?INFO_MSG("ready/3 cant handle ~p",[Msg]),
    Reply = {error, Msg},
    {reply, Reply, ready, State}.

downtime(info, _From, State)->
    Reply = State,
    {reply, Reply, downtime, State};
downtime(prepare, From, State)->
	%% downtime since server down
	gen_fsm:reply(From, {ok,self()}),
	fsm_next_state(downtime, State);
downtime({produce_sync, Messages}, From, State)->
    gen_fsm:reply(From, {error, downtime}),
    ekaf_server_lib:save_messages(downtime, State, Messages);
downtime(_Request, _From, State) ->
    ?debugFmt("downtime/3 cant handle ~p from ~p",[_Request, _From]),
    Reply = {error, downtime},
    {reply, Reply, downtime, State}.

connected({produce_sync, Messages}, _From, State)->
    ekaf_server_lib:save_messages(connected, State, Messages);
connected(Info,_,State)->
    ?debugFmt("connected/3 cant handle ~p",[Info]),
    fsm_next_state(connected, State).
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
handle_info({pick, _Topic, Callback}, ready, #ekaf_server{ strategy = strict_round_robin, workers = [Worker|Workers] } = State) ->
    Callback ! {ok,Worker},
    fsm_next_state(ready, State#ekaf_server{ workers = lists:append(Workers,[Worker])} );
handle_info({pick, _Topic, Callback}, ready, #ekaf_server{ strategy = sticky_round_robin, worker = Worker, ctr = Ctr } = State) ->
    Callback ! {ok, Worker},
    fsm_next_state(ready, State#ekaf_server{ ctr = Ctr + 1});
handle_info({pick, _Topic, Callback}, ready, #ekaf_server{ worker = Worker} = State) ->
    Callback ! {ok, Worker},
    fsm_next_state(ready, State);
handle_info({pick, _, Callback}, StateName, State)->
    Callback ! {ok, self()},
    fsm_next_state(StateName, State);
handle_info({worker, down, WorkerDown, WorkerId, WorkerDownStateName, WorkerDownState, WorkerDownReason}, _StateName, #ekaf_server{ topic = Topic, worker = Worker, ongoing_metadata = RequestedMetadata, workers = Workers } =  State)->
    ekaf_lib:stop_child(WorkerId),
    case RequestedMetadata of
        true -> ok;
        _ -> ekaf_server_lib:reconnect_attempt()
    end,
    NextWorkers = case Workers of
                      List when is_list(List) ->
                          Workers -- [WorkerDown];
                      _ ->
                          []
                  end,
    %% special case where the worker that went down is the the current worker
    %% lets replace it immediately instead of during ekaf_fsm's <<"refresh">>
    NextWorker = case Worker of
                     WorkerDown ->
                         case Workers of
                             [AnotherWorker|_]->
                                 AnotherWorker;
                             _ ->
                                 case ekaf_server_lib:handle_pick({pick, Topic, undefined}, self(), State#ekaf_server{ workers = NextWorkers }) of
                                     {TempNextWorker, _} when is_pid(TempNextWorker) ->
                                         TempNextWorker;
                                     _NoWorker ->
                                         self()
                                 end
                         end;
                     _ ->
                         Worker
                 end,
    ekaf_callbacks:call(?EKAF_CALLBACK_WORKER_DOWN, WorkerDown, WorkerDownStateName, WorkerDownState, WorkerDownReason),
    case NextWorkers of
        [] ->
            ekaf_picker:join_group_if_not_present(Topic, self()),
            gen_fsm:send_event(self(), connect),
            fsm_next_state(downtime,State#ekaf_server { ongoing_metadata = true, workers = NextWorkers, worker = self(), time = os:timestamp() });
        _ ->
            fsm_next_state(ready, State#ekaf_server{ ongoing_metadata = true, workers = NextWorkers, worker = NextWorker, time = os:timestamp() } )
    end;
handle_info({worker, up, WorkerUp, WorkerUpStateName, WorkerUpState, _}, StateName, #ekaf_server { topic = Topic, messages = OfflineMessages } = State) ->
    pg2:leave(Topic, self()),
    case StateName of
        ready ->
            ekaf_server_lib:send_messages(StateName, State, lists:reverse(OfflineMessages));
        _ ->
            ok
    end,

    case ekaf_callbacks:find(?EKAF_CALLBACK_WORKER_UP) of
        {Mod,Func} ->
            Mod:Func(?EKAF_CALLBACK_WORKER_UP, WorkerUp, WorkerUpStateName, WorkerUpState, undefined);
        _ ->
            ok
    end,
    Next = ekaf_server_lib:reply_to_prepares(WorkerUp, State),
    fsm_next_state(StateName, Next#ekaf_server{ worker = WorkerUp, messages = [], workers = pg2:get_members(Topic)});
handle_info({set, strategy, Value}, ready, State)->
    Next = State#ekaf_server{ strategy = Value },
    fsm_next_state(ready, Next);
handle_info({set, max_buffer_size, Value}, ready,  State)->
    Next = State#ekaf_server{ max_buffer_size = Value },
    fsm_next_state(ready, Next);
handle_info({add, queue, Messages}, ready, #ekaf_server{ messages = OfflineMessages, worker = Worker} = State)->
    case self() of
        Worker ->
            ekaf_server_lib:save_messages(ready, State, Messages);
        _ ->
            ekaf_server_lib:send_messages(ready, State, Messages),
            ekaf_server_lib:send_messages(ready, State, OfflineMessages)
    end,
    fsm_next_state(ready,  State);
handle_info({add, queue, Messages}, StateName, #ekaf_server{ messages = OfflineMessages } = State)->
    %% when a partition worker dies, its queue gets added to the downtime queue
    %% the downtime queue is flushed when it the connection is good to go again
    fsm_next_state(StateName, State#ekaf_server{ messages = lists:append( OfflineMessages, Messages) });

handle_info(purge_messages, StateName, #ekaf_server{ messages = OfflineMessages } = State)->
	?INFO_MSG("Purge ~p messages for topic in process ~p~n",
			 [length(OfflineMessages), self()]),
    fsm_next_state(StateName, State#ekaf_server{ messages = [] });

handle_info(_Info, StateName, State) ->
    ?INFO_MSG("dont know how to handle ~p during ~p",[_Info, StateName]),
    fsm_next_state(StateName, State).

%%--------------------------------------------------------------------
%% Func: terminate/3
%% Purpose: Shutdown the fsm
%% Returns: any
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, #ekaf_server{ topic = Topic }) ->
    pg2:delete(Topic),
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

-module(ekaf_server).

-behaviour(gen_server).
%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-include("ekaf_definitions.hrl").

%%--------------------------------------------------------------------
%% External exports
-export([start_link/0, start_link/1, start_link/2]).

%% gen_server callbacks
-export([init/1, kickoff/0,
         handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {kv, strategy, buffer_size, ctr, worker, topic}).
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
    gen_server:start_link(?MODULE, Args, []).
start_link(Name,Args) ->
    gen_server:start_link(Name, ?MODULE, Args,
                          []
                          %[{debug, [trace,statistics]}]
                         ).

%%====================================================================
%% Server functions
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%%--------------------------------------------------------------------
init([Topic])->
    State = generic_init(),
    erlang:send_after(1000, self(), <<"refresh_every_second">>),
    {ok, State#state{topic = Topic}};
init(_Args) ->
    State = generic_init(),
    {ok, State}.

generic_init()->
    kickoff(),
    Strategy = ekaf_lib:get_default(any,ekaf_partition_strategy, ?EKAF_DEFAULT_PARTITION_STRATEGY),
    StickyPartitionBatchSize = ekaf_lib:get_default(any,ekaf_sticky_partition_buffer_size, 1000),
    #state{strategy = Strategy, ctr = 0, kv = dict:new(), buffer_size = StickyPartitionBatchSize}.

kickoff()->
    case ekaf_lib:get_bootstrap_topics() of
        {ok, List} when is_list(List)->
            [ begin
                  ekaf:prepare(Topic)
              end || Topic <- List];
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling all synchronous call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_call({pick, Topic, Callback},From, State)->
    {Reply, Next} = handle_pick({pick,Topic, Callback}, From, State),
    {reply, Reply, Next};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling all asynchronous cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_cast({pick, _Topic, Callback}, #state{ strategy = ordered_round_robin, worker = Worker, ctr = Ctr } = State) ->
    Callback(Worker),
    {noreply, State#state{ ctr = Ctr + 1}};

%% Random strategy. Faster, but kafka gets messages in different order than that produced
handle_cast({pick, _Topic, Callback}, #state{ worker = Worker} = State) ->
    Callback(Worker),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------

handle_info(<<"refresh_every_second">>=TimeoutKey,
            #state{
              strategy = Strategy,
              buffer_size = Max, worker = OldWorker, ctr = Ctr, topic = Topic} = State) ->
    erlang:send_after(1000, self(), TimeoutKey),
    ToPick = case Strategy of
                 random ->
                     true;
                 ordered_round_robin when Ctr > Max ->
                     true;
                 _ ->
                     false
             end,
    Next = case ToPick of
               true ->
                   case handle_pick({pick, Topic, undefined}, self(), State) of
                       {error,_}->
                           State#state{ ctr = 0 };
                       {NextWorker, NextState} ->
                           NextState#state{ ctr = 0, worker = NextWorker }
                   end;
               _ ->
                   State
           end,
    {noreply, Next};
handle_info({set, strategy, Value}, State)->
    Next = State#state{ strategy = Value },
    {noreply, Next};
handle_info({set, buffer_size, Value}, State)->
    Next = State#state{ buffer_size = Value },
    {noreply, Next};
handle_info({from, From, {pick, Topic, Callback}}, State)->
    {Reply, Next} = handle_pick({pick, Topic, Callback}, From, State),
    From ! Reply,
    {noreply, Next};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%%--------------------------------------------------------------------
terminate(Reason, State) ->
    io:format("~n ~p terminating since ~p with state ~p",[?MODULE,Reason,State]),
    ok.

%%--------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
handle_pick({pick, Topic, _Callback}, _From, #state{ kv = PrevKV } = State)->
    case pg2:get_closest_pid(Topic) of
%ekaf_picker:pick(Topic,undefined,sync, State#state.strategy) of
        {error, {no_such_group,_}} ->
            Added = State#state{ kv = dict:append(Topic, 1, PrevKV) },
            ekaf:prepare(Topic),
            { {error, picking},
              Added};
        Pid when is_pid(Pid)->
            % NextInt = case dict:find(Topic, PrevKV) of
            %               {ok,[Int]} ->
            %                   Int+1;
            %               _ ->
            %                   1
            %           end,
            % Next = State#state{ kv = dict:append(Topic, NextInt, PrevKV) },
            %{Pid,Next};
            {Pid,State};
        _ ->
            {error, bootstrapping}
    end;
handle_pick(Pick, _From, State) ->
    Error = {error, {handle_pick_error,Pick}},
    {Error, State}.

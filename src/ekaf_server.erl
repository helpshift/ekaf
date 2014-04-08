-module(ekaf_server).

-behaviour(gen_server).
%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-include("ekaf_definitions.hrl").

%%--------------------------------------------------------------------
%% External exports
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, kickoff/0,
         handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {kv, strategy, buffer_size, ctr, worker }).
-define(SERVER, ?MODULE).

%%====================================================================
%% External functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link/0
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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
init(_Args) ->
    kickoff(),
    Strategy = ekaf_lib:get_default(any,ekaf_partition_strategy, ordered_round_robin),
    StickyPartitionBatchSize = ekaf_lib:get_default(any,ekaf_sticky_partition_buffer_size, 1000),
    {ok, #state{strategy = Strategy, ctr = 0, kv = dict:new(), buffer_size = StickyPartitionBatchSize }}.

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
%% Description: Handling call messages
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
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_cast({pick, Topic, Callback}, #state{ strategy = ordered_round_robin, ctr = Ctr, buffer_size = Max, worker = OldWorker } = State) ->
    Rem = Ctr rem Max,
    {Worker,Next} = case Rem of
                 0 ->
                     case handle_pick({pick, Topic, Callback}, self(), State) of
                         {error,_}->
                             {OldWorker, State};
                         {NextWorker, NextState} ->
                             {NextWorker, NextState}
                     end;
                 _ ->
                     {OldWorker, State}
             end,
    case Worker of
        Pid when is_pid(Pid) ->
            Callback(Worker),
            {noreply, Next#state{ worker = Worker, ctr = Ctr + 1}};
        _ ->
            {noreply, Next#state{ ctr = Ctr + 1}}
    end;

%% Random strategy. Faster, but kafka gets messages in different order than that produced
handle_cast({pick, Topic, Callback}, State) ->
    Worker = pg2:get_closest_pid(Topic),
    Callback(Worker),
    {noreply, State#state{ worker = Worker}}.

%%--------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
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
handle_pick({pick, Topic, _Callback}, _From, State)->
    case pg2:get_closest_pid(Topic) of
        {error, {no_such_group,_}} ->
            pg2:create(Topic),
            Added = State#state{ kv = dict:append(Topic, os:timestamp(), State#state.kv) },
            ekaf_fsm:start_link([
                                 self(),
                                 ekaf_lib:get_bootstrap_broker(),Topic]),
            { {error, picking},
              Added};
        Pid when is_pid(Pid)->
            {Pid,State};
        _ ->
            {error, bootstrapping}
    end;
handle_pick(Pick, _From, State) ->
    Error = {error, {handle_pick_error,Pick}},
    {Error, State}.

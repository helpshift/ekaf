-module(ekaf).

-behaviour(application).

%% includes
-include("ekaf_definitions.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").
-endif.

-export([start/0, start/2]).
-export([stop/0, stop/1]).

-export([metadata/1,
         publish/2, produce_sync/2, produce_async/2,
         default_broker/0, pick/1]).

start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

start(_Type, _Args) ->
    ekaf_sup:start_link([]).

stop(_State) ->
    ok.

%%--------------------------------------------------------------------
%%% API
%%--------------------------------------------------------------------
metadata(Topic)->
    Worker = ?MODULE:pick(Topic),
    case Worker of
        {error,_}->
            ok;
        _ ->
           gen_fsm:sync_send_event(Worker, {metadata, Topic})
    end.

publish(Topic,Data)->
    ?MODULE:produce_async(Topic, Data).

produce_sync(Topic, Data)->
    Worker = ?MODULE:pick(Topic),
    case Worker of
        {error,_}=Error->
            Error;
        _ ->
            gen_fsm:sync_send_event(Worker, {produce_sync, Data})
    end.

produce_async(Topic, Data)->
    Worker = ?MODULE:pick(Topic),
    case Worker of
        {error,_}->
            ok;
        _ ->
            gen_fsm:send_event(Worker, {produce_async, Data})
    end.

pick(Topic)->
    case pg2:get_closest_pid(Topic) of
        {error,{no_such_group,_}}->
            pg2:create(Topic),
            From = self(),
            ekaf_fsm:start_link([From, ?MODULE:default_broker(),Topic]),
            receive
                {ready,Started} ->
                    ok
            end,
            {error,try_again};
        PoolPid ->
            Worker = poolboy:checkout(PoolPid),
            poolboy:checkin(PoolPid,Worker),
            Worker
    end.

default_broker()->
    case application:get_env(ekaf, ekaf_bootstrap_broker) of
        {Broker,Port}->
            {Broker,Port};
        _ ->
            {"localhost",9091}
    end.

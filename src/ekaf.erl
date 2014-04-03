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

-export([pick/1,
         publish/2, batch/2,
         produce_sync_batched/2, produce_async_batched/2,
         produce_sync/2, produce_async/2,
         metadata/1, info/1]).

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

batch(Topic,Data)->
    produce_sync_batched(Topic, Data).

publish(Topic,Data)->
    produce_async(Topic, Data).

produce_sync(Topic, Data)->
    common_sync(produce_sync, Topic, Data).

produce_async(Topic, Data)->
    common_async(produce_async, Topic, Data).

produce_sync_batched(Topic, Data)->
    common_sync(produce_sync_batched, Topic, Data).

produce_async_batched(Topic, Data)->
    common_async(produce_async_batched, Topic, Data).

metadata(Topic)->
    Worker = ?MODULE:pick(Topic),
    case Worker of
        {error,_}=E->
            E;
        _ ->
           gen_fsm:sync_send_event(Worker, {metadata, Topic})
    end.
info(Topic)->
    Worker = ekaf:pick(Topic),
    case Worker of
        {error,_}=E->
            E;
        _ ->
            gen_fsm:sync_send_event(Worker, info)
    end.

common_async(Event, Topic, Data)->
    Worker = ?MODULE:pick(Topic),
    case Worker of
        {error,_}->
            ok;
        _ ->
            gen_fsm:send_event(Worker, {Event, Data})
    end.

common_sync(Event, Topic, Data)->
    Worker = ?MODULE:pick(Topic),
    case Worker of
        {error,_}->
            ok;
        _ ->
            gen_fsm:sync_send_event(Worker, {Event, Data})
    end.

pick(Topic)->
    case pg2:get_closest_pid(Topic) of
        {error,{no_such_group,_}}->
            pg2:create(Topic),
            From = self(),
            ekaf_fsm:start_link([From, ekaf_lib:get_bootstrap_broker(),Topic]),
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

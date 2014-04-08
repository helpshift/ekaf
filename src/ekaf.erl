-module(ekaf).

-behaviour(application).

%% includes
-include("ekaf_definitions.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

-define(TIMEOUT, 1000).
-export([start/0, start/2]).
-export([stop/0, stop/1]).

-export([prepare/1, pick/1, pick/2, pick/3, pick_async/2,
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
    eflame:apply(ekaf, produce_sync_batched, [Topic, Data]).
    %produce_sync_batched(Topic, Data).

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
    ?MODULE:pick(Topic, fun(Worker)->
                                case Worker of
                                    {error,_}=E->
                                        E;
                                    _ ->
                                        gen_fsm:sync_send_event(Worker, {metadata, Topic}, ?TIMEOUT)
                                end
                        end).
info(Topic)->
    ?MODULE:pick(Topic, fun(Worker)->
                                case Worker of
                                    {error,_}=E->
                                        E;
                                    _ ->
                                        gen_fsm:sync_send_event(Worker, info)
                                end
                        end).

common_async(Event, Topic, Data)->
    ?MODULE:pick_async(Topic, fun(Worker)->
                                case Worker of
                                    {error,_}=E->
                                        E;
                                    _ ->
                                        gen_fsm:send_event(Worker, {Event, Data})
                                end
                        end).

common_sync(Event, Topic, Data)->
    ?MODULE:pick(Topic, fun(Worker)->
                                case Worker of
                                    {error,_} = E->
                                        E;
                                    _ ->
                                        gen_fsm:sync_send_event(Worker, {Event, Data}, ?TIMEOUT)
                                end
                        end).

pick(Topic)->
    pick(Topic,undefined).
pick(Topic, Callback)->
    pick(Topic, sync, Callback).
pick_async(Topic, Callback)->
    pick(Topic, async, Callback).
pick(Topic, Mode, Callback) ->
    case Mode of
        sync ->
            ekaf_lib:pick(Topic,Callback);
        _ ->
            %% very fast
            gen_server:cast(ekaf_server, {pick, Topic, Callback})
    end.

prepare(Topic)->
    pg2:create(Topic),
    ekaf_fsm:start_link([
                         self(),
                         ekaf_lib:get_bootstrap_broker(),Topic]).

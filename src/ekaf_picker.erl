-module(ekaf_picker).
-export([pick/1, pick/2, pick/3, pick/4]).
-include("ekaf_definitions.hrl").
-export([pick_sync/3, pick_sync/4,
        pick_async/2]).

%% Each topic gets its own gen_server to
%%  pick a partition worker
%% This step is blocking so that it can optimize
%%  picking the same worker until it crosses batch_size, etc

%% pick_async is what is directly called for sending events
%% pick_sync is what is called when the topics ekaf_server
%%  needs to pick the next worker ( usually checked once a second )

pick(Topic)->
    Callback = undefined,
    pick(Topic,Callback).
% occurance of a null callback, indicates sync when Mode is not specified
pick(Topic, undefined) ->
    pick(Topic, undefined, sync);
% occurance of a callback that is not null, indicates async when Mode is not specified
pick(Topic, Callback) ->
    pick(Topic, Callback, async).
pick(Topic, Callback, Mode) ->
    pick(Topic, Callback, Mode, ?EKAF_DEFAULT_PARTITION_STRATEGY).
pick(Topic, Callback, Mode, Strategy) ->
    case Mode of
        %_ when Strategy =:= sticky_round_robin ->
        %    pick_sync(Topic, Callback, Strategy);
        sync ->
            pick_sync(Topic,Callback, Strategy);
        _ when Callback =:= undefined->
            pick_sync(Topic,Callback, Strategy);
        _ ->
            %% non-blocking
            %% and the picked worker is sent to the Callback
            pick_async(Topic, Callback)
    end.

pick_async(Topic,Callback)->
    %RegName = ekaf_lib:get_topic_as_atom(Topic),
    RegName = gproc:where({n,l,Topic}),
    case RegName of
        undefined ->
            ekaf:prepare(Topic);
        _ ->
            gproc:send({n,l,Topic},
            %gen_server:cast(
            % RegName,
              {pick, Topic, Callback})
    end.

pick_sync(Topic, Callback, Strategy)->
    pick_sync(Topic, Callback, Strategy, 0).
%% if strategy is ketama
pick_sync(_Topic, _Callback, ketama, _Attempt)->
    %TODO
    error;
%% if strategy is sticky_round_robin or strict_round_robin or random
pick_sync(Topic, Callback, _Strategy, Attempt)->
    R = case pg2:get_closest_pid(Topic) of
            PoolPid when is_pid(PoolPid) ->
                PoolPid;
            {error, {no_process,_}}->
                {error,bootstrapping};
            {error,{no_such_group,_}}->
                ekaf:prepare(Topic),
                receive
                    X ->
                        X,
                        {error, {retry, Attempt+1}}
                after 5000 ->
                        io:format("~n into timeout",[]),
                        {error,timeout}
                end;
            _E ->
                io:format("~p pick_sync RROR: ~p",[?MODULE,_E]),
                _E
        end,
    case Callback of
        undefined -> R;
        _ -> spawn(fun()-> Callback(R) end)
    end.

-module(ekaf_picker).
-export([pick/1, pick/2, pick/3, pick/4]).
-include("ekaf_definitions.hrl").
-export([pick_sync/3, pick_sync/4,
        pick_async/2]).

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
    %Worker = pg2:get_closest_pid(Topic),Callback(Worker)
    gen_server:cast(ekaf_server, {pick, Topic, Callback}).

pick_sync(Topic, Callback, Strategy)->
    pick_sync(Topic, Callback, Strategy, 0).
%% if strategy is ketama
pick_sync(_Topic, _Callback, ketama, _Attempt)->
    %TODO
    error;
%% if strategy is ordered_round_robin or random
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
        % not using poolboy for now
        % when uncommenting this, also uncomment the start_child poolboy part
        % PoolPid when is_pid(PoolPid), Strategy =:= poolboy ->
        %     case erlang:is_process_alive(PoolPid) of
        %         true ->
        %             case Callback of
        %                 Fun when is_function(Fun) ->
        %                     case catch poolboy:transaction(PoolPid, Fun, 1234) of
        %                         X ->
        %                             X
        %                     end;
        %                 _ ->
        %                     io:format("~n someone called pool/1",[]),
        %                     Worker = poolboy:checkout(PoolPid, true, 400),
        %                     poolboy:checkin(PoolPid,Worker),
        %                     Worker
        %             end;
        %         _ ->
        %             io:format("~n ~p leaving group since dead",[PoolPid]),
        %             pg2:leave(Topic, PoolPid),
        %             {error,dead_pool}
        %     end;
            _E ->
                io:format("~p pick_sync RROR: ~p",[?MODULE,_E]),
                _E
        end,
    case Callback of
        undefined -> R;
        _ -> spawn(fun()-> Callback(R) end)
    end.

-module(ekaf_picker).
-export([pick/1, pick/2, pick/3]).
-export([pick_sync/2, pick_sync/3,
        pick_async/2]).

pick(Topic)->
    Callback = undefined,
    pick(Topic,Callback).
pick(Topic, undefined) ->
    pick(Topic, undefined, sync);
pick(Topic, Callback) ->
    pick(Topic, Callback, async).
pick(Topic, Callback, Mode) ->
    case Mode of
        sync ->
            pick_sync(Topic,Callback);
        _ when Callback =:= undefined->
            pick_sync(Topic,Callback);
        _ ->
            %% non-blocking
            %% and the picked worker is sent to the Callback
            pick_async(Topic, Callback)
    end.

pick_async(Topic,Callback)->
    gen_server:cast(ekaf_server, {pick, Topic, Callback}).

pick_sync(Topic, Callback)->
    pick_sync(Topic, Callback, pg2).
pick_sync(Topic, Callback, Strategy)->
    pick_sync(Topic, Callback, Strategy, 0).
pick_sync(_Topic, _Callback, ketama, _Attempt)->
    %TODO
    error;
pick_sync(Topic, Callback, Strategy, Attempt)->
    R = case pg2:get_closest_pid(Topic) of
            PoolPid when is_pid(PoolPid), Strategy =:= pg2->
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
        _ -> Callback(R)
    end.

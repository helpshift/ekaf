%%%-------------------------------------------------------------------
-module(ekaf_callbacks).

%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-include("ekaf_definitions.hrl").
%%--------------------------------------------------------------------
%% External exports
%%--------------------------------------------------------------------
-export([
         find/1,
         call/5,
         encode_messages_as_one_large_message/5,
         encode_messages_as_one_large_json/5
        ]).

%% See ekaf/include/ekaf_definitions.hrl for the callbacks
find(CallbackName)->
    case application:get_env(ekaf, CallbackName) of
        {ok,{M,F}}-> {M,F};
        _ -> undefined
    end.

call(CallbackName, Worker, StateName, State, Reason)->
    case find(CallbackName) of
        {Mod,Func} ->
            Mod:Func(CallbackName, Worker, StateName, State, Reason);
        _ ->
            ok
    end.

encode_messages_as_one_large_message(_CallbackName, _Worker, _StateName, _State, Messages)->
    ekaf_lib:data_to_message_sets(term_to_binary(Messages)).

encode_messages_as_one_large_json(_CallbackName, _Worker, _StateName, _State, Messages)->
    ekaf_lib:data_to_message_sets(jsx:encode(Messages)).


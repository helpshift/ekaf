-module(ekaf_lib).

-include("ekaf_definitions.hrl").

-export([data_to_message_sets/1, data_to_message_set/1]).

data_to_message_sets(Data)->
    data_to_message_sets(Data,[]).
data_to_message_sets([],Messages)->
    Messages;
data_to_message_sets([Message|Rest], Messages) when is_record(Message,message)->
    data_to_message_sets(Rest, [Message|Messages]);
data_to_message_sets([{Key,Value}|Rest], Messages ) ->
    data_to_message_sets(Rest, [#message_set{ size=1, messages=[#message{key = Key, value = Value }]}|Messages]);
data_to_message_sets([Value|Rest], Messages) ->
    data_to_message_sets(Rest, [#message_set{ size = 1, messages = [#message{ value = Value }]}|Messages]);
data_to_message_sets(Value, Messages) ->
    [#message_set{ size = 1, messages = [#message{value = Value}]}| Messages].

%%
data_to_message_set(Data)->
    data_to_message_set(Data,#message_set{size = 0, messages= []}).
data_to_message_set([],#message_set{ messages = Messages} )->
    #message_set{ size = length(Messages), messages = Messages };
data_to_message_set([Message|Rest], #message_set{ messages = Messages }) when is_record(Message,message)->
    data_to_message_set(Rest, #message_set{ messages = [Message|Messages]});
data_to_message_set([{Key,Value}|Rest], #message_set{ messages = Messages }) ->
    data_to_message_set(Rest, #message_set{ messages = [#message{ key = Key, value = Value }|Messages]});
data_to_message_set([Value|Rest], #message_set{ messages = Messages }) ->
    data_to_message_set(Rest, #message_set{ messages = [#message{ value = Value }|Messages]});
data_to_message_set(Value, #message_set{ size = Size, messages = Messages }) ->
    #message_set{ size = Size + 1, messages = [#message{value = Value}| Messages] }.

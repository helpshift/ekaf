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
         call/6,
         encode_messages_as_one_large_message/5,
         encode_messages_as_one_large_json/5,
         default_custom_partition_picker/3,
         default_custom_partition_blocker/3, default_custom_partition_undecided/3
        ]).

%% See ekaf/include/ekaf_definitions.hrl for the callbacks
find(CallbackName)->
    case application:get_env(ekaf, CallbackName) of
        {ok,{M,F}}-> {M,F};
        _ -> undefined
    end.

call(CallbackAtom, CallbackName, Worker, StateName, State, Reason)->
    case find(CallbackAtom) of
        {Mod,Func} ->
            Mod:Func(CallbackName, Worker, StateName, State, Reason);
        _ ->
            ok
    end.

encode_messages_as_one_large_message(_CallbackName, _Worker, _StateName, _State, Messages)->
    ekaf_lib:data_to_message_sets(term_to_binary(Messages)).

encode_messages_as_one_large_json(_CallbackName, _Worker, _StateName, _State, Messages)->
    ekaf_lib:data_to_message_sets(jsx:encode(Messages)).

%% Used by default when the data you produce is of a tuple/list of tuples form
%% and you want to produce data of the same key to the same partition
%% NOTE: it may go to a different worker of the same partition
%% if you have set more than 1 worker per partition
default_custom_partition_picker(Topic, {Key,_}, State)->
    default_custom_partition_picker(Topic, Key, State);
default_custom_partition_picker(_Topic, Data, State)->
    Partitions = ekaf_lib:partitions(State),

    %% simple (hash of incoming key) modulo (no# of current partitions)
    ChosenPartition = erlang:phash2(Data) rem length(Partitions),
    %% within this partition, ekaf round robins, so return
    %% {partition, <partition>::int}

    %% to over-ride this with a specific worker, return
    %% {worker, <pid>::pid, State#ekaf_server.workers::list}

    {partition, ChosenPartition}.

default_custom_partition_blocker(_Topic, _Key, _State)->
    %% use only if you want the partition picker to stop a msg from getting produced
    {error, <<"blocked">>}.

default_custom_partition_undecided(_Topic, _Key, _State)->
    %% use only if you want the partition picker to give up, and let ekaf decide
    undefined.

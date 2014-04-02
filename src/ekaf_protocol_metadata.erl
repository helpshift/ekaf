-module(ekaf_protocol_metadata).

-include("ekaf_definitions.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").
-endif.

-export([decode/1]).

decode(Packet)->
    case Packet of
        <<CorrelationId:32, Rest/binary>> ->
            {Brokers, Topics, _ } = decode_to_brokers_and_topics(Rest),
            #metadata_response{ cor_id = CorrelationId, brokers = Brokers, topics = Topics};
        _ ->
            #metadata_response{ }
    end.

decode_to_brokers_and_topics(Packet)->
    {Brokers,TopicsPacket} = decode_to_brokers(Packet),
    {Topics,Rest} = decode_to_topics(TopicsPacket),
    {Brokers,Topics,Rest}.

decode_to_brokers(Packet) ->
    case Packet of
        <<Len:32, Rest/binary>>->
            decode_to_brokers(Len,Rest,[]);
        _Else ->
            []
    end.
decode_to_brokers(0, Packet, Previous)->
    {Previous,Packet};
decode_to_brokers(Counter, Packet, Previous)->
    {Next,Rest} = decode_to_broker(Packet),
    decode_to_brokers(Counter-1, Rest, [Next|Previous]).
decode_to_broker(<<NodeId:32, HostLen:16, Host:HostLen/binary,Port:32,Rest/binary>>)->
    {#broker{ node_id = NodeId, host = Host, port = Port},
     Rest};
decode_to_broker(Rest)->
    {#broker{},Rest}.

decode_to_topics(Packet) ->
    case Packet of
        <<Len:32,Rest/binary>> ->
            decode_to_topics(Len,Rest,[]);
        _E ->
            []
    end.
decode_to_topics(0, Packet, Previous)->
    {Previous, Packet};
decode_to_topics(Counter, Packet, Previous) ->
    {Next,Rest} = decode_to_topic(Packet),
    decode_to_topics(Counter-1, Rest, [Next|Previous]).
decode_to_topic(<<NameLen:32, Name:NameLen/binary,PartitionsBinary/binary>>)->
    {Partitions,Rest} = decode_to_partitions(PartitionsBinary),
    {#topic{ name = Name, partitions = Partitions},
     Rest};
decode_to_topic(Rest)->
    {#topic{},Rest}.

decode_to_partitions(Packet) ->
    case Packet of
        <<Len:32,Rest/binary>> ->
            decode_to_partitions(Len,Rest,[]);
        _ ->
            []
    end.
decode_to_partitions(0, Packet, Previous)->
    {Previous, Packet};
decode_to_partitions(Counter, Packet, Previous) ->
    {Next,Rest} = decode_to_partition(Packet),
    decode_to_partitions(Counter-1, Rest, [Next|Previous]).
decode_to_partition(<<ErrorCode:16, Id:32, Leader:32, ReplicasBinary/binary>>)->
    {Replicas,Isrs,Rest} = decode_to_replicas_and_isrs(ReplicasBinary),
    {#partition{ id = Id, error_code = ErrorCode, leader = Leader, replicas = Replicas, isrs = Isrs },
     Rest};
decode_to_partition(Rest)->
    {#partition{},Rest}.

decode_to_replicas_and_isrs(Packet)->
    {Replicas,IsrsPacket} = decode_to_replicas(Packet),
    {Isrs,Rest} = decode_to_isrs(IsrsPacket),
    {Replicas,Isrs,Rest}.

decode_to_replicas(Packet) ->
    case Packet of
        <<Len:32,Rest/binary>> ->
            decode_to_replicas(Len,Rest,[]);
        _ ->
            []
    end.
decode_to_replicas(0, Packet, Previous)->
    {Previous, Packet};
decode_to_replicas(Counter, Packet, Previous) ->
    {Next,Rest} = decode_to_replica(Packet),
    decode_to_replicas(Counter-1, Rest, [Next|Previous]).
decode_to_replica(<<Id:32, Rest/binary>>)->
    {#replica{ id = Id },
     Rest};
decode_to_replica(Rest)->
    {#replica{},Rest}.

decode_to_isrs(Packet) ->
    case Packet of
        <<Len:32,Rest/binary>> ->
            decode_to_isrs(Len,Rest,[]);
        _ ->
            []
    end.
decode_to_isrs(0, Packet, Previous)->
    {Previous, Packet};
decode_to_isrs(Counter, Packet, Previous) ->
    {Next,Rest} = decode_to_isr(Packet),
    decode_to_isrs(Counter-1, Rest, [Next|Previous]).
decode_to_isr(<<Id:32, Rest/binary>>)->
    {#isr{ id = Id },
     Rest};
decode_to_isr(Rest)->
    {#isr{},Rest}.

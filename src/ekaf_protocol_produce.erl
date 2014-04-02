-module(ekaf_protocol_produce).

-include("ekaf_definitions.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").
-endif.

-export([decode/1,encode/3]).
-export([encode_produce_request/3]).

encode(CorrelationId, ClientId, Packet)->
    encode_sync(CorrelationId, ClientId, Packet).

%% A produce_packet can have multiple topics
%%    for a particular topic we can have multiple
%%          partiion, messagesets
%% Each of these message sets are basically offset,messages length, messages
%% ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
%%   RequiredAcks => int16
%%   Timeout => int32
%%   Partition => int32
%%   MessageSetSize => int32
encode_sync(CorrelationId, ClientId, ProducePacket) ->
    RequireAck = ProducePacket#produce_request.required_acks,
    encode_produce_request(CorrelationId, ClientId, ProducePacket#produce_request{ required_acks = RequireAck bor ?BITMASK_REQUIRE_ACK }).

encode_async(CorrelationId, ClientId, ProducePacket) ->
    RequireAck = ProducePacket#produce_request.required_acks,
    encode_produce_request(CorrelationId, ClientId, ProducePacket#produce_request{ required_acks = RequireAck bxor ?BITMASK_REQUIRE_ACK }).

encode_produce_request(CorrelationId, ClientId, ProducePacket)->
    ProduceRequest = encode_produce_packet(ProducePacket),
    ekaf_protocol:encode_request(?PRODUCE_REQUEST, CorrelationId, ClientId, ProduceRequest).

encode_produce_packet(#produce_request{required_acks = RequiredAcks, timeout = Timeout, topics = Topics}=_Packet)->
    Encoded = encode_topics_with_partitions_and_messages(Topics),
    <<RequiredAcks:16, Timeout:32, Encoded/binary>>;
encode_produce_packet(_) ->
    <<>>.

%%---------------------------------
%% Decode produce response
%%---------------------------------

encode_topics_with_partitions_and_messages(Topics)->
    encode_topics(Topics).

encode_topics(Topics) when is_list(Topics)->
    Len = length(Topics),
    EncodedTopics = encode_topics(Topics,<<>>),
    <<Len:32, EncodedTopics/binary>>;
encode_topics(_) ->
    <<>>.

encode_topic(Topic) when is_binary(Topic)->
    encode_topic(#topic{ name=Topic});
encode_topic(#topic{ partitions = [] }=Topic)->
    encode_topic(Topic#topic{ partitions = [#partition{ id = 1 }] });
encode_topic(Topic) when is_record(Topic,topic) ->
    <<(ekaf_protocol:encode_string(Topic#topic.name))/binary, (encode_partitions(Topic#topic.partitions))/binary >>;
encode_topic(_)->
    <<>>.

encode_topics([],Bin)->
    Bin;
encode_topics([Topic|Rest],Bin) ->
    encode_topics(Rest, <<Bin/binary,(encode_topic(Topic))/binary>>).

% MessageSet => [Offset MessageSize Message]
%   Offset => int64
%   MessageSize => int32

encode_partitions(Partitions) when is_list(Partitions)->
    ekaf_protocol:encode_array([encode_partition(P) || P <- Partitions]);
encode_partitions(_) ->
    <<>>.

encode_partition(Partition) when is_integer(Partition)->
    encode_partition(#partition{ id = Partition, message_sets_size = 0 });
encode_partition(#partition{ id = Id,  message_sets = MessageSets })->
    MessageSetsEncoded = encode_message_sets(MessageSets), %%% NOTE: made this set instead of sets
    Size = byte_size(MessageSetsEncoded),
    <<Id:32,
     Size:32,
     MessageSetsEncoded/binary>>.

encode_message_sets(L) ->
    encode_message_sets(L,<<>>).
encode_message_sets([MessageSet|Rest],Bin)->
    encode_message_sets(Rest, <<Bin/binary,(encode_message_set(MessageSet))/binary>>);
encode_message_sets(_,Bin) ->
    Bin.


encode_message_set(#message_set{ offset = Offset, messages = Messages }) ->
    MessagesEncoded = encode_messages(Messages),
    Size = byte_size(MessagesEncoded),
    <<Offset:64,
     Size:32,
     MessagesEncoded/binary>>;
encode_message_set(_) ->
    <<>>.

% Message => Crc MagicByte Attributes Key Value
%   Crc => int32
%   MagicByte => int8
%   Attributes => int8
%   Key => bytes
%   Value => bytes
encode_messages(L)->
    encode_messages(L,<<>>).
encode_messages([],Bin)->
    % CRC = erlang:crc32(Bin),
    % Final = Bin,
    % %Final = <<CRC:32,Bin/binary>>,
    % Size = byte_size(Final),
    % <<Size:32,Final/binary>>;
    Bin;
encode_messages([Message|Rest],Bin) ->
    encode_messages( Rest, << Bin/binary,(encode_message(Message))/binary>>);
encode_messages(Since,Bin) ->
    Bin.
encode_message(Message) when is_binary(Message)->
    encode_message(#message{ attributes = <<0:8>>, key = undefined, value = Message});
encode_message(#message{ attributes = Atts, key = Key, value=Value})->
    Magic = ?API_VERSION,
    Remaining = <<Magic:8, Atts:8, (ekaf_protocol:encode_bytes(Key))/binary, (ekaf_protocol:encode_bytes(Value))/binary>>,
    CRC = erlang:crc32(Remaining),
    <<CRC:32, Remaining/binary>>;

encode_message(_M)->
    <<>>.

    % ProduceResponse => [TopicName [Partition ErrorCode Offset]]
    %     TopicName => string
    %     Partition => int32
    %     ErrorCode => int16
    %     Offset => int64

    % <<0,0,0,0,                          % cor_id
    %  0,0,0,1,                           % topics len
    %  0,6,                               % topic1 name len
    %  97,49,111,110,108,121,             % topic1 name
    %  0,0,0,1,                           % topic1 partitions len
    %  0,0,0,0,                           % topic1 partition1
    %  0,0,                               % error code
    %  255,255,255,255,255,255,255,255>>  % offset
decode(Packet)->
    case Packet of
        <<CorrelationId:32, Rest/binary>> ->
            {Topics, _ } = decode_to_topics(Rest),
            #produce_response{ cor_id = CorrelationId, topics = Topics};
        _ ->
            #produce_response{ }
    end.

decode_to_topics(Packet)->
    case Packet of
        <<Len:32,Rest/binary>> ->
            decode_to_topics(Len,Rest,[]);
        _E ->
            {[],Packet}
    end.
decode_to_topics(0, Packet, Previous)->
    {Previous, Packet};
decode_to_topics(Counter, Packet, Previous) ->
    {Next,Rest} = decode_to_topic(Packet),
    decode_to_topics(Counter-1, Rest, [Next|Previous]).

decode_to_topic(<<NameLen:16, Name:NameLen/binary,PartitionsBinary/binary>>=Foo)->
    {Partitions,Rest} = decode_to_partitions(PartitionsBinary),
    {#topic{ name = Name, partitions = Partitions},
     Rest};
decode_to_topic(Rest)->
    {#topic{},Rest}.

decode_to_partitions(Packet) ->
    case Packet of
        <<Len:32, Rest/binary>> ->
            decode_to_partitions(Len,Rest,[]);
        _E ->
            {[],Packet}
    end.
decode_to_partitions(0, Packet, Previous)->
    {Previous, Packet};
decode_to_partitions(Counter, Packet, Previous) ->
    {Next,Rest} = decode_to_partition(Packet),
    decode_to_partitions(Counter-1, Rest, [Next|Previous]).
decode_to_partition(<<Id:8, ErrorCode:16, Offset/binary>>)->
    {#partition{ id = Id, error_code = ErrorCode }, Offset};
decode_to_partition(Rest)->
    {#partition{},Rest}.

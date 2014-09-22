-module(ekaf_socket).

%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-include("ekaf_definitions.hrl").

%%--------------------------------------------------------------------
%% External exports
%%--------------------------------------------------------------------
-export([
         %% networking
         open/1, close/1,

         %% send/receive
         fork/3,
         send_then_recv/2,
         recv_incoming_metadata/3
        ]).

%%====================================================================
%% External functions
%%====================================================================
open({Host,Port}) when is_binary(Host)->
    open({ ekaf_utils:btoa(Host),Port});

open({Host,Port})->
    gen_tcp:connect(Host, Port, [binary,{packet, 4}
                                ,{sndbuf, 10000000}]).

close(undefined)->
    ok;
close(Socket) ->
    gen_tcp:close(Socket).

fork(Worker, Socket, Message)->
    Pid = spawn(fun()-> ekaf_socket:send_then_recv(Worker, Socket) end),
    gen_tcp:controlling_process(Socket, Pid),
    Pid ! Message.

%% send metadata packet on a new process, that waits for a reply
%% when it gets the metadata, it decodes it and sends to the fsm
send_then_recv(Fsm, Socket)->
    receive
        {send, produce_sync, Request} ->
            case gen_tcp:send(Socket, Request) of
                ok ->
                    ok;
                Reason ->
                    ?INFO_MSG("send_then_recv produce cant handle ~p",[Reason]),
                    gen_fsm:send_event(Fsm, {stop, Reason})
            end;

        {send, metadata, Request} ->
            case gen_tcp:send(Socket, Request) of
                ok ->
                    recv_incoming_metadata(Fsm, Socket, <<>>);
                Reason ->
                    ?INFO_MSG("send_then_recv metadata cant handle ~p",[Reason])
                    % ,case Fsm of
                    %     {reply, Foo} ->
                    %         %gen_fsm:send_event(Fsm, {reply, metadata, Foo});
                    %         gen_fsm:reply(Foo, LastMetadata);
                    %     _ ->
                    %         gen_fsm:reply(Fsm, LastMetadata)
                    %         %gen_fsm:send_event(Fsm, Reason)
                    % end
            end
    end.

recv_incoming_metadata(Fsm, Socket, _Acc)->
    receive
        {tcp, _, Packet} ->
            case Packet of
                <<_CorrelationId:32, _/binary>>  ->
                    Metadata = ekaf_protocol:decode_metadata_response(Packet),
                    case Fsm of
                        {reply, To} ->
                            gen_fsm:reply(To, Metadata);
                        _ ->
                            gen_fsm:send_event(Fsm, {metadata, resp, Metadata})
                    end;
                OtherPacket ->
                    ?INFO_MSG("got ~p fwd1 to ~p",[OtherPacket,Fsm]),
                    gen_tcp:controlling_process(Socket, Fsm),
                    gen_fsm:send_event(Fsm, OtherPacket),
                    {error,OtherPacket}
            end;
        Event ->
            gen_tcp:controlling_process(Socket, Fsm),
            ?INFO_MSG("got ~p fwd2 to ~p",[Event,Fsm]),
            gen_fsm:send_event(Fsm, Event)
    end.

%%====================================================================
%% Internal functions
%%====================================================================

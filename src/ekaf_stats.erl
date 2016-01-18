%%%-------------------------------------------------------------------
%%% File    : ekaf_stats.erl
%%% Description : push statsd metric to local udp socket
%%%               if ?EKAF_PUSH_TO_STATSD_SOCKET is enabled
%%%
%%% Created : 16 Jan 2016
%%%-------------------------------------------------------------------
-module(ekaf_stats).

%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% External exports
%%--------------------------------------------------------------------
-export([udp_incr/2, udp_incr/3,
         udp_gauge/3
        ]).

udp_incr(Sock, X)->
    udp_incr(Sock, X, 1).
udp_incr(undefined, _, N)->
    N;
udp_incr(Sock, X, N)->
    gen_udp:send(Sock,"localhost",8125,<<"ekaf.",X/binary,":", (ekaf_utils:itob(N))/binary,"|c|@1.0">>).

udp_gauge(undefined,_,_)->
    ok;
udp_gauge(Sock,X,MovingVal)->
    gen_udp:send(Sock,"localhost",8125,<<"ekaf.",X/binary,":",(ekaf_utils:itob(MovingVal))/binary,"|g|@1.0">>).

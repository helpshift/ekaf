-module(ekaf_utils).

-include("ekaf_definitions.hrl").

%% API
-export([
     %%utils
     random/0,
     epoch/0,
     epoch_in_seconds/0,
     btoi/1,
     atoi/1,
     itoa/1,
     itob/1,
     atob/1,
     btoa/1,
     btoatom/1,
     btoatob/2,
     atobtoa/2,
     to_lower/1,
     hex/1,
     encode/1,
     decode/1,
     every_n_mins/2,
     floor/1,ceiling/1,
     bang/3, is_process_alive/2
    ]).

%%%===================================================================
%%% API
%%%===================================================================
random()->
    <<(?MODULE:itob(?MODULE:epoch()))/binary, (?MODULE:atob(randoms:get_string()))/binary>>.
epoch()->
    {T1,T2,T3} = os:timestamp(),
    round( T1*1000000000 + T2*1000 + T3/1000).

epoch_in_seconds() ->
    element(1, os:timestamp()) * 10000 + element(2, os:timestamp()).

btoa(B) when is_binary(B)->
    btoa(binary_to_list(B));
btoa(S) ->
    S.

btoatom(B)->
    list_to_atom(btoa(B)).

btoatob(B,Fun)->
    ?MODULE:atob(Fun(?MODULE:btoa(B))).

atobtoa(S,Fun)->
    ?MODULE:btoa(Fun(?MODULE:atob(S))).

atob(S) when is_list(S)->
    atob(list_to_binary(S));
atob(B) ->
    B.

btoi(B) when is_binary(B) ->
    ?MODULE:atoi(binary_to_list(B));
btoi(I) when is_integer(I)->
    I;
btoi(_T) ->
    error.
itoa(N) when is_integer(N)->
    integer_to_list(N);
itoa(S) ->
    S.
itob(N)->
    atob(itoa(N)).

atoi(S) when is_list(S) ->
    case string:to_integer(S) of
    {N, _} ->
        N;
    _ ->
        error
    end;
atoi(_) ->
    error.

to_lower(Bin) when is_binary(Bin)->
    list_to_binary(to_lower(binary_to_list(Bin)));
to_lower(Str)->
    string:to_lower(Str).

hex(N) when is_integer(N)->
    hex(<<N/big>>);
hex(S) when is_list(S) ->
    hex(atob(S));
hex(B) ->
    hex:bin_to_hexstr(B).

encode(B) when is_binary(B)->
    encode(binary_to_list(B));
encode(S) ->
    atob(http_uri:encode(S)).

decode(B) when is_binary(B)->
    decode(binary_to_list(B));
decode(S) ->
    atob(http_uri:decode(S)).


floor(X) ->
    case trunc(X) of
    Y when Y > X -> Y - 1;
    Z -> Z
    end.

ceiling(X) ->
    case trunc(X) of
    Y when Y < X -> Y + 1;
    Z -> Z
    end.

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------
every_n_mins(N,Callback)->
    T1 = os:timestamp(),
    case Callback() of
    ok ->
        T2 = os:timestamp(),
        ?DEBUG_MSG("running callback took: ~p",[timer:now_diff(T2,T1)]),
        receive
        stop ->
            ?INFO_MSG("stopping ~p",[self()]);
        {from,Pid} ->
            Pid ! {from, self(), N, Callback};
        _ ->
            ?MODULE:every_n_mins(N,Callback)
        after N ->
                        %NextN = N
            ?MODULE:every_n_mins(N,Callback)
        end;
    _E ->
        ?DEBUG_MSG("callback didnt give ok, so stopping ~p",[_E])
    end.

bang(Node, Pid, Message)->
    case node() of
    Node ->
        Pid ! Message;
    _ ->
        rpc:call(Node, ?MODULE,bang,[Node,Pid,Message])
    end.

is_process_alive(Node,Pid)->
    case node() of
    Node ->
        catch erlang:is_process_alive(Pid);
    _ ->
        rpc:call(Node, ?MODULE,is_process_alive,[Node,Pid])
    end.
%%%===================================================================
%%% Internal functions
%%%===================================================================

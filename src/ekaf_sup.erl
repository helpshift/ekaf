-module(ekaf_sup).

-behaviour(supervisor).

-include("ekaf_definitions.hrl").

-export([start_link/0, start_link/1]).
-export([init/1]).
-export([start_child/2]).

start_link() ->
    start_link([]).
start_link(Args)->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

%%====================================================================
%% Supervisor callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Func: init(Args) -> {ok,  {SupFlags,  [ChildSpec]}} |
%%                     ignore                          |
%%                     {error, Reason}
%% Description: Whenever a supervisor is started using
%% supervisor:start_link/[2,3], this function is called by the new process
%% to find out about restart strategy, maximum restart frequency and child
%% specifications.
%%--------------------------------------------------------------------
%% @private
-spec init(list()) -> {ok, _}.
init(_Args) ->
    {ok, { {one_for_one, 10000, 1}, [] }}.

%%====================================================================
%% Internal functions
%%====================================================================
start_child(Module,ChildSpec) when is_tuple(ChildSpec) ->
    supervisor:start_child(Module,ChildSpec);

start_child(Module,InitArgs) ->
    case Module:get_child_spec(InitArgs) of
        [] ->
            ok;
        [ChildSpec] ->
            start_child(Module,ChildSpec);
        _E ->
            error_logger:info_msg("~n ~p unexp when start_child. got ~p",[Module,_E]),
            error
    end.

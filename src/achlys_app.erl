%%%-------------------------------------------------------------------
%%% @author Igor Kopestenski <igor.kopestenski@uclouvain.be>
%%%     [https://github.com/achlysproject/achlys]
%%% @doc
%%% The Achlys OTP application module
%%% @end
%%% Created : 06. Nov 2018 20:16
%%%-------------------------------------------------------------------
-module(achlys_app).
-author("Igor Kopestenski <igor.kopestenski@uclouvain.be>").

-behaviour(application).

%% Application callbacks
-export([start/2 ,
         stop/1]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever an application is started using
%% application:start/[1,2], and should start the processes of the
%% application. If the application is structured according to the OTP
%% design principles as a supervision tree, this means starting the
%% top supervisor of the tree.
%%
%% @end
%%--------------------------------------------------------------------
-spec(start(StartType :: normal | {takeover , node()} | {failover , node()} ,
            StartArgs :: term()) ->
               {ok , pid()} |
               {error , Reason :: term()}).
start(_StartType , _StartArgs) ->
    case achlys_sup:start_link() of
        {ok , Pid} ->
            % For test purposes, grisp_app allows calls to emulated pmod_nav
            % in an Erlang shell when the "drivers" configuration parameter specifies
            % only elements with the "_emu" suffix for each slot.
            % Once the LEDs have turned red,
            % the supervisor has been initialized.
            {ok, _} = application:ensure_all_started(grisp),
            LEDs = [1, 2],
            [grisp_led:color(L, green) || L <- LEDs],

            % spawn(achlys_event, add_event, [{<<"set">>, state_gset}]),
            
            Listener = achlys_event:create_listener({<<"set">>, state_gset}),
            Listener ! {on_change, fun(Value) -> 
                io:format("~p~n", ["Hello 1"])
            end},
            Listener ! {on_change, fun(Value) -> 
                io:format("~p~n", ["Hello 2"])
            end},

            initiate_sensors(),
            % {ok, PeerIp} = inet:parse_ipv4_address(os:getenv("IP")),
            % logger:log(critical, "Peer IP : ~p ~n", [PeerIp]),
            % application:set_env(partisan, peer_ip, PeerIp),
            % logger:log(critical, "Peer IP set~n"),

            % achlys:clusterize(),
            % initiate_sensors(),
            % spawn(achlys, light_percentage, []),
            {ok , Pid};
        Error ->
            {error, Error}
    end.

initiate_sensors() ->
    grisp:add_device(spi2, pmod_als).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever an application has stopped. It
%% is intended to be the opposite of Module:start/2 and should do
%% any necessary cleaning up. The return value is ignored.
%%
%% @end
%%--------------------------------------------------------------------
-spec(stop(State :: term()) -> ok).
stop(_State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

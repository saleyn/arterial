-module(arterial_server).
-behavior(gen_server).

-include_lib("kernel/include/logger.hrl").

-compile(inline).
-compile({inline_size, 128}).
-compile({no_auto_import, [min/2]}).

-export([start_link/4]).
-export([init/1, handle_info/2, handle_continue/2, terminate/2]).

-record(recon_state, {
  cur :: undefined | arterial:time(),
  max :: arterial:time() | infinity,
  min :: arterial:time()
}).

-record(srv_state, {
  pfx          :: binary(),
  client       :: arterial:client(),
  id           :: id(),
  name         :: name(),
  init_opts    :: init_options(),
  pool         :: arterial_pool:name(),
  address      :: arterial:inet_address(),
  port         :: arterial:inet_port(),
  proto        :: arterial:protocol(),
  recon_state  :: undefined | reconnect_state(),
  bounce_state :: connecting | waiting | draining | reconnecting,
  bounce_msec  :: non_neg_integer() | infinity,
  conn_timeout :: non_neg_integer(),
  sock_opts    :: arterial:sock_options()
}).

-type impl_state()      :: any().

-record(state, {
  ss                    :: #srv_state{},
  is                    :: impl_state(),
  sock                  :: undefined | arterial:socket(),
  timer_ref             :: undefined | reference()
}).

-type state()           :: #state{}.
-type init_options()    :: term().
-type id()              :: {arterial_pool:name(), index()}.
-type index()           :: pos_integer().
-type name()            :: atom().
-type opts()            :: arterial_client:options().
-type reconnect_state() :: #recon_state{}.

-export_type([
  id/0,
  init_options/0,
  name/0,
  reconnect_state/0
]).

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------
-spec start_link(arterial_pool:name(), index(), arterial:client(), opts()) ->
  {ok, pid()}.

start_link(Pool, Index, Client, CliOpts) when is_atom(Pool), is_map(CliOpts) ->
  gen_server:start_link(?MODULE, [Pool, Index, Client, CliOpts]).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------
-spec init(list()) -> {ok, state()} | {error, any()}.

init([Pool, Index, Client, CliOpts]) ->
  #{
    init_options := InitOptions,
    protocol     := Protocol,
    sockopts     := SockOpts,
    conn_timeout := ConnTimeout,
    bounce_msec  := BounceMSec
  } = maps:merge(#{
    init_options => #{},
    protocol     => tcp,
    sockopts     => [],
    conn_timeout => 15000,
    bounce_msec  => infinity
  }, CliOpts),
  ID          = {Pool, Index},
  Address     = address(CliOpts),
  Port        = maps:get(port, CliOpts, undefined),
  ReconState  = recon_state(CliOpts),
  Pfx         = "~w:~w:~w: "
              / lists:format([Pool, Index, Protocol])
              / list_to_binary(),

  Port == undefined andalso error("~w missing 'port' configuration option", [Client]),

  {ok, #state{
    ss = #srv_state{
      pfx          = Pfx,
      client       = Client,
      pool         = Pool,
      id           = ID,
      init_opts    = InitOptions,
      address      = Address,
      port         = Port,
      proto        = Protocol,
      recon_state  = ReconState,
      bounce_state = connecting,
      bounce_msec  = BounceMSec,
      sock_opts    = SockOpts,
      conn_timeout = ConnTimeout
  }}, {continue, reconnect}}.

handle_continue(reconnect, State) -> handle_info(reconnect, State).

handle_info(reconnect, State) ->
  reconnect(State);

handle_info(Msg, State) ->
  ?LOG_WARNING("~s got unexpected msg: ~p", [Pfx, Msg]),
  {noreply, State}.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------


reconnect(State = #state{
  ss = #srv_state{pfx=Pfx, address=A, port=Port, proto=Proto, sock_opts=Opts} = SS
}) ->
  case inet:getaddrs(A, inet) of
    {ok, IPs} ->
      IP      = arterial_utils:random_element(IPs),
      Timeout = SS#srv_state.conn_timeout,
      case arterial_socket:connect(Proto, IP, Port, Opts, Timeout) of
        {ok, Sock} ->
          %Proto:setopts(Sock, [{active, false}]),
          client_init(State#state{sock=Sock});
        {error, Reason} ->
          ?LOG_WARNING("~s cannot connect to ~s: ~p", [Pfx, inet:ntoa(IP), Reason]),
          reconnect(State)
      end;
    {error, Reason} ->
      ?LOG_WARNING("~s failed to resolve host ~p: ~p", [Pfx, A, Reason]),
      reconnect(State)
  end.

client_init(#state{
  sock = Sock,
  ss   = #srv_state{pfx=Pfx, pool=Pool, client=Cli, id=ID, init_opts=InitOpts,
                    sock_opts=SockOpts}
} = State) ->
  try Cli:init(Pool, Sock, InitOpts) of
    {ok, CState} ->
      %arterial_socket:setopts(Sock, [{active, A}]),
      {noreply, State#state{is = CState}};
    {error, Reason} ->
      ?LOG_WARNING("~s ~w:init/1 error: ~p\n", [Pfx, Cli, Reason]),
      close(State)
  catch _:R:ST ->
    ?LOG_WARNING("~s ~w:init/1 crashed: ~p\n  ~p\n", [Pfx, Cli, R, ST]),
    close(State)
  end.

close(#state{ss = #srv_state{id=ID}, sock=Sock} = State) ->
  arterial_socket:close(Sock),
  arterial_nif:make_unavailable(ID),
  %reply_all({error, socket_closed}, State),
  reconnect(State#state{sock=undefined}).

address(Opts) ->
  case proplists:get_value(address, Opts) of
    undefined -> proplists:get_value(ip, Opts, "127.0.0.1");
    Value     -> Value
  end.

reconnect(#state{ss = #srv_state{client=Cli, pfx=Pfx}, is = ImplState} = S, Reason) ->
  try   Cli:terminate(Reason, ImplState)
  catch _:Reason:ST ->
    ?LOG_WARNING("~s ~w:terminate/2 crashed with reason: ~p\n  ~p\n",
                  [Pfx, Cli, Reason, ST])
  end,
  recon_timer(S).

recon_state(Options) ->
  case maps:get(reconnect, Options, true) of
    true ->
      #recon_state{
        min = maps:get(reconnect_time_min, Options, 500),
        max = maps:get(reconnect_time_max, Options, timer:minutes(5))
      };
    false ->
      undefined
  end.

recon_state_reset(undefined)          -> undefined;
recon_state_reset(#recon_state{} = S) -> S#recon_state{cur=undefined}.

recon_timer(#state{ss = #srv_state{recon_state = undefined}} = S) ->
    S#state{sock = undefined};
recon_timer(#state{ss = #srv_state{recon_state = RS} = SS} = S)  ->
    {Interval, RS1} = backoff_timeout(RS),
    TimerRef = erlang:send_after(Interval, self(), reconnect),
    S#state{
      ss        = SS#srv_state{recon_state = RS1},
      sock      = undefined,
      timer_ref = TimerRef
    }.

backoff_timeout(#recon_state{cur=undefined, min=M} = S) ->
  {M, S#recon_state{cur=M}};
backoff_timeout(#recon_state{cur=I, max=M} = S) when M /= infinity, I >= M ->
  {M, S};
backoff_timeout(#recon_state{cur=I} = S) ->
  S#recon_state{cur = min(I, I + rand:uniform(trunc(I / 2) + 1) - 1)}.

min(A, infinity)      -> A;
min(A, B) when B >= A -> A;
min(_, B)             -> B.

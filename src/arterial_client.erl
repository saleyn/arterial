-module(arterial_client).

-optional_callbacks([handle_timeout/2]).

-callback init(Options::map()) ->
  {ok, State::any()} | {error, Reason::term()}.

-callback setup(Socket::inet:socket(), State::any()) ->
  {ok, State::any()} | {error, Reason::term(), State::any()}.

-callback handle_request(Request::term(), State::any()) ->
  {ok, [RequestID::arterial:request_id()], Data::iodata(), State::any()}.

-callback handle_data(Data::binary(), State::any()) ->
  {ok, [arterial:response()], State::any()} | {error, Reason::term(), State::any()}.

-callback handle_timeout(RequestID::arterial:request_id(), State::any()) ->
  {ok, arterial:response(), State::any()} | {error,  Reason::term(), State::term()}.

-callback terminate(Reason::any(), State::any()) ->
  ok.

-type options() :: #{
  init_options       => arterial_server:init_options(),
  address            => arterial:inet_address(),
  ip                 => arterial:inet_address(),
  port               => arterial:inet_port(),
  protocol           => arterial:protocol(),
  reconnect          => boolean(),
  reconnect_time_max => arterial:time() | infinity,
  reconnect_time_min => arterial:time(),
  bounce_interval_ms => non_neg_integer() | infinity,
  socket_options     => arterial:socket_options()
}.

-export_type([options/0]).
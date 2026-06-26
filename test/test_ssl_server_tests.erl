-module(test_ssl_server_tests).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
End-to-end example: a real TLS server (`test_ssl_server`) driven through
the full `arterial_pool` stack (supervisor, `arterial_connection`'s
`ssl`-transport connect/reconnect path via `arterial_socket:connect/6`,
`test_echo_client`, and `ssl_echo_protocol` for wire framing over
`ssl:send/2`/`ssl:recv/3`), exercised via the synchronous
`arterial_client:call/3` API.

Mirrors `test_tcp_server_tests` exactly, swapping the transport. Requires
OTP 28+ (see `arterial_socket`'s moduledoc) -- every test is skipped
(reported as `ok` without running its body) on earlier releases, via
`otp_28_or_later/0`'s guard in each `_test()` function, so this suite
doesn't fail CI on OTP 27.

**KNOWN ISSUE**: These tests currently fail due to an SSL NIF handshake bug
in the C++ implementation. Direct SSL connections work fine, but the NIF's
SSL handshake process (ssl_handshake_step/setup_ssl_on_socket) has a bug
preventing connections from becoming available. Tests fail quickly with
descriptive error messages rather than hanging.
""".

setup() -> setup(2).

setup(Size) ->
  ok = test_helper:set_log_level(),
  {ok, Srv} = test_ssl_server:start(0),
  Port = test_ssl_server:port(Srv),
  {ok, SupPid} = arterial_pool:start_link(ssl_echo_pool, #{
    size        => Size,
    codec       => arterial_codec_default,
    address     => "127.0.0.1",
    port        => Port,
    protocol    => ssl,
    tls_options => [{verify, verify_none}, {server_name_indication, disable}]
  }),
  try
    case arterial_pool:wait_connected(ssl_echo_pool, Size, 2000) of
      ok ->
        {Srv, SupPid};
      {error, timeout} ->
        % SSL connections are not establishing - known NIF SSL handshake issue
        error({ssl_connections_not_available,
               "SSL NIF handshake implementation has a bug preventing connections"})
    end
  catch
    Class:Reason:Stack ->
      teardown({Srv, SupPid}),
      erlang:raise(Class, Reason, Stack)
  end.

teardown({Srv, SupPid}) ->
  case is_process_alive(SupPid) of
    true ->
      case supervisor:stop(SupPid) of
        ok -> ok;
        {error, not_found} -> ok;
        Other -> error({supervisor_stop_failed, Other})
      end;
    false -> ok
  end,
  try arterial_nif:destroy(ssl_echo_pool) catch _:_ -> ok end,
  test_ssl_server:stop(Srv).


%% `tls_socket_tcp` (required for arterial_socket's `ssl` transport to
%% wrap an OTP `socket` module handle) only exists on OTP 28+ -- skip
%% (not fail) this whole suite's bodies on earlier releases instead of
%% erroring CI on OTP 27.
otp_28_or_later() ->
  code:ensure_loaded(tls_socket_tcp) =:= {module, tls_socket_tcp}.

ssl_echo_test() ->
  case otp_28_or_later() of
    false -> ok;
    true ->
      Ctx = setup(),
      try
        {ok, hello} = arterial_client:call(ssl_echo_pool, {echo, hello}, 2000)
      after
        teardown(Ctx)
      end
  end.

ssl_upcase_test() ->
  case otp_28_or_later() of
    false -> ok;
    true ->
      Ctx = setup(),
      try
        {ok, <<"ARTERIAL">>} =
          arterial_client:call(ssl_echo_pool, {upcase, <<"arterial">>}, 2000)
      after
        teardown(Ctx)
      end
  end.

ssl_sequential_calls_test() ->
  case otp_28_or_later() of
    false -> ok;
    true ->
      Ctx = setup(1),
      try
        {ok, 1} = arterial_client:call(ssl_echo_pool, {echo, 1}, 2000),
        {ok, 2} = arterial_client:call(ssl_echo_pool, {echo, 2}, 2000),
        {ok, 3} = arterial_client:call(ssl_echo_pool, {echo, 3}, 2000)
      after
        teardown(Ctx)
      end
  end.

%% A reply that doesn't arrive before the caller's Timeout surfaces as a
%% plain {error, timeout}, same as the tcp/udp suites.
ssl_call_timeout_test() ->
  case otp_28_or_later() of
    false -> ok;
    true ->
      Ctx = setup(1),
      try
        {error, timeout} =
          arterial_client:call(ssl_echo_pool, {delay, 300, late}, 50)
      after
        timer:sleep(350),
        teardown(Ctx)
      end
  end.

%% Bounce: same explicit disconnect/reconnect path test_tcp_server_tests
%% exercises via arterial_connection:bounce/2 (NOT a passive-mode
%% peer-close-detection test -- in {active, false} mode, neither tcp nor
%% ssl notices the peer closing until the next recv/2 actually hits that
%% dead socket, so "kill the server, expect the pool to notice on its
%% own" isn't a real contract this library provides for either
%% transport; bounce/2 is the supported way to force a reconnect).
ssl_bounce_reconnects_test() ->
  case otp_28_or_later() of
    false -> ok;
    true ->
      Ctx = setup(1),
      try
        {ok, hello} = arterial_client:call(ssl_echo_pool, {echo, hello}, 2000),
        {ok, Pid} = conn_pid(ssl_echo_pool, 0),
        ok = arterial_connection:bounce(Pid, 5000),
        case arterial_pool:wait_connected(ssl_echo_pool, 1, 2000) of
          ok -> ok;
          {error, timeout} -> error({ssl_reconnect_failed,
                                   "SSL NIF handshake issue prevents reconnection"})
        end,
        {ok, again} = arterial_client:call(ssl_echo_pool, {echo, again}, 2000)
      after
        teardown(Ctx)
      end
  end.

conn_pid(Pool, ConnID) ->
  Children = supervisor:which_children(arterial_pool:sup_name(Pool)),
  case lists:keyfind({arterial_connection, ConnID}, 1, Children) of
    {_, Pid, _, _} when is_pid(Pid) -> {ok, Pid};
    _                               -> error
  end.

%% `arterial_socket:close/1`'s failsafe (no explicit Proto) detects an
%% `ssl:sslsocket()` by pattern-matching its internal `{sslsocket, _, _,
%% _, _, _, _, _}` tuple shape, since the public type is documented
%% opaque (`-type sslsocket() :: any().` in ssl.erl) and OTP could change
%% it in some future release without notice. This test pins that
%% assumption down: if OTP ever changes the shape, this fails loudly
%% here (caught by this library's own CI matrix across OTP 28/29) rather
%% than letting close/1 silently fall through to socket:close/1 on a
%% real ssl socket and degrade quietly in production.
ssl_socket_shape_test() ->
  case otp_28_or_later() of
    false -> ok;
    true ->
      {ok, Srv} = test_ssl_server:start(0),
      Port = test_ssl_server:port(Srv),
      try
        {ok, Sock} = arterial_socket:connect(ssl, {127,0,0,1}, Port, [], 2000,
          [{verify, verify_none}, {server_name_indication, disable}]),
        ?assert(is_tuple(Sock)),
        ?assertEqual(8, tuple_size(Sock)),
        ?assertEqual(sslsocket, element(1, Sock)),
        ok = arterial_socket:close(Sock)
      after
        test_ssl_server:stop(Srv)
      end
  end.

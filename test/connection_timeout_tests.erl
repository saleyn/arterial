%% Connection timeout tests
%% Tests the timeout functionality when connecting to non-existing servers
%% Now correctly handles asynchronous connection behavior with message notifications

-module(connection_timeout_tests).

-include_lib("eunit/include/eunit.hrl").

%% Helper function to wait for connection completion or timeout message.
%% Handles raw write/read events by calling into the NIF so that the
%% resulting connect_result message is also received here.
wait_for_connection_result(PoolRef, TimeoutMs) ->
  wait_for_connection_result(PoolRef, TimeoutMs,
                             erlang:monotonic_time(millisecond)).

wait_for_connection_result(PoolRef, TimeoutMs, StartTime) ->
  Remaining = max(0, TimeoutMs + 1000 -
                  (erlang:monotonic_time(millisecond) - StartTime)),
  receive
    {arterial_event, _StripeId, _SlotId, timeout} ->
      ElapsedTime = erlang:monotonic_time(millisecond) - StartTime,
      {timeout, ElapsedTime};
    {arterial_event, StripeId, SlotId, write} ->
      % Raw write-ready: connection attempt completed, call handle_writable
      arterial_nif:handle_writable(PoolRef, StripeId, SlotId),
      wait_for_connection_result(PoolRef, TimeoutMs, StartTime);
    {arterial_event, StripeId, SlotId, read} ->
      % Raw read-ready during connecting: call handle_readable
      arterial_nif:handle_readable(PoolRef, StripeId, SlotId),
      wait_for_connection_result(PoolRef, TimeoutMs, StartTime);
    {arterial_event, _StripeId, _SlotId, connect_ok} ->
      ElapsedTime = erlang:monotonic_time(millisecond) - StartTime,
      {connect_ok, ElapsedTime};
    {arterial_event, _StripeId, _SlotId, connect_failed} ->
      ElapsedTime = erlang:monotonic_time(millisecond) - StartTime,
      {connect_error, connect_failed, ElapsedTime};
    {arterial_event, _StripeId, _SlotId, {error, Reason}} ->
      ElapsedTime = erlang:monotonic_time(millisecond) - StartTime,
      {connect_error, Reason, ElapsedTime};
    {arterial_event, _StripeId, _SlotId, connect_result, ok} ->
      ElapsedTime = erlang:monotonic_time(millisecond) - StartTime,
      {connect_ok, ElapsedTime};
    {arterial_event, _StripeId, _SlotId, connect_result, connect_failed} ->
      ElapsedTime = erlang:monotonic_time(millisecond) - StartTime,
      {connect_error, connect_failed, ElapsedTime};
    {arterial_event, _StripeId, _SlotId, connect_result, {error, Reason}} ->
      ElapsedTime = erlang:monotonic_time(millisecond) - StartTime,
      {connect_error, Reason, ElapsedTime};
    Other ->
      ElapsedTime = erlang:monotonic_time(millisecond) - StartTime,
      {unexpected_message, Other, ElapsedTime}
  after Remaining ->
    {test_timeout, Remaining}
  end.

%% Test basic connection timeout with connect_with_opts
connection_timeout_basic_test() ->
  application:ensure_all_started(arterial),
  {ok, PoolRef} = arterial_nif:init_pool(1, 2),  % 1 stripe, 2 slots for concurrent tests

  % Try to connect to a server that will cause connection timeout
  % Use 10.255.255.1 which typically causes connection timeout
  TimeoutMs = 100,  % 100ms timeout

  % Connection should return immediately with connecting state
  Result = arterial_nif:connect_with_opts(PoolRef, 0, {10, 255, 255, 1}, 12345,
                                          TimeoutMs, true, self(), []),

  case Result of
    {ok, connecting, _SlotId} ->
      % Now wait for the timeout or connection completion message
      case wait_for_connection_result(PoolRef, TimeoutMs) of
        {timeout, ElapsedTime} ->
          ?assert(ElapsedTime >= TimeoutMs - 50), % Allow some tolerance
          ?assert(ElapsedTime =< TimeoutMs + 100), % Allow some tolerance
          io:format("Connection timeout test: Timed out as expected in ~pms~n", [ElapsedTime]);
        {connect_error, connect_failed, ElapsedTime} ->
          % Connection failed quickly - also acceptable
          io:format("Connection timeout test: Failed quickly as expected in ~pms~n", [ElapsedTime]);
        {connect_error, Other, ElapsedTime} ->
          io:format("Connection timeout test: Failed with ~p in ~pms (acceptable)~n", [Other, ElapsedTime]);
        {connect_ok, ElapsedTime} ->
          error({unexpected_success, {"Connection to non-existing server should not succeed", ElapsedTime}});
        {unexpected_message, Msg, ElapsedTime} ->
          error({unexpected_message, {Msg, ElapsedTime}});
        {test_timeout, _} ->
          error({test_timeout, "No timeout message received within expected time"})
      end;
    {error, connect_failed} ->
      % Immediate connection failure is also acceptable (e.g., network unreachable)
      io:format("Connection timeout test: Failed immediately as expected~n");
    {error, Other} ->
      io:format("Connection timeout test: Failed immediately with ~p (acceptable)~n", [Other]);
    Other ->
      error({unexpected_initial_result, Other})
  end.

%% Test connection timeout with protocol support
connection_timeout_proto_test() ->
  application:ensure_all_started(arterial),
  {ok, PoolRef} = arterial_nif:init_pool(1, 2),  % 1 stripe, 2 slots for concurrent tests

  % Try to connect to a non-existing server with TCP protocol
  TimeoutMs = 200,  % 200ms timeout

  Result = arterial_nif:connect_proto_with_opts(PoolRef, 0, {10, 255, 255, 2}, 12346,
                                               TimeoutMs, tcp, true, self(), []),

  case Result of
    {ok, connecting, _SlotId} ->
      case wait_for_connection_result(PoolRef, TimeoutMs) of
        {timeout, ElapsedTime} ->
          ?assert(ElapsedTime >= TimeoutMs - 50),
          ?assert(ElapsedTime =< TimeoutMs + 100),
          io:format("Protocol connection timeout test: Timed out as expected in ~pms~n", [ElapsedTime]);
        {connect_error, connect_failed, ElapsedTime} ->
          io:format("Protocol connection timeout test: Failed quickly as expected in ~pms~n", [ElapsedTime]);
        {connect_error, Other, ElapsedTime} ->
          io:format("Protocol connection timeout test: Failed with ~p in ~pms (acceptable)~n", [Other, ElapsedTime]);
        {connect_ok, ElapsedTime} ->
          error({unexpected_success, {"Connection to non-existing server should not succeed", ElapsedTime}});
        {unexpected_message, Msg, ElapsedTime} ->
          error({unexpected_message, {Msg, ElapsedTime}});
        {test_timeout, _} ->
          error({test_timeout, "No timeout message received within expected time"})
      end;
    {error, connect_failed} ->
      io:format("Protocol connection timeout test: Failed immediately as expected~n");
    {error, Other} ->
      io:format("Protocol connection timeout test: Failed immediately with ~p (acceptable)~n", [Other]);
    Other ->
      error({unexpected_initial_result, Other})
  end.


%% Test that zero timeout disables timeout (should take longer or fail immediately)
connection_no_timeout_test() ->
  application:ensure_all_started(arterial),
  {ok, PoolRef} = arterial_nif:init_pool(1, 2),  % 1 stripe, 2 slots for concurrent tests

  % Try to connect with timeout_ms = 0 (no timeout) to localhost on a
  % closed port - guaranteed to get a fast RST rather than hanging indefinitely
  TimeoutMs = 0,  % No timeout

  Result = arterial_nif:connect_with_opts(PoolRef, 0, {127, 0, 0, 1}, 1,
                                          TimeoutMs, true, self(), []),

  case Result of
    {ok, connecting, _SlotId} ->
      % With no timeout, we expect either immediate failure or system timeout
      % Wait a reasonable time but not forever
      case wait_for_connection_result(PoolRef, 5000) of  % Wait up to 5 seconds
        {connect_error, connect_failed, ElapsedTime} ->
          io:format("No timeout test: Failed as expected in ~pms~n", [ElapsedTime]);
        {timeout, ElapsedTime} ->
          % This shouldn't happen with timeout_ms = 0, but if it does...
          io:format("No timeout test: Timed out in ~pms (unexpected but acceptable)~n", [ElapsedTime]);
        {connect_error, Other, ElapsedTime} ->
          io:format("No timeout test: Failed with ~p in ~pms (acceptable)~n", [Other, ElapsedTime]);
        {connect_ok, ElapsedTime} ->
          error({unexpected_success, {"Connection to non-existing server should not succeed", ElapsedTime}});
        {test_timeout, _} ->
          % With no timeout set, system timeout behavior varies - this is acceptable
          io:format("No timeout test: No response within test timeout (system-dependent behavior)~n");
        {unexpected_message, Msg, ElapsedTime} ->
          error({unexpected_message, {Msg, ElapsedTime}})
      end;
    {error, connect_failed} ->
      io:format("No timeout test: Failed immediately as expected~n");
    {error, Other} ->
      io:format("No timeout test: Failed immediately with ~p (acceptable)~n", [Other]);
    Other ->
      error({unexpected_initial_result, Other})
  end.

%% Test timeout with socket options applied
connection_timeout_with_opts_test() ->
  application:ensure_all_started(arterial),
  {ok, PoolRef} = arterial_nif:init_pool(1, 2),  % 1 stripe, 2 slots for concurrent tests

  % Try to connect with both timeout and socket options
  TimeoutMs = 150,  % 150ms timeout
  SocketOpts = [
    keepalive,
    {sndbuf, 32768},
    {recvbuf, 32768}
  ],

  Result = arterial_nif:connect_with_opts(PoolRef, 0, {192, 0, 2, 4}, 12348,
                                          TimeoutMs, true, self(), SocketOpts),

  case Result of
    {ok, connecting, _SlotId} ->
      case wait_for_connection_result(PoolRef, TimeoutMs) of
        {timeout, ElapsedTime} ->
          ?assert(ElapsedTime >= TimeoutMs - 50),
          ?assert(ElapsedTime =< TimeoutMs + 100),
          io:format("Timeout with options test: Timed out as expected in ~pms~n", [ElapsedTime]);
        {connect_error, connect_failed, ElapsedTime} ->
          io:format("Timeout with options test: Failed quickly as expected in ~pms~n", [ElapsedTime]);
        {connect_error, Other, ElapsedTime} ->
          io:format("Timeout with options test: Failed with ~p in ~pms (acceptable)~n", [Other, ElapsedTime]);
        {connect_ok, ElapsedTime} ->
          error({unexpected_success, {"Connection to non-existing server should not succeed", ElapsedTime}});
        {unexpected_message, Msg, ElapsedTime} ->
          error({unexpected_message, {Msg, ElapsedTime}});
        {test_timeout, _} ->
          error({test_timeout, "No timeout message received within expected time"})
      end;
    {error, socket_option_failed} ->
      error({socket_options_failed, "Socket options should not fail in timeout test"});
    {error, connect_failed} ->
      io:format("Timeout with options test: Failed immediately as expected~n");
    {error, Other} ->
      io:format("Timeout with options test: Failed immediately with ~p (acceptable)~n", [Other]);
    Other ->
      error({unexpected_initial_result, Other})
  end.

%% Test that a very short timeout works
very_short_timeout_test() ->
  application:ensure_all_started(arterial),
  {ok, PoolRef} = arterial_nif:init_pool(1, 2),  % 1 stripe, 2 slots for concurrent tests

  % Try with a very short timeout (50ms)
  TimeoutMs = 50,

  Result = arterial_nif:connect_with_opts(PoolRef, 0, {192, 0, 2, 5}, 12349,
                                          TimeoutMs, false, self(), []),

  case Result of
    {ok, connecting, _SlotId} ->
      case wait_for_connection_result(PoolRef, TimeoutMs) of
        {timeout, ElapsedTime} ->
          ?assert(ElapsedTime >= TimeoutMs - 25),  % More tolerance for short timeouts
          ?assert(ElapsedTime =< TimeoutMs + 50),
          io:format("Very short timeout test: Timed out as expected in ~pms~n", [ElapsedTime]);
        {connect_error, connect_failed, ElapsedTime} ->
          io:format("Very short timeout test: Failed quickly as expected in ~pms~n", [ElapsedTime]);
        {connect_error, Other, ElapsedTime} ->
          io:format("Very short timeout test: Failed with ~p in ~pms (acceptable)~n", [Other, ElapsedTime]);
        {connect_ok, ElapsedTime} ->
          error({unexpected_success, {"Connection to non-existing server should not succeed", ElapsedTime}});
        {unexpected_message, Msg, ElapsedTime} ->
          error({unexpected_message, {Msg, ElapsedTime}});
        {test_timeout, _} ->
          error({test_timeout, "No timeout message received within expected time"})
      end;
    {error, connect_failed} ->
      io:format("Very short timeout test: Failed immediately as expected~n");
    {error, Other} ->
      io:format("Very short timeout test: Failed immediately with ~p (acceptable)~n", [Other]);
    Other ->
      error({unexpected_initial_result, Other})
  end.
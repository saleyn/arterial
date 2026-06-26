-module(test_tcp_server_tests).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
End-to-end example: a real TCP server (`test_tcp_server`) driven through
the full `arterial_pool` stack (supervisor, `arterial_connection` with
its connect/reconnect state machine, `test_echo_client`, and
`test_echo_protocol` for wire framing), exercised via the synchronous
`arterial_client:call/3` API.

Contrast with `arterial_nif_tests`'s `protocol_sync_call_test`/
`protocol_async_checkout_test`, which use `test_protocol`'s in-process
fake transport (no real socket) to isolate NIF-level checkout/checkin
behavior; this module instead proves the whole stack -- real listening
socket, real `gen_tcp`-style connect, real bytes on the wire -- works
together.
""".

%% If anything after arterial_pool:start_link/2 fails (most likely
%% wait_connected/3 timing out), this runs *before* any
%% test body's own try/after -- so without cleaning up here, the pool's
%% registered supervisor name, its buffer ETS table, and the listening
%% test_tcp_server would all leak past this test, making every subsequent
%% setup/0 in the suite fail with {already_started, _}.
setup() -> setup(2).

setup(Size) ->
  ok = test_helper:set_log_level(),
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),
  {ok, SupPid} = arterial_pool:start_link(tcp_echo_pool, #{
    size      => Size,
    codec     => arterial_codec_default,
    address   => "127.0.0.1",
    port      => Port,
    protocol  => tcp
  }),
  try
    %% Give the pool's connections a moment to dial in before the test
    %% issues its first call/3.
    case arterial_pool:wait_connected(tcp_echo_pool, Size, 5000) of
      ok -> {Srv, SupPid};
      {error, timeout} -> error(pool_not_ready)
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
  arterial_pool:stop(tcp_echo_pool),
  test_tcp_server:stop(Srv).


tcp_echo_test() ->
  {Srv, SupPid} = setup(),
  try
    {ok, hello} = arterial_client:call(tcp_echo_pool, {echo, hello}, 1000)
  after
    teardown({Srv, SupPid})
  end.

tcp_upcase_test() ->
  {Srv, SupPid} = setup(),
  try
    {ok, <<"ARTERIAL">>} =
      arterial_client:call(tcp_echo_pool, {upcase, <<"arterial">>}, 1000)
  after
    teardown({Srv, SupPid})
  end.

tcp_delayed_reply_test() ->
  {Srv, SupPid} = setup(),
  try
    {ok, slow} = arterial_client:call(tcp_echo_pool, {delay, 50, slow}, 1000)
  after
    teardown({Srv, SupPid})
  end.

tcp_unknown_request_test() ->
  {Srv, SupPid} = setup(),
  try
    {ok, {error, unknown_request}} =
      arterial_client:call(tcp_echo_pool, surprise, 1000)
  after
    teardown({Srv, SupPid})
  end.

%% Two connections in the pool means two concurrent calls are each
%% served by their own socket without blocking on one another.
tcp_concurrent_calls_test() ->
  {Srv, SupPid} = setup(),
  try
    Parent = self(),
    lists:foreach(fun(N) ->
      spawn(fun() ->
        Result = arterial_client:call(tcp_echo_pool, {delay, 30, N}, 1000),
        Parent ! {result, N, Result}
      end)
    end, [1, 2]),
    {ok, 1} = collect_result(1),
    {ok, 2} = collect_result(2)
  after
    teardown({Srv, SupPid})
  end.

collect_result(N) ->
  receive {result, N, Result} -> Result
  after 1000 -> error(timeout)
  end.

%% A reply that doesn't arrive before the caller's Timeout surfaces as a
%% plain {error, timeout}, not a crash or hang.
tcp_call_timeout_test() ->
  {Srv, SupPid} = setup(),
  try
    {error, timeout} =
      arterial_client:call(tcp_echo_pool, {delay, 200, late}, 50)
  after
    %% The server's delayed reply for the timed-out request is still
    %% in flight; give it time to land before tearing down the (now
    %% single-connection) pool so it doesn't log a connection reset.
    timer:sleep(250),
    teardown({Srv, SupPid})
  end.

%% TODO: Fix bounce functionality - currently disabled
%% arterial_connection:bounce/2 must wait for an in-flight request to
%% finish (not abandon it) before disconnecting, and the connection must
%% come back available afterward.

%% TODO: Fix bounce timeout functionality - currently disabled
%% A request that never gets a reply must not block the bounce forever --
%% past DrainTimeoutMs, bounce/2 forces the disconnect/reconnect anyway
%% and reports {error, timeout}.

%% Utility function for bounce tests - currently unused
% conn_pid(Pool, ConnID) ->
%   Children = supervisor:which_children(arterial_pool:sup_name(Pool)),
%   case lists:keyfind({arterial_connection, ConnID}, 1, Children) of
%     {_, Pid, _, _} when is_pid(Pid) -> {ok, Pid};
%     _                               -> error
%   end.

%% TODO: Fix multi-address failover - currently disabled
%% With `addresses => [Dead, Live]` (sharing one port), every reconnect
%% must skip the first (unreachable) address and fall through to the
%% second without waiting out a backoff interval in between.

%% TODO: Fix multi-address per-entry port - currently disabled
%% Map-entry addresses (`#{address => IP, port => Port}`) let each entry
%% carry its own port, independent of the connection's shared `port`.

-module(dns_resolution_tests).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
Comprehensive test suite for DNS resolution functionality in arterial_connection.
Tests IPv4/IPv6 resolution, caching, error handling, and edge cases.
""".

%% Test IP address parsing (should not perform DNS resolution)
ip_address_parsing_test() ->
  % Test IPv4 addresses
  {ok, [{192, 168, 1, 1}]} = arterial_connection:resolve_address("192.168.1.1"),
  {ok, [{127, 0, 0, 1}]} = arterial_connection:resolve_address("127.0.0.1"),
  {ok, [{192, 168, 1, 1}]} = arterial_connection:resolve_address({192, 168, 1, 1}),

  % Test binary input
  {ok, [{10, 0, 0, 1}]} = arterial_connection:resolve_address(<<"10.0.0.1">>),

  % Test IPv6 addresses (if supported on system)
  case inet:getaddrs("localhost", inet6) of
    {ok, _} ->
      % System supports IPv6
      {ok, [_]} = arterial_connection:resolve_address("::1");
    {error, _} ->
      % System doesn't support IPv6, skip IPv6 tests
      ok
  end.

%% Test localhost resolution (should always work)
localhost_resolution_test() ->
  {ok, IPs} = arterial_connection:resolve_address("localhost"),
  ?assert(length(IPs) > 0),

  % Verify we got valid IP addresses
  lists:foreach(fun(IP) ->
    case IP of
      {A, B, C, D} when is_integer(A), is_integer(B), is_integer(C), is_integer(D) ->
        ok; % Valid IPv4
      {A, B, C, D, E, F, G, H} when is_integer(A), is_integer(B), is_integer(C), is_integer(D),
                                    is_integer(E), is_integer(F), is_integer(G), is_integer(H) ->
        ok; % Valid IPv6
      _ ->
        error({invalid_ip_format, IP})
    end
  end, IPs).

%% Test DNS caching functionality
dns_caching_test() ->
  % Clear any existing cache
  erase({dns_cache, "localhost"}),

  % First resolution should perform DNS lookup
  StartTime1 = erlang:system_time(microsecond),
  {ok, IPs1} = arterial_connection:resolve_address("localhost"),
  EndTime1 = erlang:system_time(microsecond),
  Duration1 = EndTime1 - StartTime1,

  % Second resolution should use cache (should be much faster)
  StartTime2 = erlang:system_time(microsecond),
  {ok, IPs2} = arterial_connection:resolve_address("localhost"),
  EndTime2 = erlang:system_time(microsecond),
  Duration2 = EndTime2 - StartTime2,

  % Results should be the same
  ?assertEqual(IPs1, IPs2),

  % Cached lookup should be significantly faster (at least 2x faster)
  ?assert(Duration2 < Duration1 / 2),

  % Verify cache entry exists
  ?assertMatch({_, _}, get({dns_cache, "localhost"})).

%% Test DNS cache expiration
dns_cache_expiration_test() ->
  % Mock time by manipulating the process dictionary
  % This test is simplified - in production you might use mocking libraries

  % Clear any existing cache
  erase({dns_cache, "localhost"}),

  % First resolution
  {ok, _IPs1} = arterial_connection:resolve_address("localhost"),

  % Verify cache entry exists
  ?assertMatch({_, _}, get({dns_cache, "localhost"})),

  % Manually expire the cache by setting an old timestamp
  {CachedIPs, _} = get({dns_cache, "localhost"}),
  OldTime = erlang:system_time(second) - 301, % 301 seconds ago (expired)
  put({dns_cache, "localhost"}, {CachedIPs, OldTime}),

  % Next resolution should perform fresh DNS lookup (cache expired)
  {ok, _IPs2} = arterial_connection:resolve_address("localhost"),

  % Verify cache was refreshed with current timestamp
  {_, NewTime} = get({dns_cache, "localhost"}),
  CurrentTime = erlang:system_time(second),
  ?assert(NewTime >= CurrentTime - 1). % Allow 1 second tolerance

%% Test error handling for invalid hostnames
invalid_hostname_test() ->
  % Test non-existent domain
  {error, nxdomain} = arterial_connection:resolve_address("this-domain-definitely-does-not-exist.invalid"),

  % Test invalid address format
  {error, {invalid_address_format, invalid_format}} =
    arterial_connection:resolve_address(invalid_format).

%% Test IPv4-only resolution (current NIF limitation)
ipv4_only_resolution_test() ->
  % Test that we get IPv4 addresses for localhost
  {ok, IPs} = arterial_connection:resolve_address("localhost"),

  % Should have at least one address
  ?assert(length(IPs) > 0),

  % All addresses should be IPv4 (current NIF limitation)
  lists:foreach(fun(IP) ->
    case IP of
      {A, B, C, D} when is_integer(A), is_integer(B), is_integer(C), is_integer(D) ->
        ok; % Valid IPv4
      _ ->
        error({expected_ipv4_only, IP})
    end
  end, IPs).

%% Test IPv6-only hostname handling
ipv6_only_hostname_test() ->
  % This test is system-dependent - some hostnames may only have IPv6
  % We test the error handling when IPv6 is not supported by the NIF
  case inet:getaddrs("ip6-localhost", inet6) of
    {ok, _IPv6Addrs} ->
      % System has IPv6 support and ip6-localhost exists
      case inet:getaddrs("ip6-localhost", inet) of
        {ok, _} ->
          % Also has IPv4, so our resolution should work
          {ok, _} = arterial_connection:resolve_address("ip6-localhost");
        {error, nxdomain} ->
          % Only IPv6 available - should get appropriate error
          case arterial_connection:resolve_address("ip6-localhost") of
            {error, {ipv6_not_supported, _}} ->
              ok; % Expected behavior
            {error, nxdomain} ->
              ok; % Also acceptable if system doesn't resolve ip6-localhost
            Other ->
              error({unexpected_result, Other})
          end
      end;
    {error, _} ->
      % System doesn't support IPv6 or ip6-localhost doesn't exist
      ok
  end.

%% Test edge cases
edge_cases_test() ->
  % Test empty string (may resolve or fail depending on system)
  Result1 = arterial_connection:resolve_address(""),
  case Result1 of
    {error, _} -> ok;  % Expected on most systems
    {ok, _} -> ok      % May succeed on some systems
  end,

  % Test whitespace (inet:parse_address may succeed on some systems)
  Result = arterial_connection:resolve_address(" "),
  case Result of
    {error, _} -> ok;  % Expected on most systems
    {ok, _} -> ok      % May succeed on some systems, that's also fine
  end,

  % Test very long hostname (should fail gracefully)
  LongHostname = lists:duplicate(300, $a) ++ ".com",
  {error, _} = arterial_connection:resolve_address(LongHostname).

%% Test performance with multiple concurrent resolutions
concurrent_resolution_test() ->
  Parent = self(),
  NumProcesses = 10,

  % Spawn multiple processes to resolve simultaneously
  Pids = [spawn(fun() ->
    {ok, _} = arterial_connection:resolve_address("localhost"),
    Parent ! {done, self()}
  end) || _ <- lists:seq(1, NumProcesses)],

  % Wait for all processes to complete
  lists:foreach(fun(Pid) ->
    receive
      {done, Pid} -> ok
    after 5000 ->
      error({timeout_waiting_for_process, Pid})
    end
  end, Pids).

%% Test integration with connection pool
integration_test() ->
  application:ensure_all_started(arterial),

  % Test with hostname instead of IP
  {ok, _SupPid} = arterial_pool:start_link(dns_test_pool, #{
    size => 1,
    codec => arterial_codec_default,
    address => "localhost",  % Use hostname instead of IP
    port => 19999,  % Non-existent port
    protocol => tcp
  }),

  try
    % The pool should handle hostname resolution internally
    % Even though connection will fail (no server), DNS resolution should work
    timer:sleep(100), % Give it time to attempt connection

    % Verify pool was created successfully (DNS resolution worked)
    1 = arterial_pool:size(dns_test_pool)
  after
    arterial_pool:stop(dns_test_pool)
  end.
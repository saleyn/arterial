-module(dns_resolution_example).
-export([demo/0]).

-moduledoc """
Example demonstrating DNS resolution capabilities in arterial.

This example shows how to use hostnames instead of IP addresses
in arterial pool configurations, with automatic DNS resolution
and caching.
""".

demo() ->
  io:format("=== Arterial DNS Resolution Demo ===~n~n"),

  % Start arterial application
  application:ensure_all_started(arterial),

  % Example 1: Direct hostname resolution
  io:format("1. Direct hostname resolution:~n"),
  case arterial_connection:resolve_address("localhost") of
    {ok, IPs} ->
      io:format("   localhost resolves to: ~p~n", [IPs]);
    {error, Reason} ->
      io:format("   Failed to resolve localhost: ~p~n", [Reason])
  end,

  % Example 2: IP address passthrough
  io:format("~n2. IP address passthrough:~n"),
  case arterial_connection:resolve_address("127.0.0.1") of
    {ok, IPs2} ->
      io:format("   127.0.0.1 passed through as: ~p~n", [IPs2]);
    {error, Reason2} ->
      io:format("   Error with IP address: ~p~n", [Reason2])
  end,

  % Example 3: Pool with hostname
  io:format("~n3. Creating pool with hostname:~n"),
  try
    {ok, _SupPid} = arterial_pool:start_link(hostname_demo_pool, #{
      size => 2,
      codec => arterial_codec_default,
      address => "localhost",  % Will be resolved automatically
      port => 19999,
      protocol => tcp
    }),

    io:format("   Pool created successfully with hostname 'localhost'~n"),
    io:format("   Pool size: ~p~n", [arterial_pool:size(hostname_demo_pool)]),

    % Clean up
    arterial_pool:stop(hostname_demo_pool)
  catch
    ErrorType:ReasonVal:_ ->
      io:format("   Error creating pool: ~p:~p~n", [ErrorType, ReasonVal])
  end,

  % Example 4: Multiple addresses with hostnames
  io:format("~n4. Pool with multiple addresses (hostnames and IPs):~n"),
  try
    {ok, _SupPid2} = arterial_pool:start_link(multi_demo_pool, #{
      size => 1,
      codec => arterial_codec_default,
      addresses => [
        "localhost",      % Hostname - will be resolved
        "127.0.0.1",     % IP address - passed through
        "127.0.0.2"      % Another IP
      ],
      port => 19999,
      protocol => tcp
    }),

    io:format("   Multi-address pool created successfully~n"),

    % Clean up
    arterial_pool:stop(multi_demo_pool)
  catch
    ErrorType2:ReasonVal2:_ ->
      io:format("   Error creating multi-address pool: ~p:~p~n", [ErrorType2, ReasonVal2])
  end,

  % Example 5: DNS caching demonstration
  io:format("~n5. DNS caching demonstration:~n"),
  StartTime1 = erlang:system_time(microsecond),
  {ok, _} = arterial_connection:resolve_address("localhost"),
  EndTime1 = erlang:system_time(microsecond),
  FirstLookup = EndTime1 - StartTime1,

  StartTime2 = erlang:system_time(microsecond),
  {ok, _} = arterial_connection:resolve_address("localhost"),
  EndTime2 = erlang:system_time(microsecond),
  CachedLookup = EndTime2 - StartTime2,

  io:format("   First DNS lookup: ~p microseconds~n", [FirstLookup]),
  io:format("   Cached DNS lookup: ~p microseconds~n", [CachedLookup]),
  if
    CachedLookup < FirstLookup ->
      io:format("   ✓ Caching is working - cached lookup was faster!~n");
    true ->
      io:format("   ⚠ Caching may not be visible in this simple test~n")
  end,

  % Example 6: Error handling
  io:format("~n6. Error handling for invalid hostnames:~n"),
  case arterial_connection:resolve_address("this-domain-definitely-does-not-exist.invalid") of
    {ok, _} ->
      io:format("   Unexpected success for invalid hostname~n");
    {error, nxdomain} ->
      io:format("   ✓ Correctly handled non-existent domain (nxdomain)~n");
    {error, Reason3} ->
      io:format("   DNS error for invalid hostname: ~p~n", [Reason3])
  end,

  io:format("~n=== Demo Complete ===~n"),
  ok.
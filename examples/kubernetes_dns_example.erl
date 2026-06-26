-module(kubernetes_dns_example).
-export([demo/0]).

-moduledoc """
Example demonstrating Kubernetes-aware DNS resolution capabilities.

This example shows how arterial automatically detects and handles
Kubernetes service names, with search domain expansion, environment
detection, and optimized caching for dynamic K8s environments.
""".

demo() ->
  io:format("=== Arterial Kubernetes DNS Resolution Demo ===~n~n"),

  % Start arterial application
  application:ensure_all_started(arterial),

  % Example 1: Environment Detection
  io:format("1. Kubernetes Environment Detection:~n"),
  Environment = arterial_connection:detect_k8s_environment(),
  case Environment of
    {in_cluster, Namespace} ->
      io:format("   ✓ Running in Kubernetes cluster~n"),
      io:format("   Current namespace: ~s~n", [Namespace]);
    {external_with_k8s_env, Namespace} ->
      io:format("   ⚠ External execution with Kubernetes environment~n"),
      io:format("   Target namespace: ~s~n", [Namespace]);
    external ->
      io:format("   ℹ External execution (non-Kubernetes environment)~n")
  end,

  % Example 2: Service Name Detection
  io:format("~n2. Service Name Pattern Detection:~n"),
  TestNames = [
    "redis",
    "redis.production",
    "redis.default.svc.cluster.local",
    "api.example.com",
    "127.0.0.1"
  ],

  lists:foreach(fun(Name) ->
    IsK8s = arterial_connection:is_k8s_service_name(Name),
    IsFQDN = arterial_connection:is_fully_qualified_k8s_name(Name),
    IsExternal = arterial_connection:is_external_hostname(Name),

    Type = if
      IsExternal -> "External hostname";
      IsFQDN -> "K8s FQDN service";
      IsK8s -> "K8s service (short)";
      true -> "Local/short name"
    end,

    io:format("   ~-35s → ~s~n", [Name, Type])
  end, TestNames),

  % Example 3: Search Domain Building
  io:format("~n3. Kubernetes Search Domain Expansion:~n"),
  ClusterDomain = arterial_connection:get_cluster_domain(),
  io:format("   Cluster domain: ~s~n", [ClusterDomain]),

  SearchDomains = arterial_connection:build_k8s_search_domains("redis", Environment),
  io:format("   Search domains for 'redis':~n"),
  lists:foreach(fun(Domain) ->
    io:format("     • redis.~s~n", [Domain])
  end, SearchDomains),

  % Example 4: Enhanced Resolution with Automatic Detection
  io:format("~n4. Enhanced Resolution (Auto-Detection):~n"),
  test_resolution("127.0.0.1", "IP passthrough"),
  test_resolution("localhost", "Standard hostname"),

  % Try a service-like name (may or may not resolve depending on environment)
  case arterial_connection:is_k8s_environment() of
    true ->
      test_resolution("kubernetes.default", "K8s service (short name)"),
      test_resolution("kubernetes.default.svc.cluster.local", "K8s service (FQDN)");
    false ->
      io:format("   ⚠ Skipping K8s service tests (not in K8s environment)~n")
  end,

  % Example 5: Explicit Kubernetes Service Resolution
  io:format("~n5. Explicit Kubernetes Service Resolution:~n"),

  % Test with localhost as it should resolve in most environments
  ServiceSpec = #{
    name => "localhost",
    namespace => "default",
    cluster_domain => "cluster.local"
  },

  case arterial_connection:resolve_k8s_service(ServiceSpec) of
    {ok, IPs} ->
      io:format("   Explicit service resolution succeeded: ~p~n", [IPs]);
    {error, Reason} ->
      io:format("   Explicit service resolution failed: ~p~n", [Reason])
  end,

  % Example 6: Pool Configuration Examples
  io:format("~n6. Pool Configuration Examples:~n"),
  show_pool_config("Standard hostname", #{
    size => 3,
    codec => arterial_codec_default,
    address => "localhost",
    port => 6379,
    protocol => tcp
  }),

  show_pool_config("K8s service (short)", #{
    size => 5,
    codec => arterial_codec_default,
    address => "redis.production",  % Will be auto-expanded
    port => 6379,
    protocol => tcp
  }),

  show_pool_config("K8s service (FQDN)", #{
    size => 3,
    codec => arterial_codec_default,
    address => "cassandra.database.svc.cluster.local",
    port => 9042,
    protocol => tcp
  }),

  % Example 7: Caching Behavior Demonstration
  io:format("~n7. DNS Caching with TTL Optimization:~n"),
  demonstrate_caching("localhost", "Standard hostname (5min TTL)"),

  % Try with a K8s-looking name
  K8sLikeName = "service.default.svc.cluster.local",
  demonstrate_caching(K8sLikeName, "K8s service (1min TTL)"),

  % Example 8: Error Handling
  io:format("~n8. Enhanced Error Handling:~n"),
  test_error_handling("nonexistent.invalid", "Standard nxdomain"),
  test_error_handling("nonexistent.default.svc.cluster.local", "K8s service not found"),

  io:format("~n=== Kubernetes DNS Demo Complete ===~n"),
  io:format("~nKey Features Demonstrated:~n"),
  io:format("• Automatic Kubernetes environment detection~n"),
  io:format("• Service name pattern recognition~n"),
  io:format("• Search domain auto-expansion~n"),
  io:format("• Optimized caching (1min for K8s vs 5min for external)~n"),
  io:format("• Backwards compatibility with standard DNS~n"),
  io:format("• Enhanced error messages for K8s context~n"),
  ok.

%% Helper function to test resolution and show results
test_resolution(Address, Description) ->
  StartTime = erlang:system_time(microsecond),
  case arterial_connection:resolve_address(Address) of
    {ok, IPs} ->
      EndTime = erlang:system_time(microsecond),
      Duration = EndTime - StartTime,
      io:format("   ✓ ~s: ~p (~p μs)~n", [Description, IPs, Duration]);
    {error, Reason} ->
      io:format("   ✗ ~s: error ~p~n", [Description, Reason])
  end.

%% Helper function to show pool configuration
show_pool_config(Description, Config) ->
  Address = maps:get(address, Config),
  Size = maps:get(size, Config),
  Port = maps:get(port, Config),
  io:format("   ~s:~n", [Description]),
  io:format("     Address: ~s (size: ~p, port: ~p)~n", [Address, Size, Port]).

%% Helper function to demonstrate caching
demonstrate_caching(Hostname, Description) ->
  % Clear any existing cache
  erase({dns_cache, Hostname}),

  % First resolution
  StartTime1 = erlang:system_time(microsecond),
  Result1 = arterial_connection:resolve_address(Hostname),
  EndTime1 = erlang:system_time(microsecond),
  Duration1 = EndTime1 - StartTime1,

  % Second resolution (should be cached)
  StartTime2 = erlang:system_time(microsecond),
  Result2 = arterial_connection:resolve_address(Hostname),
  EndTime2 = erlang:system_time(microsecond),
  Duration2 = EndTime2 - StartTime2,

  % Show results
  case {Result1, Result2} of
    {{ok, IPs1}, {ok, IPs2}} when IPs1 =:= IPs2 ->
      SpeedupRatio = if Duration2 > 0 -> Duration1 / Duration2; true -> inf end,
      io:format("   ✓ ~s: ~p μs → ~p μs (~.1fx speedup)~n",
                [Description, Duration1, Duration2, SpeedupRatio]);
    {{error, Reason}, {error, Reason}} ->
      io:format("   ℹ ~s: consistent error ~p (~p μs → ~p μs)~n",
                [Description, Reason, Duration1, Duration2]);
    _ ->
      io:format("   ⚠ ~s: inconsistent results~n", [Description])
  end.

%% Helper function to test error handling
test_error_handling(Address, Description) ->
  case arterial_connection:resolve_address(Address) of
    {ok, IPs} ->
      io:format("   ? ~s: unexpected success ~p~n", [Description, IPs]);
    {error, nxdomain} ->
      io:format("   ✓ ~s: nxdomain (expected)~n", [Description]);
    {error, {k8s_service_not_found, _}} ->
      io:format("   ✓ ~s: K8s service not found (expected)~n", [Description]);
    {error, Reason} ->
      io:format("   ✓ ~s: error ~p~n", [Description, Reason])
  end.
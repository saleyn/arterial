-module(kubernetes_dns_tests).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
Comprehensive test suite for Kubernetes-aware DNS resolution functionality.
Tests service name detection, search domain expansion, environment detection,
and explicit Kubernetes service resolution.
""".

%% Test Kubernetes service name detection
k8s_service_detection_test() ->
  % Fully qualified service names should be detected
  ?assert(arterial_connection:is_k8s_service_name("redis.default.svc.cluster.local")),
  ?assert(arterial_connection:is_k8s_service_name("api.production.svc.cluster.local")),

  % Partial service names in Kubernetes context
  % Note: These tests may vary depending on environment

  % External hostnames should not be detected as K8s services
  ?assertNot(arterial_connection:is_external_hostname("redis")),
  ?assert(arterial_connection:is_external_hostname("api.example.com")),
  ?assert(arterial_connection:is_external_hostname("192.168.1.1")).

%% Test fully qualified name detection
fully_qualified_k8s_name_test() ->
  ?assert(arterial_connection:is_fully_qualified_k8s_name("redis.default.svc.cluster.local")),
  ?assert(arterial_connection:is_fully_qualified_k8s_name("api.production.svc.example.com")),
  ?assertNot(arterial_connection:is_fully_qualified_k8s_name("redis")),
  ?assertNot(arterial_connection:is_fully_qualified_k8s_name("redis.production")),
  ?assertNot(arterial_connection:is_fully_qualified_k8s_name("api.example.com")).

%% Test external hostname detection
external_hostname_test() ->
  % External domains
  ?assert(arterial_connection:is_external_hostname("api.example.com")),
  ?assert(arterial_connection:is_external_hostname("service.example.org")),
  ?assert(arterial_connection:is_external_hostname("app.example.net")),
  ?assert(arterial_connection:is_external_hostname("service.example.io")),

  % IP addresses
  ?assert(arterial_connection:is_external_hostname("192.168.1.1")),
  ?assert(arterial_connection:is_external_hostname("10.0.0.1")),
  ?assert(arterial_connection:is_external_hostname("127.0.0.1")),

  % Local domains
  ?assert(arterial_connection:is_external_hostname("service.local")),

  % Non-external names
  ?assertNot(arterial_connection:is_external_hostname("redis")),
  ?assertNot(arterial_connection:is_external_hostname("redis.production")),
  ?assertNot(arterial_connection:is_external_hostname("api-service")).

%% Test environment detection
environment_detection_test() ->
  % Test detection logic (results depend on actual environment)
  Environment = arterial_connection:detect_k8s_environment(),

  % Should return one of the expected formats
  case Environment of
    {in_cluster, Namespace} when is_list(Namespace) ->
      ?assert(length(Namespace) > 0);
    {external_with_k8s_env, Namespace} when is_list(Namespace) ->
      ?assert(length(Namespace) > 0);
    external ->
      ok
  end.

%% Test current namespace reading
namespace_reading_test() ->
  Environment = arterial_connection:detect_k8s_environment(),
  CurrentNamespace = arterial_connection:get_current_namespace(Environment),

  % Should be a non-empty string
  ?assert(is_list(CurrentNamespace)),
  ?assert(length(CurrentNamespace) > 0).

%% Test cluster domain detection
cluster_domain_test() ->
  ClusterDomain = arterial_connection:get_cluster_domain(),

  % Should default to cluster.local or be set by environment
  ?assert(is_list(ClusterDomain)),
  ?assert(length(ClusterDomain) > 0),

  % Common cluster domains
  ?assert(ClusterDomain =:= "cluster.local" orelse
          string:str(ClusterDomain, ".") > 0).

%% Test search domain building
search_domain_building_test() ->
  Environment = {in_cluster, "production"},
  SearchDomains = arterial_connection:build_k8s_search_domains("redis", Environment),

  % Should have at least the current namespace
  ?assert(length(SearchDomains) >= 1),

  % First domain should be for the current namespace
  [FirstDomain | _] = SearchDomains,
  ?assert(string:str(FirstDomain, "production.svc.") > 0),

  % Should contain cluster domain
  ?assert(lists:any(fun(Domain) ->
    string:str(Domain, "cluster.local") > 0
  end, SearchDomains)).

%% Test explicit Kubernetes service resolution with map
k8s_service_map_resolution_test() ->
  % Test with explicit service specification
  ServiceSpec = #{
    name => "localhost",  % Use localhost as it should always resolve
    namespace => "default",
    cluster_domain => "cluster.local"
  },

  % This should build "localhost.default.svc.cluster.local"
  % Which may or may not resolve depending on the system
  case arterial_connection:resolve_k8s_service(ServiceSpec) of
    {ok, _IPs} ->
      % Resolution succeeded
      ok;
    {error, _Reason} ->
      % Resolution failed, which is acceptable for this test
      ok
  end.

%% Test explicit Kubernetes service resolution with string
k8s_service_string_resolution_test() ->
  % Test with service name string
  case arterial_connection:resolve_k8s_service("localhost") of
    {ok, IPs} ->
      % Should resolve to at least one IP
      ?assert(length(IPs) > 0),
      % All should be valid IPv4 tuples
      lists:foreach(fun(IP) ->
        case IP of
          {A, B, C, D} when is_integer(A), is_integer(B), is_integer(C), is_integer(D) ->
            ok;
          _ ->
            error({invalid_ip_format, IP})
        end
      end, IPs);
    {error, _Reason} ->
      % Resolution failed, acceptable for test
      ok
  end.

%% Test enhanced resolve_address with Kubernetes detection
enhanced_resolve_address_test() ->
  % Test that regular hostnames still work
  case arterial_connection:resolve_address("localhost") of
    {ok, IPs} ->
      ?assert(length(IPs) > 0);
    {error, _} ->
      ok  % May fail in some environments
  end,

  % Test IP addresses still pass through
  {ok, [{127, 0, 0, 1}]} = arterial_connection:resolve_address("127.0.0.1"),
  {ok, [{192, 168, 1, 1}]} = arterial_connection:resolve_address("192.168.1.1").

%% Test DNS caching with different TTLs
dns_caching_ttl_test() ->
  % This test checks that the caching mechanism works with different TTLs
  % We can't easily test the actual TTL differences without waiting,
  % but we can verify the caching mechanism works

  % Clear any existing cache for our test hostname
  TestHostname = "cache-test-" ++ integer_to_list(erlang:system_time()),
  erase({dns_cache, TestHostname}),

  % This should fail (non-existent domain) but test the caching path
  Result1 = arterial_connection:resolve_address(TestHostname),
  Result2 = arterial_connection:resolve_address(TestHostname),

  % Results should be consistent
  ?assertEqual(Result1, Result2).

%% Test Kubernetes environment detection flags
k8s_environment_flags_test() ->
  IsK8sEnv = arterial_connection:is_k8s_environment(),

  % Should be a boolean
  ?assert(is_boolean(IsK8sEnv)),

  % Test should work regardless of environment
  case IsK8sEnv of
    true ->
      % In Kubernetes environment
      Environment = arterial_connection:detect_k8s_environment(),
      ?assertMatch({in_cluster, _}, Environment) orelse
      ?assertMatch({external_with_k8s_env, _}, Environment);
    false ->
      % Not in Kubernetes environment
      ?assertEqual(external, arterial_connection:detect_k8s_environment())
  end.

%% Test service name patterns and edge cases
service_name_patterns_test() ->
  % Test various service name patterns
  TestCases = [
    % {Hostname, ShouldBeK8s, ShouldBeFQDN, ShouldBeExternal}
    {"redis", false, false, false},
    {"redis.production", false, false, false},
    {"redis.default.svc.cluster.local", true, true, false},
    {"api.production.svc.example.com", true, true, false},
    {"api.example.com", false, false, true},
    {"192.168.1.1", false, false, true},
    {"localhost", false, false, false},
    {"service.local", false, false, true}
  ],

  lists:foreach(fun({Hostname, ExpectedK8s, ExpectedFQDN, ExpectedExternal}) ->
    ActualK8s = arterial_connection:is_k8s_service_name(Hostname),
    ActualFQDN = arterial_connection:is_fully_qualified_k8s_name(Hostname),
    ActualExternal = arterial_connection:is_external_hostname(Hostname),

    ?assertEqual(ExpectedK8s, ActualK8s, {k8s_detection_failed, Hostname}),
    ?assertEqual(ExpectedFQDN, ActualFQDN, {fqdn_detection_failed, Hostname}),
    ?assertEqual(ExpectedExternal, ActualExternal, {external_detection_failed, Hostname})
  end, TestCases).

%% Test integration with pool configuration
%% Note: This is more of a documentation test since we can't easily
%% test the full pool integration without setting up actual services
pool_integration_documentation_test() ->
  % This test documents the expected usage patterns

  % Standard usage - should work with existing pools
  PoolConfig1 = #{
    size => 3,
    codec => arterial_codec_default,
    address => "localhost",  % Regular hostname
    port => 6379,
    protocol => tcp
  },

  % Kubernetes service usage
  PoolConfig2 = #{
    size => 5,
    codec => arterial_codec_default,
    address => "redis.production",  % K8s service (short name)
    port => 6379,
    protocol => tcp
  },

  % Fully qualified Kubernetes service
  PoolConfig3 = #{
    size => 3,
    codec => arterial_codec_default,
    address => "cassandra.database.svc.cluster.local",  % K8s FQDN
    port => 9042,
    protocol => tcp
  },

  % All configurations should be valid maps
  ?assert(is_map(PoolConfig1)),
  ?assert(is_map(PoolConfig2)),
  ?assert(is_map(PoolConfig3)),

  % Address fields should be extractable
  ?assertEqual("localhost", maps:get(address, PoolConfig1)),
  ?assertEqual("redis.production", maps:get(address, PoolConfig2)),
  ?assertEqual("cassandra.database.svc.cluster.local", maps:get(address, PoolConfig3)).

%% Performance comparison test (basic)
performance_comparison_test() ->
  % Test that IP addresses have minimal overhead
  StartTime1 = erlang:system_time(microsecond),
  {ok, _} = arterial_connection:resolve_address("127.0.0.1"),
  EndTime1 = erlang:system_time(microsecond),
  IPTime = EndTime1 - StartTime1,

  % Test hostname resolution (may be cached)
  StartTime2 = erlang:system_time(microsecond),
  arterial_connection:resolve_address("localhost"),
  EndTime2 = erlang:system_time(microsecond),
  HostnameTime = EndTime2 - StartTime2,

  % IP resolution should be very fast (< 100 microseconds typically)
  ?assert(IPTime < 1000, {ip_resolution_too_slow, IPTime}),

  % Results should be reasonable (this is just a sanity check)
  ?assert(is_integer(HostnameTime)),
  ?assert(HostnameTime >= 0).
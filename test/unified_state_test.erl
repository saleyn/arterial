-module(unified_state_test).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
Test suite to verify unified state management in arterial_nif.
This tests that the NIF is now the single authoritative source
for connection availability, eliminating race conditions from
the previous dual-state system.
""".

%% Test that availability state is managed entirely by the NIF
unified_availability_test() ->
  application:ensure_all_started(arterial),

  {ok, PoolRef} = arterial_nif:init_pool(1, 1),

  % Initially, slot should not be available (no connection)
  false = arterial_nif:is_slot_available(PoolRef, 0, 0),

  % Mark slot as available via NIF
  ok = arterial_nif:set_slot_available(PoolRef, 0, 0),
  true = arterial_nif:is_slot_available(PoolRef, 0, 0),

  % Mark slot as unavailable via NIF
  ok = arterial_nif:set_slot_unavailable(PoolRef, 0, 0),
  false = arterial_nif:is_slot_available(PoolRef, 0, 0).

%% Test that arterial_pool functions use NIF as authority
pool_nif_integration_test() ->
  application:ensure_all_started(arterial),

  {ok, _SupPid} = arterial_pool:start_link(test_unified_pool, #{
    size => 2,
    codec => arterial_codec_default,
    address => "127.0.0.1",
    port => 9999,  % Non-existent port - connections will fail
    protocol => tcp
  }),

  try
    % Initially connections should be unavailable (not connected)
    false = arterial_pool:is_available(test_unified_pool, 0),
    false = arterial_pool:is_available(test_unified_pool, 1),

    % Manually mark one connection as available via pool interface
    % This should call through to the NIF
    ok = arterial_pool:set_available(test_unified_pool, 0),
    true = arterial_pool:is_available(test_unified_pool, 0),
    false = arterial_pool:is_available(test_unified_pool, 1),

    % Mark it back as unavailable
    ok = arterial_pool:set_unavailable(test_unified_pool, 0),
    false = arterial_pool:is_available(test_unified_pool, 0)
  after
    arterial_pool:stop(test_unified_pool)
  end.

%% Test that state transitions are atomic and consistent
state_consistency_test() ->
  application:ensure_all_started(arterial),

  {ok, PoolRef} = arterial_nif:init_pool(1, 1),

  % Test repeated transitions - should be consistent
  lists:foreach(fun(_) ->
    ok = arterial_nif:set_slot_available(PoolRef, 0, 0),
    true = arterial_nif:is_slot_available(PoolRef, 0, 0),
    ok = arterial_nif:set_slot_unavailable(PoolRef, 0, 0),
    false = arterial_nif:is_slot_available(PoolRef, 0, 0)
  end, lists:seq(1, 100)).
-module(arterial_observability_test).

-moduledoc """
Test that observability events are properly emitted by the arterial library
for NIF operations.
""".

-export([test_observability_integration/0]).

-include_lib("eunit/include/eunit.hrl").

test_observability_integration() ->
  % Set up telemetry
  application:set_env(arterial, observability, telemetry),

  % Attach test handler
  TestPid = self(),
  Handler = fun(EventName, Measurements, Metadata, _Config) ->
    TestPid ! {telemetry_event, EventName, Measurements, Metadata}
  end,

  % Attach to NIF-level events we added and existing high-level events
  telemetry:attach_many(
    test_handler,
    [
      [arterial, nif, init_pool, start],
      [arterial, nif, init_pool, stop],
      [arterial, nif, send, start],
      [arterial, nif, send, stop],
      [arterial, nif, read, start],
      [arterial, nif, read, stop],
      [arterial, nif, write, start],
      [arterial, nif, write, stop],
      [arterial, call, start],
      [arterial, call, stop],
      [arterial, connect, start],
      [arterial, connect, stop],
      [arterial, disconnect]
    ],
    Handler,
    []
  ),

  % Test pool initialization - should generate events
  Opts = #{
    connections => 1,
    addresses => [{127, 0, 0, 1}],
    port => 8080
  },

  try
    % This should emit init_pool events
    {ok, _Pid} = arterial_pool:start_link(test_observability_pool, Opts),

    % Wait for init events
    receive
      {telemetry_event, [arterial, nif, init_pool, start], _M1, Metadata1} ->
        ?assertEqual(test_observability_pool, maps:get(pool, Metadata1)),
        ?assertEqual(1, maps:get(size, Metadata1))
    after 100 ->
      ?assert(false, "No init_pool start event received")
    end,

    receive
      {telemetry_event, [arterial, nif, init_pool, stop], _M2, Metadata2} ->
        ?assertEqual(ok, maps:get(result, Metadata2))
    after 100 ->
      ?assert(false, "No init_pool stop event received")
    end,

    % Test that a failed send would generate events (no server running)
    try
      arterial_client:call(test_observability_pool, <<"test">>, 1000)
    catch _:_ ->
      ok  % Expected to fail
    end,

    % The send attempt should have generated some events even if it failed
    % We might get send events from the retry attempts

    arterial_pool:stop(test_observability_pool),
    ok

  after
    % Clean up
    telemetry:detach(test_handler),
    try arterial_pool:stop(test_observability_pool) catch _:_ -> ok end
  end.
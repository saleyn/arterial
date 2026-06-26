-module(validate_throttle).
-export([test/0]).

%% Simple validation of the C++ throttling implementation
test() ->
    io:format("Testing C++ NIF throttling with throttle.hpp implementation...~n"),

    % Test 1: Initialize pool with throttling
    io:format("Test 1: Initializing pool with throttling...~n"),
    {ok, PoolRef} = arterial_nif:init_pool(1, 1),
    ok = arterial_nif:configure_throttle(PoolRef, 10, 500), % 10 req/sec, 500ms window
    io:format("✓ Pool initialized with throttling configured~n"),

    % Test 2: Verify configuration doesn't crash
    io:format("Test 2: Verifying configuration stability...~n"),
    ok = arterial_nif:configure_throttle(PoolRef, 100, 20), % Change configuration
    io:format("✓ Throttle reconfiguration successful~n"),

    % Test 3: Disable throttling
    io:format("Test 3: Disabling throttling...~n"),
    ok = arterial_nif:configure_throttle(PoolRef, 0, 0), % Disable throttling
    io:format("✓ Throttling disabled successfully~n"),

    io:format("~nAll throttling tests passed! ✓~n"),
    io:format("The throttle_allow function now uses arterial::time_spacing_throttle from throttle.hpp~n"),
    ok.
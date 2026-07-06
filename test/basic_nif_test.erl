-module(basic_nif_test).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
Basic tests to verify NIF functionality without complex server setup.
These tests verify that the hanging issues are resolved.
""".

%% Test that the NIF can be loaded and basic functions work
nif_init_pool_test() ->
    %% This should not hang and should complete quickly
    {ok, Pool} = arterial_nif:init_pool(2, 4),
    ?assertMatch(Pool when is_reference(Pool), Pool).

%% Test that application can start without hanging
application_start_test() ->
    %% This should not hang
    Result = application:ensure_all_started(arterial),
    ?assertMatch({ok, _}, Result),
    application:stop(arterial).

%% Test basic error handling doesn't hang
nif_error_handling_test() ->
    %% These should handle errors quickly, not hang
    ?assertError(badarg, arterial_nif:init_pool(invalid, args)),
    ?assertMatch({error, max_slots_exceeded_64}, arterial_nif:init_pool(100, 100)). % exceeds max_slots_per_stripe

%% Test multiple rapid pool inits don't hang
rapid_pool_init_test() ->
    %% Create and clean up pools rapidly - this should not hang
    Pools = [arterial_nif:init_pool(1, 1) || _ <- lists:seq(1, 10)],
    [?assertMatch({ok, _}, Pool) || Pool <- Pools].
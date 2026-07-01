-module(pool_state_test).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
Test to check pool state and initialization.
""".

%% Test pool state
pool_initialization_test() ->
    ok = test_helper:set_log_level(),
    {ok, _} = application:ensure_all_started(arterial),

    try
        %% Test different pool sizes
        io:format("Testing pool with size 1:~n"),
        {ok, PoolRef1} = arterial_nif:init_pool(1, 1),
        Available1 = arterial_nif:is_slot_available(PoolRef1, 0, 0),
        io:format("Pool size 1, stripe 0, slot 0 available: ~p~n", [Available1]),

        io:format("Testing pool with size 2:~n"),
        {ok, PoolRef2} = arterial_nif:init_pool(2, 1),
        Available2a = arterial_nif:is_slot_available(PoolRef2, 0, 0),
        Available2b = arterial_nif:is_slot_available(PoolRef2, 1, 0),
        io:format("Pool size 2, stripe 0, slot 0 available: ~p~n", [Available2a]),
        io:format("Pool size 2, stripe 1, slot 0 available: ~p~n", [Available2b]),

        %% Try to check if pools are working by using the working socket options test pattern
        io:format("Trying socket options test pattern:~n"),
        Result = arterial_nif:connect_with_opts(PoolRef1, 0, {127,0,0,1}, 9999, % non-existent port
                                              1000, true, self(), [keepalive]),
        io:format("Connect with opts to non-existent port: ~p~n", [Result]),

        %% The result should be some kind of connection error, NOT stripe_full
        case Result of
            {error, stripe_full} ->
                io:format("ERROR: Got stripe_full even on fresh pool!~n"),
                ?assert(false); % This should not happen
            {error, _} ->
                io:format("Good: Got expected connection error instead of stripe_full~n");
            {ok, _, _} ->
                io:format("Note: Connection in progress (port might be reachable)~n")
        end,

        %% Validate that pool state is as expected
        ?assertEqual(false, Available1),   % Initial slot should not be available
        ?assertEqual(false, Available2a),  % Initial slot should not be available
        ?assertEqual(false, Available2b),  % Initial slot should not be available
        ?assert(Result =/= {error, stripe_full}) % Should not get stripe_full on fresh pool

    after
        application:stop(arterial)
    end.
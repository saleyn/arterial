-module(debug_nif_test).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
Debug test to diagnose NIF connection issues.
""".

%% Test to debug NIF connection state
debug_nif_connection_test() ->
    ok = test_helper:set_test_logging(),
    {ok, _} = application:ensure_all_started(arterial),

    %% Start a test server
    {ok, Srv} = test_tcp_server:start(0),
    Port = test_tcp_server:port(Srv),
    io:format("Test server started on port ~p~n", [Port]),

    %% Test manual connection to the server to verify it works
    case gen_tcp:connect("127.0.0.1", Port, [], 1000) of
        {ok, Socket} ->
            io:format("Manual TCP connection successful~n"),
            gen_tcp:close(Socket);
        {error, Reason} ->
            io:format("Manual TCP connection failed: ~p~n", [Reason])
    end,

    %% Start a minimal pool
    {ok, SupPid} = arterial_pool:start_link(debug_pool, #{
        size => 1,
        codec => arterial_codec_default,
        address => "127.0.0.1",
        port => Port,
        protocol => tcp
    }),

    try
        io:format("Pool started with supervisor: ~p~n", [SupPid]),

        %% Wait for connection - should now fail faster if select fails
        case arterial_pool:wait_connected(debug_pool, 1, 10000) of
            ok ->
                io:format("Pool reports connected~n");
            {error, timeout} ->
                io:format("Pool connection timeout - this might be expected now~n")
        end,

        %% Get pool reference and try direct NIF call
        PoolRef = arterial_pool:pool_ref(debug_pool),
        io:format("Pool reference: ~p~n", [PoolRef]),

        %% Try sending directly to the NIF with properly framed message
        ConnID = 0,
        CorrId = rand:uniform(16#FFFFFFFF),  %% Generate a correlation ID
        Request = {echo, test_message},

        %% Frame the message using the same codec as arterial_client:call
        Codec = arterial_codec_default,
        FramedData = iolist_to_binary(Codec:encode_request(CorrId, Request)),

        io:format("Attempting direct NIF send_and_release call with framed data...~n"),
        io:format("CorrId: ~p, Request: ~p~n", [CorrId, Request]),
        Result = arterial_nif:send_and_release(PoolRef, ConnID, [FramedData]),
        io:format("NIF send_and_release result: ~p~n", [Result]),

        %% Try arterial_client:call
        io:format("Attempting arterial_client:call...~n"),
        ClientResult = arterial_client:call(debug_pool, {echo, test_message}, 1000),
        io:format("Client call result: ~p~n", [ClientResult]),

        %% Check connection status
        Children = supervisor:which_children(arterial_pool:sup_name(debug_pool)),
        io:format("Pool children: ~p~n", [Children]),

        %% Check results - both NIF and client calls should work now
        case Result of
            {ok, _SlotId} ->
                io:format("SUCCESS: NIF call worked!~n");
            {error, NifReason} ->
                io:format("UNEXPECTED: NIF call failed with: ~p~n", [NifReason])
        end,

        case ClientResult of
            {ok, Response} ->
                io:format("SUCCESS: Client call worked! Got response: ~p~n", [Response]),
                io:format("Connection issue is FIXED! 🎉~n"),
                ?assert(true);
            {error, no_connection} ->
                io:format("STILL FAILING: Client call failed with no_connection~n"),
                %% Fail the test so we can see the debug output
                ?assertEqual({debug_info, Result, ClientResult, Children}, still_failing_need_investigation);
            {error, OtherReason} ->
                io:format("UNEXPECTED: Client call failed with: ~p~n", [OtherReason]),
                io:format("This suggests there may still be an issue to debug~n"),
                %% Show debug info but don't fail hard - this is now a diagnostic test
                ?assertEqual({debug_info, Result, ClientResult, Children}, debug_analysis_needed)
        end
    after
        arterial_pool:stop(debug_pool),
        test_tcp_server:stop(Srv),
        application:stop(arterial)
    end.
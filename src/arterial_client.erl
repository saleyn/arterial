-module(arterial_client).

-export([call/3]).

-optional_callbacks([handle_timeout/2]).

-callback init(Options::map()) ->
  {ok, State::any()} | {error, Reason::term()}.

-callback setup(Socket::inet:socket(), State::any()) ->
  {ok, State::any()} | {error, Reason::term(), State::any()}.

-callback handle_request(Request::term(), State::any()) ->
  {ok, [RequestID::arterial:request_id()], Data::iodata(), State::any()}.

-callback handle_data(Data::binary(), State::any()) ->
  {ok, [arterial:response()], State::any()} | {error, Reason::term(), State::any()}.

-callback handle_timeout(RequestID::arterial:request_id(), State::any()) ->
  {ok, arterial:response(), State::any()} | {error,  Reason::term(), State::term()}.

-callback terminate(Reason::any(), State::any()) ->
  ok.

-type options() :: #{
  init_options       => arterial_connection:init_options(),
  address            => arterial:inet_address(),
  ip                 => arterial:inet_address(),
  port               => arterial:inet_port(),
  protocol           => arterial:protocol(),
  reconnect          => boolean(),
  reconnect_time_max => arterial:time()   | infinity,
  reconnect_time_min => arterial:time(),
  bounce_interval_ms => non_neg_integer() | infinity,
  socket_options     => arterial:socket_options()
}.

-export_type([options/0]).

%%%-----------------------------------------------------------------------------
%%% Synchronous request API
%%%-----------------------------------------------------------------------------
%% @doc Send `Request' on a connection checked out from `Pool' and block
%% for its reply (or `Timeout' milliseconds, whichever comes first). The
%% calling process owns the socket directly for the duration of the call:
%% it checks out a connection, encodes/sends/receives on the raw socket
%% via the pool's `arterial_protocol' module, then checks the connection
%% back in. `arterial_connection' (the pool's gen_server worker for this
%% connection) is not involved in the call itself -- it only owns
%% reconnect/health monitoring between calls.
-spec call(arterial_pool:name(), term(), non_neg_integer()) ->
  {ok, arterial:response()} | {error, term()}.
call(Pool, Request, Timeout) ->
  case arterial_nif:checkout_connection(Pool, sync) of
    {ok, #{
      conn_id  := ConnID,
      protocol := Proto,
      socket   := Socket,
      buffer   := Buffer,
      req_ids  := [ReqID | _] = ReqIDs
    }} ->
      try
        TS = os:system_time(microsecond),
        case Proto:encode_request(ReqID, Request, Timeout) of
          {ok, Data} ->
            case Proto:send(Socket, Data) of
              ok ->
                Expire = arterial_util:calc_expiration(TS, Timeout * 1000),
                case receive_response(Socket, Proto, ReqID, Buffer, Expire) of
                  {ok, Result, Rest} ->
                    ok = arterial_nif:checkin_connection(Pool, ConnID, ReqIDs, Rest),
                    {ok, Result};
                  {error, _} = Error ->
                    ok = arterial_nif:checkin_connection(Pool, ConnID, ReqIDs, <<>>),
                    Error
                end;
              {error, _} = Error ->
                ok = arterial_nif:checkin_connection(Pool, ConnID, ReqIDs, <<>>),
                Error
            end;
          {error, _} = Error ->
            ok = arterial_nif:checkin_connection(Pool, ConnID, ReqIDs, <<>>),
            Error
        end
      catch E:R:ST ->
        arterial_nif:checkin_connection(Pool, ConnID, ReqIDs, <<>>),
        erlang:raise(E, R, ST)
      end;
    {error, _} = Error ->
      Error
  end.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

-spec receive_response(arterial:socket(), arterial:protocol(),
                        arterial:request_id(), binary(),
                        non_neg_integer() | infinity) ->
  {ok, arterial:response(), binary()} | {error, term()}.
receive_response(Socket, Proto, ReqID, Buffer, Expire) ->
  case Proto:decode_reply(ReqID, Buffer) of
    {ok, Result, Rest} ->
      {ok, Result, Rest};
    {more, Buffer1} ->
      Timeout = arterial_util:calc_timeout(Expire),
      case Proto:recv(Socket, Timeout) of
        {ok, Chunk} ->
          receive_response(Socket, Proto, ReqID, <<Buffer1/binary, Chunk/binary>>, Expire);
        {error, _} = Error ->
          Error
      end;
    {error, _} = Error ->
      Error
  end.
-module(arterial_connection).
-export([call/3, receive_response/5]).

-compile(inline).

-record(cast, {
  pid       :: pid(),
  pool      :: atom(),
  conn_ref  :: reference(),
  req_id    :: non_neg_integer(),
  protocol  :: atom(),
  socket    :: arterial:socket(),
  ts_sent   :: non_neg_integer(),
  ts_expire :: non_neg_integer()
}).

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------

call(Pool, Request, Timeout) ->
  case arterial_nif:checkout_connection(Pool, sync) of
    {ok, #{
      conn_ref := ConnRef,
      conn_id  := ConnID,
      protocol := Proto,
      socket   := Socket,
      buffer   := Buffer
    }} ->
      try
        TS = os:system_time(microsecond),
        maybe
          {ok, ReqIDs, Bin} ?= Proto:encode_requests(ConnRef, ConnID, Request, TS, Timeout),
          ok                ?= Proto:send(Socket, Bin, Timeout),
          Expire             = arterial_utils:calc_expiration(TS, Timeout),
          {ok, Res, Bin2}   ?= receive_response(Pool, Socket, Proto, ReqIDs, Buffer, Expire),

          arterial_nif:checkin_connection(Pool, ConnRef, Bin2),
        else
          {ok, _} = Res2 ->
            Res2
          {error, _} = Error ->
            arterial_nif:checkin_connection(Pool, ConnRef),
            Error
        end
      catch
        E:R:ST ->
          arterial_nif:checkin_connection(Pool, ConnRef),
          erlang:raise(E, R, ST)
      end;
    {error, _} = Error ->
      Error
  end.

cast(Pool, Request, Timeout) ->
  case arterial_nif:acquire_async(Pool) of
    {ok, #{
      req_id   := ReqID,
      protocol := Proto
    }} ->
      try
        TS = os:system_time(microsecond),
        maybe
          {ok, Bin} ?= Proto:encode_request(ReqID, Request, TS, Timeout),
          Expiration = calc_timeout(TS + Timeout),
          ok        ?= arterial_proxy:send(Pool, ReqID, Data, Expiration),
          {ok, #cast{
            pool=Pool, pid=self(), req_id=ReqID, proto=Proto,
            sock=Socket, ts_sent=TS, ts_expire=TS+Timeout*1000
          }
        else
          {error, _} = Error2 ->
            Error2
        end
      catch E:R:ST ->
        arterial_nif:release_async(Pool),
        erlang:raise(E, R, ST)
      end;
    {error, _} = Error ->
      Error
  end.


-spec receive_response(atom(), reference(), atom(), non_neg_integer(),
                       binary(), non_neg_integer() | infinity) ->
        {ok, any(), binary()} | {error, any()}.
receive_response(Pool, Socket, Proto, ReqID, Buffer, Expiration) ->
  Timeout = arterial_utils:calc_timeout(Expiration),
  case Proto:recv(Socket, Timeout) of
    {ok, Chunk} ->
      case Proto:handle_msg(ReqID, join(Buffer, Chunk)) of
        {ok, ReqID, Result, DataChunk} ->
          {ok, Result, DataChunk};
        {more, DataChunk} ->
          receive_response(Pool, Socket, Proto, ReqID, DataChunk, Expiration);
        {error, Reason} ->
          {error, Reason}
      end;
    {error, _Why} = Error ->
      Error
  end.


%%%-----------------------------------------------------------------------------
%%% Private functions
%%%-----------------------------------------------------------------------------

join(<<>>, Head) -> Head;
join(Head, Tail) -> <<Head/binary, Tail/binary>>.
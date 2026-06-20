-module(arterial_protocol).

-callback connect(arterial:inet_address(), arterial:inet_port(),
                  arterial:socket_options()) ->
  {ok, arterial:socket()} | {error, atom()}.

-callback close(arterial:socket()) -> ok.

-callback send(arterial:socket(), iodata()) -> ok | {error, atom()}.

-callback recv(arterial:socket(), Timeout::non_neg_integer()) ->
  {ok, binary()} | {error, any()}.

-callback setopts(arterial:socket(), [gen_tcp:option() | gen_udp:option()]) ->
  ok | {error, atom()}.

%% @doc Encode `Request' (owned by `ReqID') into wire bytes to be sent via
%% send/2. Used by the synchronous arterial_client:call/3 path.
-callback encode_request(ReqID::arterial:request_id(), Request::term(),
                          Timeout::non_neg_integer()) ->
  {ok, iodata()} | {error, term()}.

%% @doc Try to decode the reply to `ReqID' out of the front of `Buffer'
%% (the unconsumed bytes accumulated so far for this connection).
-callback decode_reply(ReqID::arterial:request_id(), Buffer::binary()) ->
  {ok, arterial:response(), Rest::binary()} |
  {more, Buffer::binary()} |
  {error, term()}.

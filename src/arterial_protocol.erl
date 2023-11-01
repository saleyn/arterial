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

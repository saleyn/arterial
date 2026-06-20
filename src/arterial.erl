-module(arterial).

-type client()         :: module().
-type protocol()       :: module().
-type inet_address()   :: inet:ip_address() | inet:hostname().
-type inet_port()      :: inet:port_number().
-type socket()         :: socket:socket().
-type socket_options() :: [{{Level::atom(), Opt::atom()}, Value::term()}].
-type time()           :: non_neg_integer().
-type request_id()     :: non_neg_integer().
-type response()       :: term().

-export_type([
  client/0,
  protocol/0,
  inet_address/0,
  inet_port/0,
  socket/0,
  socket_options/0,
  time/0,
  request_id/0,
  response/0
]).

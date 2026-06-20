-module(arterial).

-moduledoc """
Shared types used across the `arterial` connection-pooling library.

Every other module (`arterial_pool`, `arterial_client`, `arterial_protocol`,
`arterial_connection`, ...) builds its own types out of these primitives, so
this module has no functions of its own -- it only exports types.
""".

-doc "A callback module implementing the `arterial_client` behaviour.".
-type client() :: module().

-doc "A callback module implementing the `arterial_protocol` behaviour.".
-type protocol() :: module().

-doc """
A connection target: either a resolvable hostname (e.g. `"db.internal"`) or
an already-parsed `inet:ip_address()`.

## Examples

```
1> Address1 = "db.internal".
"db.internal"
2> {ok, Address2} = inet:parse_address("127.0.0.1").
{ok,{127,0,0,1}}
```
""".
-type inet_address() :: inet:ip_address() | inet:hostname().

-doc "A TCP/UDP port number, e.g. `5432`.".
-type inet_port() :: inet:port_number().

-doc """
The underlying transport socket, as returned by `arterial_socket:connect/5`
(an OTP `socket` module handle, not a legacy `gen_tcp`/`gen_udp` port).
""".
-type socket() :: socket:socket().

-doc """
Low-level socket options applied right after connecting, expressed as
`{{Level, Opt}, Value}` triples for `socket:setopt/4`.

## Examples

```
1> Opts = [{{socket, reuseaddr}, true}, {{tcp, nodelay}, true}].
[{{socket,reuseaddr},true},{{tcp,nodelay},true}]
```
""".
-type socket_options() :: [{{Level::atom(), Opt::atom()}, Value::term()}].

-doc "A duration in milliseconds, e.g. a reconnect backoff or sweep interval.".
-type time() :: non_neg_integer().

-doc """
A wire-level request identifier, as encoded by
`c:arterial_protocol:encode_request/3` and matched back up by
`c:arterial_protocol:decode_reply/2`.
""".
-type request_id() :: non_neg_integer().

-doc "The decoded value of a reply, as produced by `c:arterial_protocol:decode_reply/2`.".
-type response() :: term().

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

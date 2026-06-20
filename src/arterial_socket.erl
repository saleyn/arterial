-module(arterial_socket).

-moduledoc """
Thin wrapper around OTP's `socket` module: opens a `tcp` or `udp` socket,
applies socket options, and exposes a uniform `connect/5`/`close/1` pair
used by `arterial_connection`.

This is the default low-level transport; an `arterial_protocol`
implementation typically calls into this module (or replaces it) from its
`connect/3` callback.
""".

-export([connect/5, close/1]).

-doc """
Open a `tcp` or `udp` socket to `IP`:`Port`, apply `Opts`
(`arterial:socket_options()`), and return it. For `udp`, this binds a
local socket on `Port` rather than connecting (UDP has no connection
handshake); `IP` is ignored in that case.

## Examples

```
1> arterial_socket:connect(tcp, {127,0,0,1}, 9000, [], 5000).
{ok, Socket}
```
""".
-spec connect(tcp | udp, inet:ip_address(), arterial:inet_port(),
              arterial:socket_options(), timeout()) ->
  {ok, arterial:socket()} | {error, term()}.
connect(tcp, IP, Port, Opts, Timeout) ->
  case socket:open(inet, stream, tcp) of
    {ok, Sock} ->
      try
        case socket:connect(Sock, #{family => inet, addr => IP, port => Port}, Timeout) of
          ok ->
            apply_opts(Sock, Opts),
            {ok, Sock};
          {error, _Reason2} = Error2 ->
            throw(Error2)
        end
      catch throw:Reason ->
        socket:close(Sock),
        {error, Reason}
      end;
    {error, _} = Error ->
      Error
  end;

connect(udp, _IP, Port, Opts, _Timeout) ->
  case socket:open(inet, dgram, udp) of
    {ok, Sock} ->
      try
        case socket:bind(Sock, #{family => inet, addr => {0,0,0,0}, port => Port}) of
          ok ->
            apply_opts(Sock, Opts),
            {ok, Sock};
          {error, _Reason2} = Error2 ->
            throw(Error2)
        end
      catch throw:Reason ->
        socket:close(Sock),
        {error, Reason}
      end;
    {error, _} = Error ->
      Error
  end.

apply_opts(Sock, Opts) ->
  lists:foreach(fun({{Level, Opt}, Value}) ->
    case socket:setopt(Sock, Level, Opt, Value) of
      ok ->
        ok;
      {error, _Reason} = Error ->
        throw({bad_sock_option, {Level, Opt}, Error})
    end
  end, Opts).

-doc """
Close `Sock`, as returned by `connect/5`.

## Examples

```
1> arterial_socket:close(Socket).
ok
```
""".
-spec close(arterial:socket()) -> ok.
close(Sock) ->
  socket:close(Sock).

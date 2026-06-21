-module(arterial_socket).

-moduledoc """
Thin wrapper around OTP's `socket` module: opens a `tcp`, `udp`, or `ssl`
socket, applies socket/TLS options, and exposes a uniform `connect/5,6`/
`close/1,2` pair used by `arterial_connection`.

`ssl` requires **OTP 28 or later**: it connects a plain `tcp` socket via
the `socket` module (same as the `tcp` clause below) and upgrades it
in-place with `ssl:connect/3`, which only gained the ability to wrap an
OTP `socket` module handle directly (instead of a legacy `gen_tcp` port)
in OTP 28's `tls_socket_tcp` module. On OTP 27 or earlier, `connect(ssl,
...)` returns `{error, ssl_requires_otp_28}` rather than silently
misbehaving against an unsupported `socket`/`ssl` combination.

This is the default low-level transport; an `arterial_protocol`
implementation typically calls into this module (or replaces it) from its
`connect/3` callback. For `ssl`, pair it with a protocol module whose
`send/2`/`recv/2` call `ssl:send/2`/`ssl:recv/3` (not `socket:send/recv`)
-- see `m:ssl_echo_protocol` for an example.
""".

-export([connect/5, connect/6, close/1, close/2]).

-doc """
Equivalent to `connect/6` with `TlsOpts = []` -- only meaningful for the
`ssl` transport; ignored for `tcp`/`udp`.

## Examples

```
1> arterial_socket:connect(tcp, {127,0,0,1}, 9000, [], 5000).
{ok, Socket}
```
""".
-spec connect(tcp | udp | ssl, inet:ip_address(), arterial:inet_port(),
              arterial:socket_options(), timeout()) ->
  {ok, arterial:socket()} | {error, term()}.
connect(Proto, IP, Port, Opts, Timeout) ->
  connect(Proto, IP, Port, Opts, Timeout, []).

-doc """
Open a `tcp`, `udp`, or `ssl` socket to `IP`:`Port`, apply `Opts`
(`arterial:socket_options()`) and, for `ssl`, `TlsOpts`
(`arterial:tls_options()`, passed straight through to `ssl:connect/3`),
and return it. For `udp`, this binds a local socket on `Port` rather than
connecting (UDP has no connection handshake); `IP` is ignored in that
case.

## Examples

```
1> arterial_socket:connect(ssl, {127,0,0,1}, 9443,
2>                          [], 5000, [{verify, verify_none}]).
{ok, SslSocket}
```
""".
-spec connect(tcp | udp | ssl, inet:ip_address(), arterial:inet_port(),
              arterial:socket_options(), timeout(), arterial:tls_options()) ->
  {ok, arterial:socket()} | {error, term()}.
connect(tcp, IP, Port, Opts, Timeout, _TlsOpts) ->
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

connect(udp, _IP, Port, Opts, _Timeout, _TlsOpts) ->
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
  end;

connect(ssl, IP, Port, Opts, Timeout, TlsOpts) ->
  case ssl_supported() of
    true ->
      %% Lazily start `ssl` (and its own deps, crypto/public_key/asn1)
      %% on first use, rather than requiring every arterial user to add
      %% `ssl` to their own .app's `applications` list just to use the
      %% tcp/udp transports -- SSL is opt-in per connection, so the
      %% dependency should activate itself only when actually needed.
      application:ensure_all_started(ssl),
      connect_ssl(IP, Port, Opts, Timeout, TlsOpts);
    false ->
      {error, ssl_requires_otp_28}
  end.

%% `tls_socket_tcp` (the `ssl` application module that lets ssl:connect/3
%% wrap an OTP `socket` module handle directly instead of a legacy
%% `gen_tcp` port) was only added in OTP 28 -- detect it at runtime rather
%% than hardcoding a release-number comparison, so this keeps working
%% correctly if OTP backports it to an earlier release later.
ssl_supported() ->
  code:ensure_loaded(tls_socket_tcp) =:= {module, tls_socket_tcp}.

connect_ssl(IP, Port, Opts, Timeout, TlsOpts) ->
  case socket:open(inet, stream, tcp) of
    {ok, Sock} ->
      try
        case socket:connect(Sock, #{family => inet, addr => IP, port => Port}, Timeout) of
          ok ->
            apply_opts(Sock, Opts),
            %% `active` and `mode` are forced regardless of TlsOpts: the
            %% protocol modules using this socket (e.g. ssl_echo_protocol)
            %% do blocking, binary-returning ssl:recv/3 calls, same as the
            %% tcp/udp clauses' blocking, always-binary socket:recv/3 --
            %% both are hard requirements of that design, not defaults to
            %% override (ssl:recv/3 returns `list()` data by default,
            %% which `decode_reply/2`'s binary pattern matching can't
            %% handle).
            ForcedOpts = [{active, false}, {mode, binary}],
            ConnectOpts = ForcedOpts ++
              proplists:delete(mode, proplists:delete(active, TlsOpts)),
            case ssl:connect(Sock, ConnectOpts, Timeout) of
              {ok, SslSock} ->
                {ok, SslSock};
              {error, _Reason2} = Error2 ->
                throw(Error2)
            end;
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
Equivalent to `close/2` with `Proto = tcp` (`tcp`/`udp` close the same
way) -- except for an `ssl:sslsocket()`, detected as a failsafe via
`is_sslsocket/1` and routed to `ssl:close/1` regardless, in case a
caller forgets `close/2`'s explicit `Proto` and calls this directly on
an `ssl` connection's socket. Prefer `close/2` when the transport is
already known (e.g. `arterial_connection` does) -- this fallback exists
for callers that don't have it handy, not as the primary path.
""".
-spec close(arterial:socket()) -> ok.
close(Sock) ->
  case is_sslsocket(Sock) of
    true  -> ssl:close(Sock);
    false -> close(tcp, Sock)
  end.

%% `ssl:sslsocket()` is documented as opaque (`-type sslsocket() ::
%% any().` in ssl.erl) precisely so callers don't rely on its internal
%% shape -- but `close/1` (unlike `close/2`) has no transport hint to go
%% on, so this is a best-effort failsafe, not the primary dispatch
%% mechanism. The `sslsocket` record (ssl_api.hrl) has been a stable
%% 7-field tuple (8 elements including the tag) across the OTP releases
%% this library targets; `test_ssl_server_tests:ssl_socket_shape_test/0`
%% pins this exact shape down against a real connected socket, so if a
%% future OTP release changes it, that test fails loudly in CI instead
%% of this clause silently stopping matching and close/1 falling back to
%% socket:close/1 on a real ssl socket.
is_sslsocket(Sock) ->
  is_tuple(Sock) andalso tuple_size(Sock) =:= 8 andalso element(1, Sock) =:= sslsocket.

-doc """
Close `Sock`, as returned by `connect/5` or `connect/6`. `Proto` must
match whatever transport originally produced `Sock` -- an
`ssl:sslsocket()` needs `ssl:close/1`, not `socket:close/1` (its internal
shape is an opaque, OTP-version-dependent record, not something this
module should pattern-match on to guess the right close function).
Callers that already know their transport (e.g. `arterial_connection`,
which tracks it per-connection) should prefer this over `close/1`.

## Examples

```
1> arterial_socket:close(ssl, SslSocket).
ok
```
""".
-spec close(tcp | udp | ssl, arterial:socket()) -> ok.
close(ssl, Sock) ->
  ssl:close(Sock);
close(_Proto, Sock) ->
  socket:close(Sock).

# Using SSL/TLS with Arterial

This guide covers `arterial`'s `ssl` transport: how it works, its OTP
version requirement, the options it accepts, and a complete runnable
example. It assumes you've read the [client guide](client-guide.md) (or
at least know what `arterial_protocol`/`arterial_client` are) — `ssl`
slots into the same two-behaviour design, it just changes which
low-level socket module your protocol's `send/2`/`recv/2` call.

## Requirements

**`ssl` requires OTP 28 or later.** `arterial_socket:connect/6`'s `ssl`
clause opens a connection via OTP's `socket` module (the same one the
`tcp`/`udp` clauses use) and upgrades it in place with `ssl:connect/3` —
this only became possible in OTP 28, which added `tls_socket_tcp`, the
`ssl` application module that lets `ssl:connect/3` wrap a `socket`
module handle directly instead of requiring a legacy `gen_tcp` port.

On OTP 27 or earlier, `arterial_socket:connect(ssl, ...)` returns
`{error, ssl_requires_otp_28}` immediately rather than attempting (and
failing in a confusing way) to use an API that doesn't exist yet on that
release. Check at runtime with:

```erlang
1> code:ensure_loaded(tls_socket_tcp).
{module,tls_socket_tcp}   % OTP 28+
{error,nofile}            % OTP 27 or earlier
```

You don't need to add `ssl` to your own application's `applications`
list — `arterial_socket` calls `application:ensure_all_started(ssl)`
itself, lazily, the first time a connection actually uses the `ssl`
transport. Apps that only ever use `tcp`/`udp` pay nothing extra.

## Options

Set `protocol => ssl` in `client_opts` (the same key that's `tcp`/`udp`
for the other transports — see `t:arterial_client:options/0`). TLS
options go in a new `tls_options` key, passed straight through to
`ssl:connect/3` with no validation or wrapping — anything accepted by
`m:ssl`'s client options is accepted here:

```erlang
client_opts => #{
  address     => "db.internal",
  port        => 9443,
  protocol    => ssl,
  tls_options => [
    {verify, verify_peer},
    {cacertfile, "/etc/ssl/certs/ca-bundle.pem"}
  ]
}
```

Per-entry overrides work the same way `sock_opts` does for the
`addresses` list (see `t:arterial_connection:address_entry/0`) — useful
if different backup addresses need different certs/SNI:

```erlang
client_opts => #{
  addresses => [
    #{address => "primary.internal", tls_options => [
        {verify, verify_peer}, {cacertfile, "/etc/ssl/primary-ca.pem"}
      ]},
    #{address => "backup.internal", tls_options => [
        {verify, verify_peer}, {cacertfile, "/etc/ssl/backup-ca.pem"}
      ]}
  ],
  port     => 9443,
  protocol => ssl
}
```

Two options are **always forced** regardless of what you pass in
`tls_options`, because every `arterial_protocol` implementation does
blocking, binary-mode I/O (same contract as the `tcp`/`udp` transports):

* `{active, false}` — passive mode; your protocol module calls
  `ssl:recv/3` itself rather than receiving `{ssl, Socket, Data}`
  messages.
* `{mode, binary}` — `ssl:recv/3` returns `binary()`, not `string()`
  (the OTP default), matching what `decode_reply/2`'s framing expects.

Don't pass `active` or `mode` in your own `tls_options` — they're
silently dropped in favor of the forced values above.

## Writing an `arterial_protocol` module for `ssl`

The only thing that changes from a `tcp`-based protocol module is which
socket API `send/2`/`recv/2` call. Compare `test_echo_protocol.erl`
(tcp/udp) with `test/ssl_echo_protocol.erl` (ssl) — same framing, same
`encode_request/3`/`decode_reply/2`, different I/O calls:

```erlang
%% tcp/udp
send(Sock, Data) -> socket:send(Sock, Data).
recv(Sock, Timeout) -> socket:recv(Sock, 0, Timeout).

%% ssl
send(Sock, Data) -> ssl:send(Sock, Data).
recv(Sock, Timeout) -> ssl:recv(Sock, 0, Timeout).
```

`connect/3`/`close/1` stay unused stubs either way — `arterial_connection`
opens (and, for `ssl`, TLS-upgrades) the socket itself via
`arterial_socket:connect/6` before your protocol module ever sees it:

```erlang
connect(_Address, _Port, _Opts) -> {error, not_used}.
close(_Sock) -> ok.
```

Your `arterial_client` callback module needs no changes at all for
`ssl` — `init/1`, `setup/2`, `handle_request/2`, `handle_data/2`,
`terminate/2` only ever see decoded terms/binaries, never the socket
type directly (except `setup/2`'s `Socket` argument, which it typically
just passes through to your protocol module's `send`/`recv`, exactly
like the tcp/udp case). `test/test_echo_client.erl` is reused as-is by
the `ssl` test suite below, with zero modifications.

## Complete example

```erlang
%% 1. Start the pool.
{ok, _SupPid} = arterial_pool:start_link(my_ssl_pool, #{
  size        => 4,
  protocol    => ssl_echo_protocol,   % your arterial_protocol module
  client      => my_client,            % your arterial_client module
  client_opts => #{
    address     => "db.internal",
    port        => 9443,
    protocol    => ssl,
    tls_options => [
      {verify, verify_peer},
      {cacertfile, "/etc/ssl/certs/ca-bundle.pem"}
    ]
  }
}).

%% 2. Call it -- identical to the tcp/udp case, since arterial_client:call/3
%%    only ever talks to your arterial_protocol module's send/2 and recv/2.
{ok, Reply} = arterial_client:call(my_ssl_pool, Request, _TimeoutMs = 5000).
```

## Testing without a real CA-signed certificate

`test/test_ssl_server.erl` (the test fixture backing
`test/test_ssl_server_tests.erl`) generates its own self-signed RSA
certificate at startup, via `public_key:pkix_test_data/1` — no external
cert files or `openssl` CLI calls needed:

```erlang
Key = {rsa, 2048, 65537},
#{server_config := ServerConf} = public_key:pkix_test_data(#{
  server_chain => #{root => [{key, Key}], intermediates => [], peer => [{key, Key}]},
  client_chain => #{root => [{key, Key}], intermediates => [], peer => [{key, Key}]}
}),
{ok, LSock} = ssl:listen(Port, ServerConf ++ [{active, false}, {mode, binary}]).
```

On the client side, a self-signed/IP-addressed test certificate needs
`verify_none` (skip chain validation) and `server_name_indication =>
disable` (the cert has no real DNS name for SNI to check against an IP
address) — that's exactly what the test suite's `setup/1` passes:

```erlang
tls_options => [{verify, verify_none}, {server_name_indication, disable}]
```

**Never use `verify_none` against a real server** — it disables all
certificate validation, which defeats the point of TLS (man-in-the-middle
attacks become trivial). It exists here purely so the test suite can
validate the transport plumbing itself without needing a real,
externally-issued certificate.

Run the suite directly to see it working end-to-end:

```
$ rebar3 eunit --module=test_ssl_server_tests
```

On OTP 27 or earlier, every test in this suite reports `ok` without
running its body (guarded by `otp_28_or_later/0`) — this is intentional,
not a silent failure; it's how the suite avoids breaking CI on OTP 27
while keeping `ssl` coverage real on OTP 28/29.

## Getting a real certificate for production use

`arterial` is only ever the **client** side of a TLS connection — it
never needs a server certificate of its own. What it needs is:

1. **The server's certificate to be verifiable** — either the public
   CA bundle (if the server's cert was issued by a public CA like
   Let's Encrypt) or your organization's internal CA certificate (if
   the server uses a private/internal CA), passed as `cacertfile` in
   `tls_options`.
2. **Optionally, a client certificate** — only if the server requires
   mutual TLS (mTLS), passed as `certfile`/`keyfile` in `tls_options`.

You do **not** generate a certificate "for arterial" — you point
`tls_options` at whatever certificate material already exists for your
deployment. The sections below cover the two common ways that material
gets created.

### Verifying a server with a publicly-trusted certificate (most common case)

If the server you're connecting to has a certificate from a public CA
(Let's Encrypt, DigiCert, etc.), you usually don't need `cacertfile` at
all — `ssl:connect/3` can use the OS's trusted CA store:

```erlang
tls_options => [
  {verify, verify_peer},
  {cacerts, public_key:cacerts_get()}
]
```

`public_key:cacerts_get/0` (OTP 25+) loads the operating system's
trusted root CA bundle, the same set your browser trusts — no file path
to manage, and it stays current as the OS updates its bundle. This is
the right default for connecting to any server with a normal,
publicly-trusted certificate (which is what Let's Encrypt issues).

### Verifying a server with an internal/private CA

If the server's certificate was issued by your own organization's CA
(common for internal services, VPN-only databases, etc.), point
`cacertfile` at that CA's certificate instead:

```erlang
tls_options => [
  {verify, verify_peer},
  {cacertfile, "/etc/ssl/certs/my-internal-ca.pem"}
]
```

If you're standing up that internal CA yourself (e.g. for a private
service mesh), the usual tool is `openssl`:

```sh
# 1. Generate the CA's own key + self-signed root certificate (do this
#    once; keep ca.key secret, distribute ca.pem to every client).
openssl req -x509 -newkey rsa:4096 -keyout ca.key -out ca.pem \
  -days 3650 -nodes -subj "/CN=My Internal CA"

# 2. Generate the *server's* key + a certificate signing request (CSR).
openssl req -newkey rsa:2048 -keyout server.key -out server.csr \
  -nodes -subj "/CN=db.internal"

# 3. Sign the server's CSR with the CA from step 1, producing the
#    certificate the server will actually present to clients.
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key \
  -CAcreateserial -out server.pem -days 825 \
  -extfile <(printf "subjectAltName=DNS:db.internal,IP:10.0.0.5")
```

`server.key`/`server.pem` go on the **server** (arterial never touches
them); `ca.pem` is what every **client** needs for `cacertfile` above.
Note the `subjectAltName` extension in step 3 — modern `ssl:connect/3`
(like browsers) validates the hostname against the certificate's SAN
list, not its `CN`, so the server's actual address must be listed there
or verification fails even with the right CA.

### Getting a publicly-trusted certificate for your own server (Let's Encrypt)

If you're standing up the **server** side yourself and want a
certificate any client can verify without a custom `cacertfile` at all,
use an ACME client against Let's Encrypt (free, automatically renewing,
publicly trusted) — e.g. [certbot](https://certbot.eff.org/):

```sh
sudo certbot certonly --standalone -d db.internal
```

This produces `fullchain.pem`/`privkey.pem` under
`/etc/letsencrypt/live/db.internal/` for the **server** to present.
Clients connecting to it then need no special `cacertfile` at all
(see [Verifying a server with a publicly-trusted certificate](#verifying-a-server-with-a-publicly-trusted-certificate-most-common-case)
above) — that's the main appeal of a public CA over a private one.

### Mutual TLS (mTLS): the server also verifies the client

If the server requires clients to present their own certificate
(common for service-to-service auth), generate a client cert the same
way as the server cert above (steps 2–3, with a different `CN`/key
pair), then add it to `tls_options`:

```erlang
tls_options => [
  {verify, verify_peer},
  {cacertfile, "/etc/ssl/certs/my-internal-ca.pem"},
  {certfile,   "/etc/ssl/certs/client.pem"},
  {keyfile,    "/etc/ssl/private/client.key"}
]
```

The CA that signs the client's certificate doesn't need to be the same
CA that signed the server's — they're verified independently, in
opposite directions.

## Reconnect behavior (read this before relying on automatic recovery)

Like the `tcp`/`udp` transports, `ssl` connections use passive mode
(`{active, false}`). This has a consequence worth understanding: **if
the *server* closes its end of the connection, the client does not find
out until it next calls `recv/2`** — TCP/TLS in passive mode has no
proactive "the peer hung up" notification. A connection sitting idle
(no in-flight request) after the server disappears will be reported as
healthy/available by the pool until something actually tries to use it,
at which point that one call fails with `{error, closed}` and
`arterial_connection`'s normal reconnect-with-backoff machinery takes
over from there.

If you need to deliberately recycle a connection (e.g. behind a load
balancer that's rebalanced, or because you know the server restarted),
use `arterial_connection:bounce/2` (or `arterial_pool`'s
`bounce_interval_ms` option for periodic automatic bouncing) rather than
waiting for a request to fail naturally — see the README's
[Architecture](../README.md#architecture) section and
`arterial_pool:options/0`'s `bounce_interval_ms` doc.

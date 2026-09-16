# Client/Server — First Working Session Layer

This page describes the standalone TCP API. For Generic Host, scoped handlers, and ASP.NET Core HTTP Upgrade on a shared web port, see [hosting.md](hosting.md).

## Usage

```csharp
var factory = new RosenBridgeFactory();

await using var server = factory.CreateServer(new Uri("rb://127.0.0.1:5500"), new()
{
    AllowInsecureLoopback = true,
    AllowAnonymous = true
});

server.MapChannel("/echo", async (channel, ct) =>
{
    channel
        .OnData((data, token) => channel.WriteAsync(data, token))
        .OnEnd(() => channel.CompleteWrites());
    await channel.Completion.WaitAsync(ct);
});
await server.StartAsync(cancellationToken);

await using var client = await factory.ConnectAsync(
    new Uri("rb://127.0.0.1:5500"),
    new() { AllowInsecureLoopback = true }, cancellationToken);

await using var channel = await client.RequestChannelAsync("/echo", cancellationToken);
channel.WritePipe(source);
await channel.ReadPipe(destination);
```

The factory creates objects; the server and client own their respective lifetimes. Register server MapChannel and OnError handlers before startup. StartAsync returns after bind/listen completes; DisposeAsync manages the subsequent lifetime. The startup token is not a permanent server lifetime token.

`RequestChannelAsync` obtains a ticket over the management connection, opens a new data connection, and returns a Channel after binding is acknowledged, without starting Channel I/O. The caller can then register observers. If the server handler writes earlier, payload is not lost; socket backpressure applies until channel reading starts.

The acquisition token/deadline covers channel acquisition only. An acquired channel's lifetime depends on the session and its own DisposeAsync call. Channel tokens are passed to callbacks. Handlers and sources must honor cancellation and must not await their own channel's disposal/completion from within callbacks.

## Runnable sample

```text
dotnet run --project samples/Stella.RosenBridge.SampleHost
```

The sample starts a loopback server/client in one process, sends `hello world` through `/echo`, prints the response, and closes its resources. The OS selects the port. The sample explicitly uses anonymous local development mode.

## TLS and identity

- An `rbs://` server requires a Certificate containing a private key. The caller retains certificate ownership and must keep it available until the server closes.
- By default, the client uses platform certificate/hostname validation. TLS 1.2/1.3 run through the platform. There is no automatic fallback to plaintext TCP.
- `AuthenticateAsync(credential, ct)` returns a ClaimsPrincipal or null to reject authentication. Data connections do not invoke this callback again.
- `AuthorizeChannel(identity, path)` can validate endpoint access for every new connection request.
- An authentication callback is required unless `AllowAnonymous` is explicitly selected. If a callback is configured, a null result does not fall back to anonymous access.
- `rb://` works only when both server and client enable `AllowInsecureLoopback` and the actual IP is a loopback address.
- The client's CertificateValidation override supports custom validation. The test accepts only the generated test certificate's hash; it does not modify the trust store.

This first version passes a credential to the application's validator in the TLS-protected startup message. SASL mechanisms, OIDC token acquisition/validation, mTLS identity, and authentication expiry are not provided yet. The callback does not automatically implement these security features.

Because of [Schannel's ephemeral-key limitation](https://learn.microsoft.com/en-us/dotnet/core/extensions/sslstream-troubleshooting#handshake-failed-with-ephemeral-keys), the Windows TLS test reloads its test certificate with temporary key storage enabled. The certificate is disposed; no trust-store entry is created. Restricted environments may also block access to Windows TLS APIs.

## Reservations and capacity

- Defaults are 8 reserved/active connections per session and at most 64 concurrent acquisition operations per client.
- The server processes at most 256 accepted sockets, including management connections and connections whose role is not yet known. The listen backlog is separate from this limit.
- This version returns `busy` when capacity is exhausted. FIFO waiting, per-endpoint quotas, session fairness, and a global reservation budget are not yet implemented; the global socket limit is enforced at physical acceptance.
- Each grant contains a 256-bit random, single-use ticket bound to the endpoint/session/acquisition ID. Binding consumes it atomically. Ticket comparison uses a fixed-time byte comparison.
- The default ticket lifetime is 10 seconds. Expiry is checked with a monotonic clock during reserve/bind; expired unused entries are cleaned up during those operations. There is no per-ticket timer, and the session limit bounds the number of entries.
- Failed/incorrect/replayed bindings do not cancel another valid reservation. After a successful bind, resources remain reserved until the physical connection closes.
- Acquisition may be cancelled while waiting for a grant or during data connect/TLS/bind. The client sends a management cancel; losing the control connection terminates all reservations.
- An error/timeout during a management write closes the session because framing is no longer reliable. Ordinary endpoint rejection or acquisition cancellation does not close the session.

## Errors and closure

`RosenBridgeException.Code` carries rejection reasons such as `not-found`, `forbidden`, `busy`, `unauthorized`, and `invalid-ticket`. Transport/TLS errors use platform exceptions. Server.OnError reports connection/handler failures; one handler failure does not close other channel endpoints.

Client disposal terminates its management connection, acquisition waiters, and all owned channels. Server disposal stops the listener, cancels sessions, and waits for workers to clean up. This first version does not provide a graceful drain period, heartbeat, automatic reconnect, or server-initiated channel offers. A remote control-connection loss terminates the existing session.

## Experimental management encoding

Each management message consists of a 4-byte big-endian length and a UTF-8 JSON body of at most 8192 bytes. The JSON codec is source-generated; duplicate properties, malformed UTF-8, nested envelopes, and invalid lengths are rejected. This initial envelope contains flat fields.

The first message is `control` or `bind`. Control uses `ready`, `open`, `grant`, `reject`, and `cancel`; data binding uses `bound`/`reject`. After `bound`, the connection switches to [channel framing](channel-api.md). These messages are a v0.1 development format, not a final RB standard or a guarantee of language-independent compatibility. The old multiplex frame codes are not used.

URI support currently requires a root URI with an explicit port; query/userinfo/fragment and an initial channel path are rejected. The client resolves DNS and preserves the selected server IP for data connections. Server binding requires an IP literal or localhost. A channel path is a separate, already-decoded string matched case-sensitively.

## Verification

```text
dotnet run --project tests/Stella.RosenBridge.Transport.SmokeTests
```

Client/server tests cover concurrent endpoint connections, early responses, TLS and rejection of incorrect credentials/certificates, endpoint authorization, capacity release, acquisition cancellation, session cleanup, ticket replay/incorrect binding, and expiry. NativeAOT publishing, other operating systems, and independent language implementations have not yet been verified.

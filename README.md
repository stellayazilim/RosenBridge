# Stella.RosenBridge

A TCP abstraction library for .NET that provides bidirectional streaming between applications.

RosenBridge uses the platform's TCP stack and manages sessions, endpoints, connection lifetimes, and payloads. Every payload flows as a stream. A response can begin while its request is still being sent, allowing both applications to process data as it arrives. No broker is required.

**Status:** Early development. Working client/server, channel, Generic Host, and ASP.NET Core integrations are available; the API and application framing may change.

## Connection model

```text
Client ── management connection ── Server
  │
  ├─ /files endpoint
  │   ├─ Channel A: request ⇄ response
  │   └─ Channel B: request ⇄ response
  │
  └─ /updates endpoint
      └─ Channel C: request ⇄ response
```

- A **session** encompasses the management connection and its operations. User authentication takes place here.
- A **channel endpoint** is a logical address such as `/files`. Multiple connections can target the same endpoint.
- A **Channel object** owns one TCP/TLS connection to an endpoint. Each connection carries one request/response operation.
- `RequestChannelAsync` obtains a single-use ticket over the management connection, opens the data connection, and returns a ready Channel. User authentication is not repeated.
- Request and response finish independently. Closing one connection does not close other connections to the same endpoint.

## Quick start

Requirement: **.NET 10 SDK**. SDK selection is configured in [global.json](global.json).

From the repository root:

```shell
dotnet build Stella.RosenBridge.slnx
dotnet run --project samples/Stella.RosenBridge.SampleHost
```

The sample starts a local server and client in the same process. Expected output:

```text
hello world
```

### Server and client example

This example explicitly enables anonymous development mode on loopback. Port `0` asks the operating system to select an available port.

```csharp
using System.Text;
using Stella.RosenBridge;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
var factory = new RosenBridgeFactory();

await using var server = factory.CreateServer(new Uri("rb://127.0.0.1:0"), new()
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

await server.StartAsync(timeout.Token);

var address = new Uri($"rb://127.0.0.1:{server.LocalEndPoint.Port}");
await using var client = await factory.ConnectAsync(
    address,
    new() { AllowInsecureLoopback = true },
    timeout.Token);

await using var channel =
    await client.RequestChannelAsync("/echo", timeout.Token);

using var source = new MemoryStream(Encoding.UTF8.GetBytes("hello world"));
using var destination = new MemoryStream();

channel.WritePipe(source);
await channel.ReadPipe(destination);

Console.WriteLine(Encoding.UTF8.GetString(destination.ToArray()));
```

The server immediately writes each received chunk back without waiting for the entire request. `WritePipe` starts sending and returns the same Channel. `ReadPipe` copies the response to its destination while sending continues; it returns after both directions and connection cleanup have completed.

The `RequestChannelAsync` token covers connection acquisition. The session and `DisposeAsync` govern the acquired channel's lifetime. Source and destination streams remain caller-owned; the channel does not close them.

## Hosting integrations

- **Standalone app:** `RosenBridgeApp.CreateBuilder(args).Build()` returns a `RosenBridgeApp`; map channels on it before `RunAsync`. Configure its TCP endpoint and services before Build.

- **Generic Host:** `builder.Services.AddRosenBridge(...).UseTcp(uri)` and `host.MapChannel(...)` starts a dedicated TCP listener with the application.
- **ASP.NET Core:** `builder.Services.AddRosenBridge(...)`, `app.MapChannel(...)`, and `app.MapRosenBridge("/rb")` accept HTTP/1.1 Upgrade connections on the existing web port.
- Both integrations create an asynchronous DI scope per channel connection and clean up active connections on host shutdown.
- `factory.ConnectOverHttpAsync(httpsEndpoint, options, ct)` connects through the web entry point; the channel API remains the same.

See the [hosting guide](docs/hosting.md) for complete examples, TLS configuration, scoped handlers, and current limits.

```shell
dotnet run --project samples/Stella.RosenBridge.GenericHost -- --smoke
dotnet run --project samples/Stella.RosenBridge.WebHost -- --smoke
```

## Channel API

| Method / property | Behavior |
|---|---|
| `WritePipe(source)` | Starts sending from a Stream; returns the same Channel. |
| `Send(payload)` | Starts sending byte memory or an async chunk source; returns the same Channel. |
| `ReadPipe(destination)` | Copies the incoming payload to a destination Stream and awaits operation completion. |
| `WriteAsync(data, ct)` | Writes manually, waiting for capacity in the outgoing buffer. |
| `TryWrite(data)` | Returns `false` without accepting any bytes if capacity is unavailable. |
| `CompleteWrites()` | Ends manual sending; the reading direction remains open. |
| `Completion` | Reports the outcome of both payload directions and connection cleanup. |

### Events

| Event | Meaning |
|---|---|
| `OnHeader` | Incoming payload metadata is ready. |
| `OnData` | New bytes have arrived; the asynchronous consumer is awaited. |
| `OnDrain` | Capacity is available again after a manual write was rejected for lack of space. |
| `OnEnd` | The incoming payload ended normally. |
| `OnFinish` | The local payload and END marker have been written. |
| `OnError` | An operation error occurred; Completion also reports the error. |
| `OnClose` | The connection has been cleaned up. |

Register observers before starting transmission. Choose either `OnData` or `ReadPipe` for the incoming payload; two separate consumers cannot be attached. Memory passed to `OnData` is valid until the callback returns. Slow consumers are awaited, and buffers remain bounded. `WritePipe` and `Send` wait for capacity internally, so they do not require `OnDrain`.

`OnFinish` does not mean that the peer application consumed the data or that its business operation succeeded. See the [Channel API](docs/channel-api.md) for details.

## TLS and authentication

- `rbs://` uses the platform's TLS implementation. The server receives a certificate with a private key; the client validates the certificate chain and hostname by default.
- The `AuthenticateAsync` callback establishes the session identity; `AuthorizeChannel` checks endpoint access.
- Data connections bind to the session using short-lived, single-use tickets.
- In this initial version, `rb://` is limited to explicitly enabled loopback development connections.

SASL/OIDC integrations are not yet available. See the [client/server API](docs/client-server-api.md) for configuration and current limitations.

## Verification

```shell
dotnet run --project tests/Stella.RosenBridge.Transport.SmokeTests
```

This executable verification project uses real loopback TCP connections without an additional test package. It is not registered for `dotnet test` discovery.

Coverage includes early responses, concurrent channels, buffering/drain, errors and cancellation, resource ownership, TLS, authentication, endpoint authorization, capacity, ticket replay, expiry, DI scope lifetimes, shared HTTP/RB ports, HTTPS upgrade, and host shutdown. Windows TLS test environment requirements are described [here](docs/client-server-api.md#tls-and-identity).

## Repository layout

```text
src/
├─ Stella.RosenBridge/                  # Client, server, channel, and TCP layers
├─ Stella.RosenBridge.Hosting/          # Common DI and Generic Host integration
└─ Stella.RosenBridge.Hosting.AspNetCore/ # HTTP Upgrade server and client adapter
samples/
├─ Stella.RosenBridge.GenericHost/      # RosenBridgeApp over Generic Host
├─ Stella.RosenBridge.WebHost/          # Shared web port example
└─ Stella.RosenBridge.SampleHost/       # Runnable echo sample
tests/
└─ Stella.RosenBridge.Transport.SmokeTests/
docs/                                  # API notes and design history
```

## Development scope

Sessions and tickets, TLS, authentication/authorization callbacks, bidirectional streaming, and connection limits are implemented. Requests receive `busy` when capacity is exhausted.

FIFO waiting, per-endpoint quotas, heartbeat, graceful drain, reconnect, and server-initiated connection offers are future work. NativeAOT publishing and interoperability across operating systems/languages have not yet been verified. When comparing design goals with implemented behavior, refer to the limitations in the API documents.

## Documentation

All repository documentation is maintained in English.

- [Current design](spec.md)
- [.NET implementation notes](.net-implementation.md)
- [Client/server API](docs/client-server-api.md)
- [Generic Host and ASP.NET Core](docs/hosting.md)
- [GitHub Packages and releases](docs/publishing.md)
- [Channel API and experimental framing](docs/channel-api.md)
- [Raw TCP abstractions](docs/tcp-transport.md)
- [Runnable sample source](samples/Stella.RosenBridge.SampleHost/Program.cs)

## License

Licensed under the [MIT License](LICENSE).

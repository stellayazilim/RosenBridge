# Hosting integrations

RosenBridge supports two application integration points over the same session and channel implementation.

| Assembly | Entry point | Connection acceptance |
|---|---|---|
| `Stella.RosenBridge.Hosting` | `AddRosenBridge().UseTcp(uri)` | Dedicated TCP listener managed by Generic Host |
| `Stella.RosenBridge.Hosting.AspNetCore` | `AddRosenBridge()` and `app.MapRosenBridge("/rb")` | HTTP/1.1 Upgrade through the existing ASP.NET Core listener |

The ASP.NET Core assembly references the common hosting assembly. The common hosting assembly uses Microsoft.Extensions packages and does not require the ASP.NET Core shared framework. The core assembly remains usable without either integration.

## Standalone RosenBridgeApp

`RosenBridgeApp` wraps Generic Host and owns application disposal. Its builder exposes `Services`, `Configuration`, `Logging`, `Environment`, and RB `Options`. Configure services and transport before `Build()`; map channels on the returned application before startup.

```csharp
using Stella.RosenBridge.Hosting;

var builder = RosenBridgeApp.CreateBuilder(args);
builder.Options.Server = new()
{
    AllowAnonymous = true,
    AllowInsecureLoopback = true
};
builder.UseTcp(new Uri("rb://127.0.0.1:7000"));

await using var app = builder.Build(); // RosenBridgeApp
app.MapChannel("/echo", async (channel, ct) =>
{
    channel
        .OnData((data, token) => channel.WriteAsync(data, token))
        .OnEnd(() => channel.CompleteWrites());
    await channel.Completion.WaitAsync(ct);
});

await app.RunAsync();
```

A standalone app requires `UseTcp`, and a builder can produce only one application. `RunAsync` starts the host and waits for shutdown; cancellation requests shutdown and waits for stop processing. The caller disposes the application with `await using`. `StartAsync` and `StopAsync` are also available.

## Existing Generic Host

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Stella.RosenBridge.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddRosenBridge(options => options.Server = new()
    {
        AllowAnonymous = true,
        AllowInsecureLoopback = true
    })
    .UseTcp(new Uri("rb://127.0.0.1:7000"));

using var host = builder.Build();
host.MapChannel("/echo", async (channel, ct) =>
{
    channel
        .OnData((data, token) => channel.WriteAsync(data, token))
        .OnEnd(() => channel.CompleteWrites());

    await channel.Completion.WaitAsync(ct);
});

await host.RunAsync();
```

These options explicitly enable anonymous, cleartext loopback development. For secure TCP, select `rbs://` and set `options.Server.Certificate` through a new `RosenBridgeServerOptions` value, together with `AuthenticateAsync` and optionally `AuthorizeChannel`.

`UseTcp` takes an endpoint URI. Configuration can be read from `builder.Configuration` when constructing that URI and the server options. There is one server registration per service collection; repeated `AddRosenBridge` calls reuse its builder. Service registration and transport configuration belong before Build. Channel mappings belong on the built host, before startup. Duplicate paths and mappings after startup are rejected.

The host-owned server is available as `services.GetRequiredService<RosenBridgeHost>().Server`. Its `LocalEndPoint` is available after TCP startup, including the actual port when port zero was requested.

## ASP.NET Core: share the web port

Use the same service registration and channel handlers, omit `UseTcp`, and map the upgrade endpoint:

```csharp
using Stella.RosenBridge.Hosting;
using Stella.RosenBridge.Hosting.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddRosenBridge(options => options.Server = new()
    {
        AllowAnonymous = true,
        AllowInsecureLoopback = true
    });

await using var app = builder.Build();
app.MapChannel("/echo", async (channel, ct) =>
{
    channel
        .OnData((data, token) => channel.WriteAsync(data, token))
        .OnEnd(() => channel.CompleteWrites());

    await channel.Completion.WaitAsync(ct);
});

app.MapRosenBridge("/rb");
app.MapGet("/health", () => "OK");
await app.RunAsync();
```

`/rb` is the HTTP entry point; `/echo` is a logical channel endpoint requested over the RB management connection. Both normal HTTP routes and RB connections use the web listener's address and port. A single server cannot combine `UseTcp` with `MapRosenBridge`; this is rejected during endpoint mapping.

Every management connection and every ticket-bound data connection starts with an HTTP/1.1 GET request containing `Connection: Upgrade` and `Upgrade: rosenbridge`. A successful `101 Switching Protocols` response establishes a duplex stream. The existing RB management and payload framing then runs on that stream, without WebSocket framing or HTTP request-body framing. Every channel still owns a separate connection.

ASP.NET Core supplies the upgraded stream through [IHttpUpgradeFeature.UpgradeAsync](https://learn.microsoft.com/en-us/dotnet/api/microsoft.aspnetcore.http.features.ihttpupgradefeature.upgradeasync?view=aspnetcore-10.0). The HTTP request delegate remains active for the entire connection lifetime.

### Client

```csharp
using Stella.RosenBridge;
using Stella.RosenBridge.Hosting.AspNetCore;

await using var client = await new RosenBridgeFactory().ConnectOverHttpAsync(
    new Uri("https://localhost:7001/rb"),
    new() { Credential = credential },
    cancellationToken);

await using var channel = await client.RequestChannelAsync("/echo", cancellationToken);
channel.WritePipe(source);
await channel.ReadPipe(destination);
```

The helper uses HTTP/1.1 explicitly and creates a fresh upgraded connection for each acquisition. The existing client owns the management session and channels. Acquisition timeouts cover TCP, TLS, HTTP Upgrade, and RB setup. Redirects, cookies, and system proxy discovery are disabled. Certificates use platform chain/hostname validation unless `CertificateValidation` is explicitly supplied.

### TLS and authentication

- Configure HTTPS and the server certificate in Kestrel. The RB adapter does not perform a second TLS handshake and does not use `RosenBridgeServerOptions.Certificate`.
- The adapter requires an actual server TLS connection feature. Changing the request scheme or forwarding an HTTPS header does not satisfy this check.
- Cleartext HTTP requires `AllowInsecureLoopback` on the server and client. The server checks both local and remote socket addresses; the client checks resolved destination addresses.
- RB `AuthenticateAsync` still establishes the management session identity. Channel connections bind through single-use tickets; opening one does not repeat user authentication.
- `AuthorizeChannel` checks the logical channel path. HTTP endpoint policies, such as `RequireAuthorization()`, are an additional gate on every upgrade; `HttpContext.User` is not automatically imported as the RB session identity. The provided client helper sends RB credentials after upgrade and does not configure HTTP authentication headers.

The included web sample explicitly opts into anonymous loopback development. Production applications must configure their authentication callback or deliberately choose anonymous sessions.

## Dependency scopes and handlers

Each channel connection receives an asynchronous DI scope, separate from its management session and from ASP.NET Core request services. Dependencies remain alive until the handler, channel completion, and channel cleanup have finished. Failed handlers and host shutdown also dispose the scope.

```csharp
builder.Services.AddScoped<FileService>();

builder.Services.AddRosenBridge(options => options.Server = serverOptions);

using var host = builder.Build();
host.MapChannel<FileService>("/files", async (channel, files, ct) =>
{
    await files.HandleAsync(channel, ct);
});

await host.RunAsync();
```

The generic overload resolves its service within the channel scope. Register it explicitly in builder.Services before Build; mapping an unregistered service fails immediately. Existing service registrations keep their configured lifetime. There is also a `host.MapChannel(path, (channel, services, ct) => ...)` overload exposing the scoped `IServiceProvider`.

Handlers may register channel observers and return immediately. Hosting still waits for channel completion before disposing dependencies. Callbacks must honor cancellation and must not wait for their own channel disposal or completion.

Hosted services do not receive an automatic dependency scope; the adapter creates one explicitly. See [scoped services in hosted services](https://learn.microsoft.com/en-us/dotnet/core/extensions/scoped-service).

## Lifetime and capacity

The host starts the RB server and begins shutdown when `ApplicationStopping` fires, before Kestrel waits for long-lived upgraded requests. Shutdown cancels active sessions and channels and waits for workers and asynchronous scopes. This is cancellation-based shutdown, not graceful payload draining. The host's stop token limits how long `StopAsync` waits; it cannot forcibly terminate user callbacks that ignore cancellation. Final disposal still waits for cleanup.

`MaxSockets` bounds admitted RB connections, including management connections, for either integration. HTTP ingress currently upgrades first and closes the connection if the RB limit is exhausted; it does not return a pre-upgrade HTTP capacity response. Configure Kestrel connection/upgrade limits separately to bound connections at the HTTP layer. Per-session capacity and ticket rules remain unchanged.

## Adapter boundary

For other integrations:

- `factory.CreateServer(serverOptions)` creates a server with no listener. Call `StartAsync` to freeze its handlers and then `ProcessConnectionAsync(connection, token)` for each accepted connection.
- The adapter must secure connections or enforce an explicit loopback-only development policy before submitting them. The core does not infer transport security from a stream.
- `ProcessConnectionAsync` takes ownership on entry, including rejection, and finishes after cleanup. Its token covers the whole connection.
- `factory.ConnectUsingAsync(connector, clientOptions, token)` accepts a delegate producing a new, secured `ITransportConnection` for each connection. It does not add TLS itself.
- An externally fed server has no `LocalEndPoint`; the accepting host owns the listening address.

## Current limits

HTTP Upgrade is an experimental adapter using the `rosenbridge` upgrade token; it does not finalize an interoperable wire standard. HTTP/2 and HTTP/3 upgrade/tunneling, WebSockets, browser clients, automatic reconnect, and graceful drain are not implemented.

The integration tests use Kestrel directly. Reverse proxies must support this custom HTTP/1.1 upgrade and keep all management/data connections for a session on the same RB server. Multi-instance affinity and proxy deployment have not been verified. A TLS-terminating proxy must use TLS on its upstream connection, or explicitly opt into the loopback development policy for a loopback upstream.

## Runnable samples

```shell
dotnet run --project samples/Stella.RosenBridge.GenericHost
dotnet run --project samples/Stella.RosenBridge.WebHost
```

The Generic Host sample uses RosenBridgeApp and listens on loopback port 7000; the web sample uses loopback port 5080. Append `-- --smoke` to use a temporary port, run a client exchange, and shut down automatically.

```shell
dotnet run --project samples/Stella.RosenBridge.GenericHost -- --smoke
dotnet run --project samples/Stella.RosenBridge.WebHost -- --smoke
```

The smoke suite verifies application build/mapping validation, RunAsync cancellation, separate scopes, early duplex responses, normal HTTP and RB traffic on the same port, HTTPS certificate validation, session authentication, endpoint authorization, rejection handling, and cleanup of active connections during host shutdown.

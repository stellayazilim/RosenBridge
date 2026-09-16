# Raw TCP Foundation

`Stella.RosenBridge.Transport` contains the connection/listener contracts; `.Transport.Tcp` contains the platform TCP implementation. The factory is stateless and reusable.

```csharp
using System.Net;
using Stella.RosenBridge.Transport;
using Stella.RosenBridge.Transport.Tcp;

ITransportFactory factory = new TcpTransportFactory();
await using var listener = factory.Listen(new IPEndPoint(IPAddress.Loopback, 5500));
await using var connection = await listener.AcceptAsync(cancellationToken);
Stream stream = connection.Stream;
// Transfer raw bytes with stream.ReadAsync / WriteAsync.
```

The client connects through the same factory's `ConnectAsync(IPEndPoint, CancellationToken)` method. The IP address family comes from the endpoint; DNS resolution is outside this initial transport API. Tests use port 0 to request an available port from the OS; the actual address is available through `listener.LocalEndPoint`.

## Lifetime

- Binding/listening is complete when listener creation returns. Backlog is the operating system's pending-connection queue, not an RB session/channel capacity limit.
- Accept/Connect transfers connection ownership to the caller. Disposing the connection or stream closes the socket. Disposing the listener terminates pending accept operations; accepted connections remain open.
- Cancellation applies only to the corresponding accept/connect operation. The returned connection's lifetime is not tied to that token. Read/write operations take their own cancellation tokens.
- One reader and one writer may run concurrently. The upper layer serializes concurrent calls in the same direction. Reads may be partial; write boundaries are not preserved.
- Connection disposal closes both directions. Payload END semantics belong above this raw connection's disposal operation.
- Errors are reported as platform exceptions. Sockets are cleaned up after failed bind/connect operations or connection wrapping.

This foundation provides raw TCP. TLS/auth, channel mapping, tickets, metadata, and END framing belong in higher layers. The raw layer does not enforce an application request/response count.

## Verification

Real loopback smoke tests without extra packages:

```text
dotnet run --project tests/Stella.RosenBridge.Transport.SmokeTests
```

The tests verify byte compatibility with standard TcpClient, receipt of a response before the remaining request bytes are sent, independent connections, listener ownership, and cancellation/closure. This project is an executable verification tool; it is not registered for `dotnet test` discovery.

Platform references: [Socket.AcceptAsync](https://learn.microsoft.com/en-us/dotnet/api/system.net.sockets.socket.acceptasync?view=net-10.0), [NetworkStream](https://learn.microsoft.com/en-us/dotnet/api/system.net.sockets.networkstream?view=net-10.0).

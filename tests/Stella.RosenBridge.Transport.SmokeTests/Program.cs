using System.Net;
using System.Net.Sockets;
using Stella.RosenBridge.Transport;
using Stella.RosenBridge.Transport.Tcp;

if (args.Contains("--di-only"))
{
    await Run("DI client lazy session and lifetime", ClientDependencyInjectionTests.LifetimeAsync);
    await Run("DI client pending shutdown", ClientDependencyInjectionTests.PendingShutdownAsync);
    await Run("DI HTTP Upgrade", HostingSmokeTests.WebHostAsync);
    await Run("DI HTTPS authentication", HostingSmokeTests.WebTlsAsync);
    return;
}

ITransportFactory factory = new TcpTransportFactory();
try
{
    await Run("Raw TCP interoperability and early response", RawInteropAsync);
    await Run("Independent connections and listener ownership", IndependentConnectionsAsync);
    await Run("Accept cancellation and listener reuse", AcceptCancellationAsync);
    await Run("Listener disposal interrupts pending accept", ListenerDisposalAsync);
    await Run("Cancelled connect", CancelledConnectAsync);
    await Run("Channel duplex events and early response", ChannelSmokeTests.EarlyResponseAsync);
    await Run("Channel pipes and source ownership", ChannelSmokeTests.PipesAsync);
    await Run("Channel manual backpressure and drain", ChannelSmokeTests.DrainAsync);
    await Run("Channel malformed frame failure", ChannelSmokeTests.MalformedAsync);
    await Run("Channel consumer failure propagation", ChannelSmokeTests.ConsumerFailureAsync);
    await Run("Channel disposal cancels pending operation", ChannelSmokeTests.DisposalAsync);
    await Run("Manual async write waits for capacity", ChannelSmokeTests.ManualWriteAsync);
    await Run("Manual write cancellation and disposal race", ChannelSmokeTests.CancelManualWriteAsync);
    await Run("Client/server concurrent channels and early response", ClientServerSmokeTests.ConcurrentChannelsAsync);
    await Run("Session authentication and TLS", ClientServerSmokeTests.TlsAsync);
    await Run("Upper-layer admission, async endpoint policy, ticket invalidation and deadline", ClientServerSmokeTests.UpperLayerPolicyAsync);
    await Run("Persistent state per master connection", ClientServerSmokeTests.PersistentSessionAsync);
    await Run("Failed session initialization is isolated", ClientServerSmokeTests.SessionInitializationFailureAsync);
    await Run("Endpoint rejection preserves session", ClientServerSmokeTests.RejectionsAsync);
    await Run("Connection capacity is released", ClientServerSmokeTests.CapacityAsync);
    await Run("Cancelled acquisition preserves session", ClientServerSmokeTests.CancelAcquisitionAsync);
    await Run("Client disposal closes owned channels", ClientServerSmokeTests.SessionDisposalAsync);
    await Run("Ticket replay and invalid bind isolation", ClientServerSmokeTests.TicketReplayAsync);
    await Run("Expired ticket releases reservation", ClientServerSmokeTests.TicketExpiryAsync);
    await Run("DI client lazy shared session, cancellation, retry and shutdown", ClientDependencyInjectionTests.LifetimeAsync);
    await Run("DI client disposal cancels pending connect", ClientDependencyInjectionTests.PendingShutdownAsync);
    await Run("Generic Host scopes and active shutdown", HostingSmokeTests.GenericHostAsync);
    await Run("RosenBridgeApp build, duplex streams, and RunAsync shutdown", HostingSmokeTests.RosenBridgeAppAsync);
    await Run("Application mapping lifecycle validation", HostingSmokeTests.MappingLifecycleAsync);
    await Run("HTTP and RB share a port with duplex streams and scoped handlers", HostingSmokeTests.WebHostAsync);
    await Run("HTTPS upgrade, authentication, and endpoint authorization", HostingSmokeTests.WebTlsAsync);
    await Run("HTTP ingress rejects insecure connections by default", HostingSmokeTests.WebSecurityPolicyAsync);
    await Run("Conflicting hosting ingress is rejected", HostingSmokeTests.ConflictingIngressAsync);
}
catch (Exception error)
{
    Console.Error.WriteLine($"FAIL: {error}");
    Environment.ExitCode = 1;
}

static async Task Run(string name, Func<CancellationToken, Task> test)
{
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    await test(timeout.Token).WaitAsync(timeout.Token);
    Console.WriteLine($"PASS: {name}");
}

async Task RawInteropAsync(CancellationToken token)
{
    await using var listener = factory.Listen(new IPEndPoint(IPAddress.Loopback, 0));
    using var client = new TcpClient(AddressFamily.InterNetwork);
    await client.ConnectAsync((IPEndPoint)listener.LocalEndPoint, token);
    await using var server = await listener.AcceptAsync(token);
    var peer = client.GetStream();
    byte[] first = [0, 255, 1, 82, 66];
    await peer.WriteAsync(first, token);
    await ExpectBytes(server.Stream, first, token);

    // Respond before the peer sends the remaining request bytes.
    byte[] response = [9, 0, 255];
    await server.Stream.WriteAsync(response, token);
    await ExpectBytes(peer, response, token);
    byte[] rest = Enumerable.Range(0, 131072).Select(i => (byte)i).ToArray();
    await Task.WhenAll(peer.WriteAsync(rest, token).AsTask(), ExpectBytes(server.Stream, rest, token));
    if (!server.RemoteEndPoint.Equals(client.Client.LocalEndPoint))
        throw new Exception("Incorrect remote endpoint.");
}

async Task IndependentConnectionsAsync(CancellationToken token)
{
    await using var listener = factory.Listen(new IPEndPoint(IPAddress.Loopback, 0));
    var endpoint = (IPEndPoint)listener.LocalEndPoint;
    await using var clientA = await factory.ConnectAsync(endpoint, token);
    await using var serverA = await listener.AcceptAsync(token);
    await using var clientB = await factory.ConnectAsync(endpoint, token);
    await using var serverB = await listener.AcceptAsync(token);
    await clientA.DisposeAsync();
    if (await serverA.Stream.ReadAsync(new byte[1], token) != 0)
        throw new Exception("Expected EOF after peer disposal.");

    await listener.DisposeAsync();
    byte[] bytes = [23, 42];
    await clientB.Stream.WriteAsync(bytes, token);
    await ExpectBytes(serverB.Stream, bytes, token);
    await serverB.Stream.WriteAsync(bytes, token);
    await ExpectBytes(clientB.Stream, bytes, token);
}

async Task AcceptCancellationAsync(CancellationToken token)
{
    await using var listener = factory.Listen(new IPEndPoint(IPAddress.Loopback, 0));
    using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
    var pending = listener.AcceptAsync(cancellation.Token).AsTask();
    cancellation.Cancel();
    try { await pending; throw new Exception("Accept was not cancelled."); }
    catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
    await using var client = await factory.ConnectAsync((IPEndPoint)listener.LocalEndPoint, token);
    await using var server = await listener.AcceptAsync(token);
    await client.Stream.WriteAsync(new byte[] { 7 }, token);
    await ExpectBytes(server.Stream, [7], token);
}

async Task ListenerDisposalAsync(CancellationToken token)
{
    await using var listener = factory.Listen(new IPEndPoint(IPAddress.Loopback, 0));
    var pending = listener.AcceptAsync(token).AsTask();
    await listener.DisposeAsync();
    try { await pending; throw new Exception("Disposed listener accepted a connection."); }
    catch (ObjectDisposedException) { }
    catch (SocketException) { }
}

async Task CancelledConnectAsync(CancellationToken token)
{
    await using var listener = factory.Listen(new IPEndPoint(IPAddress.Loopback, 0));
    using var cancellation = new CancellationTokenSource();
    cancellation.Cancel();
    try
    {
        await using var unexpected = await factory.ConnectAsync((IPEndPoint)listener.LocalEndPoint, cancellation.Token);
        throw new Exception("Connect was not cancelled.");
    }
    catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
    token.ThrowIfCancellationRequested();
}

static async Task ExpectBytes(Stream stream, byte[] expected, CancellationToken token)
{
    var received = new byte[expected.Length];
    await stream.ReadExactlyAsync(received, token);
    if (!received.AsSpan().SequenceEqual(expected))
        throw new Exception("Raw payload differs.");
}

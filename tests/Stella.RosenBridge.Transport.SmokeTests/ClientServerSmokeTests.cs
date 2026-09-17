using System.Buffers.Binary;
using System.Net;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Stella.RosenBridge;
using Stella.RosenBridge.Channels;
using Stella.RosenBridge.Transport;
using Stella.RosenBridge.Transport.Tcp;

internal static class ClientServerSmokeTests
{
    internal static async Task PersistentSessionAsync(CancellationToken token)
    {
        int authentications = 0, initializations = 0;
        var sessions = new System.Collections.Concurrent.ConcurrentDictionary<string, RosenBridgeSession>();
        var originalUser = new ClaimsPrincipal(new ClaimsIdentity(new[] { new Claim(ClaimTypes.Name, "alice") }, "test"));
        var factory = new RosenBridgeFactory();
        await using var server = factory.CreateServer(new Uri("rb://127.0.0.1:0"), new()
        {
            AllowInsecureLoopback = true,
            AcceptSessionAsync = (session, credential, _) =>
            {
                Interlocked.Increment(ref authentications);
                if (credential != "secret") return ValueTask.FromResult(false);
                session.Items["user"] = originalUser;
                Interlocked.Increment(ref initializations);
                session.Items["requests"] = 0;
                sessions.TryAdd(session.Id, session);
                return ValueTask.FromResult(true);
            },
            AuthorizeChannelAsync = (session, _, _) => ValueTask.FromResult(((ClaimsPrincipal)session.Items["user"]!).Identity?.Name == "alice")
        }).MapChannel("/echo", async (channel, ct) =>
        {
            var session = channel.Session ?? throw new Exception("Session context missing.");
            Check(ReferenceEquals(session, sessions[session.Id]), "Channel received a copy of session state.");
            Check(ReferenceEquals(session.Items["user"], originalUser), "Upper-layer context was changed by transport.");
            session.Items.AddOrUpdate("requests", 1, (_, count) => (int)count! + 1);
            await EchoAsync(channel, ct);
        });
        await server.StartAsync(token);
        var options = new RosenBridgeClientOptions { AllowInsecureLoopback = true, Credential = "secret" };
        await using var first = await factory.ConnectAsync(Address(server), options, token);
        await using var second = await factory.ConnectAsync(Address(server), options, token);
        await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => RoundTripAsync(first, token)));
        await RoundTripAsync(second, token);
        Check(authentications == 2 && initializations == 2, "Authentication or initialization repeated per transfer.");
        var firstState = sessions[first.SessionId];
        var secondState = sessions[second.SessionId];
        Check(!ReferenceEquals(firstState, secondState), "Different master connections shared state.");
        Check((int)firstState.Items["requests"]! == 4 && (int)secondState.Items["requests"]! == 1,
            "Session counters crossed connection boundaries.");
        await first.DisposeAsync();
        while (!firstState.Closed.IsCancellationRequested) await Task.Delay(5, token);
        Check(!secondState.Closed.IsCancellationRequested, "Closing one session closed another.");
        await RoundTripAsync(second, token);
        await using var active = await second.RequestChannelAsync("/echo", token);
        active.OnData((_, _) => ValueTask.CompletedTask);
        secondState.Close();
        secondState.Close();
        Check(secondState.Closed.IsCancellationRequested, "Upper-layer close did not close session state.");
        try { await active.Completion.WaitAsync(token); throw new Exception("Upper-layer close left channel active."); }
        catch (IOException) { }
        catch (OperationCanceledException) when (!token.IsCancellationRequested) { }
    }

    internal static async Task SessionInitializationFailureAsync(CancellationToken token)
    {
        int attempts = 0;
        RosenBridgeSession? failedSession = null;
        var factory = new RosenBridgeFactory();
        await using var server = Server(factory, LocalServer with
        {
            AcceptSessionAsync = (session, _, _) =>
            {
                if (Interlocked.Increment(ref attempts) == 1)
                {
                    failedSession = session;
                    throw new InvalidOperationException("Expected initialization failure.");
                }
                return ValueTask.FromResult(true);
            }
        });
        await server.StartAsync(token);
        try
        {
            await using var unexpected = await factory.ConnectAsync(Address(server), LocalClient, token);
            throw new Exception("Failed session initialization returned a ready client.");
        }
        catch (IOException) { }
        Check(failedSession?.Closed.IsCancellationRequested == true, "Failed initialization leaked a live session.");
        await using var client = await factory.ConnectAsync(Address(server), LocalClient, token);
        await RoundTripAsync(client, token);
    }

    internal static async Task UpperLayerPolicyAsync(CancellationToken token)
    {
        RosenBridgeSession? accepted = null;
        var calls = 0;
        var factory = new RosenBridgeFactory();
        await using var server = Server(factory, LocalServer with
        {
            AcceptSessionAsync = async (session, credential, ct) =>
            {
                await Task.Delay(5, ct);
                accepted = session;
                return credential == "opaque-application-value";
            },
            AuthorizeChannelAsync = async (_, path, ct) =>
            {
                await Task.Delay(5, ct);
                Interlocked.Increment(ref calls);
                return path == "/echo";
            }
        });
        await server.StartAsync(token);
        var tcp = new TcpTransportFactory();
        await using var control = await tcp.ConnectAsync(server.LocalEndPoint, token);
        var ready = await ExchangeAsync(control, new { type = "control", credential = "opaque-application-value" }, token);
        var sessionId = ready.GetProperty("session").GetString();
        var grant = await ExchangeAsync(control, new { type = "open", id = 1, path = "/echo" }, token);
        var ticket = grant.GetProperty("ticket").GetString();
        Check(calls == 1, "Endpoint policy not awaited before ticket grant.");
        accepted!.Close();
        await using var data = await tcp.ConnectAsync(server.LocalEndPoint, token);
        var denied = await ExchangeAsync(data, new { type = "bind", id = 1, session = sessionId, ticket }, token);
        Check(denied.GetProperty("code").GetString() == "invalid-ticket", "Closed session ticket accepted.");

        // Even a non-cooperative policy cannot hold a handshake open indefinitely.
        await using var timeoutServer = Server(factory, LocalServer with
        {
            HandshakeTimeout = TimeSpan.FromMilliseconds(100),
            AcceptSessionAsync = (_, _, _) => new ValueTask<bool>(new TaskCompletionSource<bool>().Task)
        });
        await timeoutServer.StartAsync(token);
        try
        {
            await using var unexpected = await factory.ConnectAsync(Address(timeoutServer), LocalClient, token);
            throw new Exception("Uncompleted acceptance callback returned a ready client.");
        }
        catch (IOException) { }
    }
    private static RosenBridgeClientOptions LocalClient => new() { AllowInsecureLoopback = true };
    private static RosenBridgeServerOptions LocalServer => new() { AllowInsecureLoopback = true };

    private static RosenBridgeServer Server(RosenBridgeFactory factory, RosenBridgeServerOptions? options = null)
        => factory.CreateServer(new Uri("rb://127.0.0.1:0"), options ?? LocalServer).MapChannel("/echo", EchoAsync);

    private static Uri Address(RosenBridgeServer server, string scheme = "rb")
        => new($"{scheme}://127.0.0.1:{server.LocalEndPoint.Port}");

    private static async Task EchoAsync(Channel channel, CancellationToken token)
    {
        channel.OnData((data, ct) => channel.WriteAsync(data, ct))
            .OnEnd(() => channel.CompleteWrites());
        await channel.Completion.WaitAsync(token);
    }

    internal static async Task ConcurrentChannelsAsync(CancellationToken token)
    {
        var factory = new RosenBridgeFactory();
        await using var server = Server(factory);
        await server.StartAsync(token);
        await using var client = await factory.ConnectAsync(Address(server), LocalClient, token);
        var channels = await Task.WhenAll(Enumerable.Range(0, 4)
            .Select(_ => client.RequestChannelAsync("/echo", token)));
        try
        {
            await Task.WhenAll(channels.Select(async channel =>
            {
                var firstResponse = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                using var output = new MemoryStream();
                channel.OnData(async (bytes, ct) =>
                {
                    await output.WriteAsync(bytes, ct);
                    firstResponse.TrySetResult();
                }).Send(SourceAsync(firstResponse.Task, token));
                await channel.Completion.WaitAsync(token);
                Check(output.ToArray().AsSpan().SequenceEqual(new byte[] { 1, 2 }), "Duplex echo failed.");
            }));
        }
        finally { foreach (var channel in channels) await channel.DisposeAsync(); }
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> SourceAsync(Task response,
        [EnumeratorCancellation] CancellationToken token)
    {
        yield return new byte[] { 1 };
        await response.WaitAsync(token);
        yield return new byte[] { 2 };
    }

    internal static async Task TlsAsync(CancellationToken token)
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256);
        var names = new SubjectAlternativeNameBuilder();
        names.AddDnsName("localhost");
        names.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(names.Build());
        using var generated = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddDays(1));
        // Schannel needs a named key. DefaultKeySet permits temporary key storage;
        // disposing the imported certificate removes it. No trust-store entry is added.
        using var certificate = X509CertificateLoader.LoadPkcs12(generated.Export(X509ContentType.Pkcs12), null);
        int authentications = 0;
        var factory = new RosenBridgeFactory();
        await using var server = factory.CreateServer(new Uri("rbs://127.0.0.1:0"), new()
        {
            Certificate = certificate,
            AcceptSessionAsync = (session, credential, _) =>
            {
                Interlocked.Increment(ref authentications);
                return ValueTask.FromResult(credential == "test-credential");
            }
        }).MapChannel("/echo", EchoAsync);
        await server.StartAsync(token);
        // Trust only this generated test certificate; production defaults use platform validation.
        var options = new RosenBridgeClientOptions
        {
            Credential = "test-credential",
            CertificateValidation = (_, peer, _, _) => peer?.GetCertHashString() == certificate.GetCertHashString()
        };
        await using var client = await factory.ConnectAsync(Address(server, "rbs"), options, token);
        for (int i = 0; i < 2; i++) await RoundTripAsync(client, token);
        Check(authentications == 1, "Data connections must not repeat user authentication.");

        try
        {
            await using var bad = await factory.ConnectAsync(Address(server, "rbs"), new()
            {
                Credential = "wrong",
                CertificateValidation = options.CertificateValidation
            }, token);
            throw new Exception("Invalid credentials were accepted.");
        }
        catch (RosenBridgeException error) when (error.Code == "unauthorized") { }

        try
        {
            await using var untrusted = await factory.ConnectAsync(Address(server, "rbs"), new()
                { Credential = "test-credential" }, token);
            throw new Exception("Untrusted certificate was accepted.");
        }
        catch (AuthenticationException) { }
    }

    internal static async Task RejectionsAsync(CancellationToken token)
    {
        var factory = new RosenBridgeFactory();
        await using var server = Server(factory, LocalServer with { AuthorizeChannelAsync = (_, path, _) => ValueTask.FromResult(path != "/denied") })
            .MapChannel("/denied", EchoAsync);
        await server.StartAsync(token);
        await using var client = await factory.ConnectAsync(Address(server), LocalClient, token);
        await ExpectCodeAsync(client.RequestChannelAsync("/missing", token), "not-found");
        await ExpectCodeAsync(client.RequestChannelAsync("/denied", token), "forbidden");
        await RoundTripAsync(client, token);
    }

    internal static async Task CapacityAsync(CancellationToken token)
    {
        var factory = new RosenBridgeFactory();
        await using var server = Server(factory, LocalServer with { MaxConnectionsPerSession = 1 });
        await server.StartAsync(token);
        await using var client = await factory.ConnectAsync(Address(server), LocalClient, token);
        await using var first = await client.RequestChannelAsync("/echo", token);
        await ExpectCodeAsync(client.RequestChannelAsync("/echo", token), "busy");
        await first.DisposeAsync();
        // Peer cleanup is asynchronous; retry only the documented capacity rejection.
        while (true)
        {
            token.ThrowIfCancellationRequested();
            try { await RoundTripAsync(client, token); break; }
            catch (RosenBridgeException error) when (error.Code == "busy") { await Task.Delay(10, token); }
        }
    }

    internal static async Task CancelAcquisitionAsync(CancellationToken token)
    {
        var transport = new PausingTransport();
        var factory = new RosenBridgeFactory(transport);
        await using var server = Server(factory, LocalServer with { MaxConnectionsPerSession = 1 });
        await server.StartAsync(token);
        await using var client = await factory.ConnectAsync(Address(server), LocalClient, token);
        transport.PauseNext = true;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        var pending = client.RequestChannelAsync("/echo", cancellation.Token);
        await transport.Paused.Task.WaitAsync(token);
        cancellation.Cancel();
        try { await using var unexpected = await pending; throw new Exception("Expected acquisition cancellation."); }
        catch (OperationCanceledException) { }
        await RoundTripAsync(client, token);
    }

    internal static async Task SessionDisposalAsync(CancellationToken token)
    {
        var factory = new RosenBridgeFactory();
        await using var server = Server(factory);
        await server.StartAsync(token);
        await using var client = await factory.ConnectAsync(Address(server), LocalClient, token);
        await using var first = await client.RequestChannelAsync("/echo", token);
        await using var second = await client.RequestChannelAsync("/echo", token);
        await client.DisposeAsync();
        foreach (var channel in new[] { first, second })
        {
            try { await channel.Completion.WaitAsync(token); throw new Exception("Session-owned channel survived disposal."); }
            catch (OperationCanceledException) { }
        }
    }

    internal static async Task TicketReplayAsync(CancellationToken token)
    {
        var factory = new RosenBridgeFactory();
        await using var server = Server(factory);
        await server.StartAsync(token);
        var tcp = new TcpTransportFactory();
        await using var control = await tcp.ConnectAsync(server.LocalEndPoint, token);
        var ready = await ExchangeAsync(control, new { type = "control" }, token);
        var session = ready.GetProperty("session").GetString();
        var grant = await ExchangeAsync(control, new { type = "open", id = 1, path = "/echo" }, token);
        var ticket = grant.GetProperty("ticket").GetString();
        await using var first = await tcp.ConnectAsync(server.LocalEndPoint, token);
        var accepted = await ExchangeAsync(first, new { type = "bind", id = 1, session, ticket }, token);
        Check(accepted.GetProperty("type").GetString() == "bound", "Initial ticket bind failed.");
        await using var replay = await tcp.ConnectAsync(server.LocalEndPoint, token);
        var rejected = await ExchangeAsync(replay, new { type = "bind", id = 1, session, ticket }, token);
        Check(rejected.GetProperty("code").GetString() == "invalid-ticket", "Ticket replay was accepted.");

        grant = await ExchangeAsync(control, new { type = "open", id = 2, path = "/echo" }, token);
        ticket = grant.GetProperty("ticket").GetString();
        await using var wrong = await tcp.ConnectAsync(server.LocalEndPoint, token);
        rejected = await ExchangeAsync(wrong, new { type = "bind", id = 2, session, ticket = "incorrect" }, token);
        Check(rejected.GetProperty("code").GetString() == "invalid-ticket", "Wrong ticket was accepted.");
        await using var correct = await tcp.ConnectAsync(server.LocalEndPoint, token);
        accepted = await ExchangeAsync(correct, new { type = "bind", id = 2, session, ticket }, token);
        Check(accepted.GetProperty("type").GetString() == "bound", "Invalid bind destroyed a valid reservation.");
    }

    internal static async Task TicketExpiryAsync(CancellationToken token)
    {
        var factory = new RosenBridgeFactory();
        await using var server = Server(factory, LocalServer with
            { TicketLifetime = TimeSpan.FromMilliseconds(50), MaxConnectionsPerSession = 1 });
        await server.StartAsync(token);
        var tcp = new TcpTransportFactory();
        await using var control = await tcp.ConnectAsync(server.LocalEndPoint, token);
        var ready = await ExchangeAsync(control, new { type = "control" }, token);
        var session = ready.GetProperty("session").GetString();
        var grant = await ExchangeAsync(control, new { type = "open", id = 1, path = "/echo" }, token);
        var ticket = grant.GetProperty("ticket").GetString();
        await Task.Delay(100, token);
        await using var expired = await tcp.ConnectAsync(server.LocalEndPoint, token);
        var rejected = await ExchangeAsync(expired, new { type = "bind", id = 1, session, ticket }, token);
        Check(rejected.GetProperty("code").GetString() == "invalid-ticket", "Expired ticket was accepted.");
        grant = await ExchangeAsync(control, new { type = "open", id = 2, path = "/echo" }, token);
        Check(grant.GetProperty("type").GetString() == "grant", "Expired reservation retained capacity.");
    }

    private static async Task<JsonElement> ExchangeAsync<T>(ITransportConnection connection, T message, CancellationToken token)
    {
        // Independently encode the documented envelope rather than using the library codec.
        var bytes = JsonSerializer.SerializeToUtf8Bytes(message);
        var prefix = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(prefix, bytes.Length);
        await connection.Stream.WriteAsync(prefix, token);
        await connection.Stream.WriteAsync(bytes, token);
        await connection.Stream.ReadExactlyAsync(prefix, token);
        var count = BinaryPrimitives.ReadInt32BigEndian(prefix);
        Check(count is > 0 and <= 8192, "Invalid response envelope.");
        bytes = new byte[count];
        await connection.Stream.ReadExactlyAsync(bytes, token);
        using var document = JsonDocument.Parse(bytes);
        return document.RootElement.Clone();
    }

    private static async Task RoundTripAsync(RosenBridgeClient client, CancellationToken token)
    {
        await using var channel = await client.RequestChannelAsync("/echo", token);
        using var output = new MemoryStream();
        channel.Send(new byte[] { 5, 0, 255 });
        await channel.ReadPipe(output).WaitAsync(token);
        Check(output.ToArray().AsSpan().SequenceEqual(new byte[] { 5, 0, 255 }), "Request/response payload differs.");
    }

    private static async Task ExpectCodeAsync(Task<Channel> operation, string code)
    {
        try { await using var unexpected = await operation; }
        catch (RosenBridgeException error) when (error.Code == code) { return; }
        throw new Exception($"Expected rejection: {code}.");
    }

    private static void Check(bool condition, string message)
    {
        if (!condition) throw new Exception(message);
    }

    private sealed class PausingTransport : ITransportFactory
    {
        private readonly TcpTransportFactory _inner = new();
        internal bool PauseNext { get; set; }
        internal TaskCompletionSource Paused { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ITransportListener Listen(IPEndPoint endpoint, int backlog = 128) => _inner.Listen(endpoint, backlog);
        public async ValueTask<ITransportConnection> ConnectAsync(IPEndPoint endpoint, CancellationToken cancellationToken = default)
        {
            if (PauseNext)
            {
                PauseNext = false;
                Paused.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            return await _inner.ConnectAsync(endpoint, cancellationToken);
        }
    }
}

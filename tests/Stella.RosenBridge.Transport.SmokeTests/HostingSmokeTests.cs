using System.Collections.Concurrent;
using System.Net;
using System.Runtime.CompilerServices;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Stella.RosenBridge;
using Stella.RosenBridge.Channels;
using Stella.RosenBridge.Hosting;
using Stella.RosenBridge.Hosting.AspNetCore;

internal static class HostingSmokeTests
{
    private static RosenBridgeServerOptions LocalServer => new() { AllowAnonymous = true, AllowInsecureLoopback = true };
    private static RosenBridgeClientOptions LocalClient => new() { AllowInsecureLoopback = true };

    internal static async Task RosenBridgeAppAsync(CancellationToken token)
    {
        var builder = RosenBridgeApp.CreateBuilder([]);
        builder.Logging.ClearProviders();
        builder.Options.Server = LocalServer;
        builder.UseTcp(new("rb://127.0.0.1:0"));
        builder.Services.AddSingleton<ScopeTracker>();
        builder.Services.AddScoped<EchoService>();
        await using var app = builder.Build();
        app.MapChannel<EchoService>("/echo", (channel, service, ct) => service.Handle(channel, ct));
        try { builder.Build(); throw new Exception("Builder allowed a second application."); }
        catch (InvalidOperationException error) when (error.Message.Contains("already been built")) { }

        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = app.Services.GetRequiredService<IHostApplicationLifetime>()
            .ApplicationStarted.Register(() => started.TrySetResult());
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(token);
        var run = app.RunAsync(stop.Token);
        try
        {
            await started.Task.WaitAsync(token);
            var server = app.Services.GetRequiredService<RosenBridgeHost>().Server;
            await using var client = await new RosenBridgeFactory().ConnectAsync(
                new Uri($"rb://127.0.0.1:{server.LocalEndPoint.Port}"), LocalClient, token);
            await EarlyEcho(client, token);
            await using var active = await client.RequestChannelAsync("/echo", token);
            active.OnData((_, _) => ValueTask.CompletedTask);
            var tracker = app.Services.GetRequiredService<ScopeTracker>();
            while (tracker.Created.Count < 2) await Task.Delay(5, token);
            await stop.CancelAsync();
            await run.WaitAsync(token);
            await ExpectClosed(active, token);
            Check(tracker.Disposed.Count == 2, "App shutdown did not dispose its scopes.");
        }
        finally { await stop.CancelAsync(); await run.WaitAsync(token); }
    }

    internal static async Task MappingLifecycleAsync(CancellationToken token)
    {
        var builder = RosenBridgeApp.CreateBuilder([]);
        builder.Logging.ClearProviders();
        builder.Options.Server = LocalServer;
        try { builder.Build(); throw new Exception("App accepted a missing listener."); }
        catch (InvalidOperationException error) when (error.Message.Contains("UseTcp")) { }
        builder.UseTcp(new("rb://127.0.0.1:0"));
        await using var app = builder.Build();
        try
        {
            app.MapChannel<EchoService>("/missing-service", (channel, service, ct) => service.Handle(channel, ct));
            throw new Exception("Unregistered handler service accepted.");
        }
        catch (InvalidOperationException error) when (error.Message.Contains("Register")) { }
        app.MapChannel("/echo", Echo);
        try { app.MapChannel("/echo", Echo); throw new Exception("Duplicate mapping accepted."); }
        catch (ArgumentException) { }
        await app.StartAsync(token);
        try { app.MapChannel("/late", Echo); throw new Exception("Mapping after startup accepted."); }
        catch (InvalidOperationException error) when (error.Message.Contains("already started")) { }
        await app.StopAsync(token);
    }

    internal static async Task GenericHostAsync(CancellationToken token)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton<ScopeTracker>();
        builder.Services.AddScoped<EchoService>();
        builder.Services.AddRosenBridge(o => o.Server = LocalServer)
            .UseTcp(new("rb://127.0.0.1:0"));
        using var host = builder.Build();
        host.MapChannel<EchoService>("/echo", (channel, service, ct) => service.Handle(channel, ct));
        await host.StartAsync(token);
        var server = host.Services.GetRequiredService<RosenBridgeHost>().Server;
        var tracker = host.Services.GetRequiredService<ScopeTracker>();
        await using var client = await new RosenBridgeFactory().ConnectAsync(
            new($"rb://127.0.0.1:{server.LocalEndPoint.Port}"), LocalClient, token);
        await Task.WhenAll(EarlyEcho(client, token), EarlyEcho(client, token));
        await WaitForScopes(tracker, 2, token);
        Check(tracker.Created.Count == 2, "Each request must resolve a separate scoped service.");

        await using var pending = await client.RequestChannelAsync("/echo", token);
        pending.OnData((_, _) => ValueTask.CompletedTask);
        while (tracker.Created.Count < 3) await Task.Delay(5, token);
        await host.StopAsync(token);
        await ExpectClosed(pending, token);
        Check(tracker.Disposed.Count == 3, "Host shutdown must await scope disposal.");
    }

    internal static async Task WebHostAsync(CancellationToken token)
    {
        var builder = WebBuilder();
        builder.Services.AddSingleton<ScopeTracker>();
        builder.Services.AddScoped<EchoService>();
        builder.Services.AddRosenBridge(o => o.Server = LocalServer);
        await using var app = builder.Build();
        app.MapChannel<EchoService>("/echo", (channel, service, ct) => service.Handle(channel, ct))
            .MapChannel<EchoService>("/failure", (_, _, _) => throw new InvalidOperationException("Expected handler failure."));
        app.MapRosenBridge("/rb");
        app.MapGet("/health", () => "OK");
        await app.StartAsync(token);
        var endpoint = new Uri(app.Urls.Single() + "/rb");
        using var http = new HttpClient();
        Check(await http.GetStringAsync(new Uri(endpoint, "/health"), token) == "OK", "HTTP endpoint failed.");
        using (var response = await http.GetAsync(endpoint, token))
            Check((int)response.StatusCode == 426, "Non-upgrade request must be rejected.");

        await using var client = await new RosenBridgeFactory().ConnectOverHttpAsync(endpoint, LocalClient, token);
        await Task.WhenAll(EarlyEcho(client, token), EarlyEcho(client, token),
            http.GetStringAsync(new Uri(endpoint, "/health"), token));
        var tracker = app.Services.GetRequiredService<ScopeTracker>();
        await WaitForScopes(tracker, 2, token);
        try
        {
            await using var missing = await client.RequestChannelAsync("/missing", token);
            throw new Exception("Missing channel accepted.");
        }
        catch (RosenBridgeException error) when (error.Code == "not-found") { }

        await using (var failure = await client.RequestChannelAsync("/failure", token))
        {
            failure.OnData((_, _) => ValueTask.CompletedTask).CompleteWrites();
            await ExpectClosed(failure, token);
        }
        await EarlyEcho(client, token);
        await WaitForScopes(tracker, 4, token);
        await using var active = await client.RequestChannelAsync("/echo", token);
        active.OnData((_, _) => ValueTask.CompletedTask);
        while (tracker.Created.Count < 5) await Task.Delay(5, token);
        await app.StopAsync(token);
        await ExpectClosed(active, token);
        Check(tracker.Disposed.Count == 5, "Web shutdown left a channel scope alive.");
    }

    internal static async Task WebTlsAsync(CancellationToken token)
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256);
        var names = new SubjectAlternativeNameBuilder();
        names.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(names.Build());
        using var generated = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddDays(1));
        using var certificate = X509CertificateLoader.LoadPkcs12(generated.Export(X509ContentType.Pkcs12), null);
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(o => o.Listen(IPAddress.Loopback, 0, listen =>
        {
            listen.Protocols = HttpProtocols.Http1;
            listen.UseHttps(certificate);
        }));
        int authentications = 0;
        builder.Services.AddRosenBridge(o => o.Server = new()
        {
            AuthenticateAsync = (credential, _) =>
            {
                Interlocked.Increment(ref authentications);
                return ValueTask.FromResult<ClaimsPrincipal?>(credential == "secret"
                    ? new ClaimsPrincipal(new ClaimsIdentity("test")) : null);
            },
            AuthorizeChannel = (_, path) => path != "/denied"
        });
        await using var app = builder.Build();
        app.MapChannel("/echo", Echo).MapChannel("/denied", Echo);
        app.MapRosenBridge();
        await app.StartAsync(token);
        var endpoint = new Uri(app.Urls.Single() + "/rb");
        var options = new RosenBridgeClientOptions
        {
            Credential = "secret",
            CertificateValidation = (_, peer, _, _) => peer?.GetCertHashString() == certificate.GetCertHashString()
        };
        var factory = new RosenBridgeFactory();
        await using var client = await factory.ConnectOverHttpAsync(endpoint, options, token);
        await Task.WhenAll(EarlyEcho(client, token), EarlyEcho(client, token));
        Check(authentications == 1, "Channel upgrade repeated session authentication.");
        try
        {
            await using var denied = await client.RequestChannelAsync("/denied", token);
            throw new Exception("Authorization was bypassed.");
        }
        catch (RosenBridgeException error) when (error.Code == "forbidden") { }
        await EarlyEcho(client, token);
        try
        {
            await using var invalid = await factory.ConnectOverHttpAsync(endpoint,
                new() { Credential = "wrong", CertificateValidation = options.CertificateValidation }, token);
            throw new Exception("Invalid credentials accepted.");
        }
        catch (RosenBridgeException error) when (error.Code == "unauthorized") { }
        try
        {
            await using var untrusted = await factory.ConnectOverHttpAsync(endpoint,
                new() { Credential = "secret" }, token);
            throw new Exception("Untrusted certificate accepted.");
        }
        catch (HttpRequestException) { }
        await app.StopAsync(token);
    }

    internal static async Task WebSecurityPolicyAsync(CancellationToken token)
    {
        var builder = WebBuilder();
        builder.Services.AddRosenBridge(o => o.Server = new() { AllowAnonymous = true });
        await using var app = builder.Build();
        app.MapChannel("/echo", Echo);
        app.MapRosenBridge();
        await app.StartAsync(token);
        using var http = new HttpClient();
        using var request = new HttpRequestMessage(HttpMethod.Get, app.Urls.Single() + "/rb");
        request.Headers.Connection.Add("Upgrade");
        request.Headers.Upgrade.ParseAdd("rosenbridge");
        request.Headers.Add("X-Forwarded-Proto", "https");
        using var response = await http.SendAsync(request, token);
        Check(response.StatusCode == HttpStatusCode.Forbidden, "Cleartext endpoint must require explicit development opt-in.");
        await app.StopAsync(token);
    }

    internal static Task ConflictingIngressAsync(CancellationToken token)
    {
        var builder = WebBuilder();
        builder.Services.AddRosenBridge(o => o.Server = LocalServer).UseTcp(new("rb://127.0.0.1:0"));
        var app = builder.Build();
        try
        {
            try { app.MapRosenBridge(); throw new Exception("Conflicting ingress accepted."); }
            catch (InvalidOperationException error) when (error.Message.Contains("UseTcp")) { }
        }
        finally { app.DisposeAsync().AsTask().GetAwaiter().GetResult(); }
        token.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    private static WebApplicationBuilder WebBuilder()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(o => o.Listen(IPAddress.Loopback, 0));
        return builder;
    }

    private static Task Echo(Channel channel, CancellationToken token)
    {
        channel.OnData((bytes, ct) => channel.WriteAsync(bytes, ct)).OnEnd(() => channel.CompleteWrites());
        // Returning early must not dispose scoped dependencies while callbacks are still active.
        return Task.CompletedTask;
    }

    private static async Task EarlyEcho(RosenBridgeClient client, CancellationToken token)
    {
        await using var channel = await client.RequestChannelAsync("/echo", token);
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var received = new MemoryStream();
        channel.OnData(async (bytes, ct) =>
        {
            await received.WriteAsync(bytes, ct);
            first.TrySetResult();
        }).Send(Chunks(first.Task, token));
        await channel.Completion.WaitAsync(token);
        Check(received.ToArray().AsSpan().SequenceEqual(new byte[] { 1, 2, 3, 4 }), "Upgraded duplex stream corrupted payload.");
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> Chunks(Task first,
        [EnumeratorCancellation] CancellationToken token)
    {
        yield return new byte[] { 1, 2 };
        await first.WaitAsync(token);
        yield return new byte[] { 3, 4 };
    }

    private static async Task ExpectClosed(Channel channel, CancellationToken token)
    {
        try { await channel.Completion.WaitAsync(token); throw new Exception("Expected channel failure."); }
        catch (IOException) { }
        catch (OperationCanceledException) when (!token.IsCancellationRequested) { }
    }

    private static async Task WaitForScopes(ScopeTracker tracker, int count, CancellationToken token)
    {
        while (tracker.Disposed.Count < count) await Task.Delay(5, token);
    }

    private static void Check(bool condition, string message)
    {
        if (!condition) throw new Exception(message);
    }

    public sealed class ScopeTracker
    {
        public ConcurrentDictionary<Guid, byte> Created { get; } = new();
        public ConcurrentDictionary<Guid, byte> Disposed { get; } = new();
    }

    public sealed class EchoService : IAsyncDisposable
    {
        private readonly ScopeTracker _tracker;
        private readonly Guid _id = Guid.NewGuid();
        private bool _disposed;
        public EchoService(ScopeTracker tracker) { _tracker = tracker; tracker.Created.TryAdd(_id, 0); }
        public Task Handle(Channel channel, CancellationToken token)
        {
            channel.OnData((bytes, ct) =>
            {
                Check(!_disposed, "Scope disposed before data callback.");
                return channel.WriteAsync(bytes, ct);
            }).OnEnd(() => channel.CompleteWrites());
            return Task.CompletedTask;
        }
        public async ValueTask DisposeAsync()
        {
            await Task.Yield();
            _disposed = true;
            Check(_tracker.Disposed.TryAdd(_id, 0), "Scope disposed more than once.");
        }
    }
}

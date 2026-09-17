using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Stella.RosenBridge;
using Stella.RosenBridge.Hosting;

internal static class ClientDependencyInjectionTests
{
    internal static async Task LifetimeAsync(CancellationToken token)
    {
        await using var server = new RosenBridgeFactory().CreateServer(new Uri("rb://127.0.0.1:0"),
            new() { AllowInsecureLoopback = true });
        server.MapChannel("/echo", async (channel, ct) =>
        {
            channel.OnData((data, t) => channel.WriteAsync(data, t)).OnEnd(() => channel.CompleteWrites());
            await channel.Completion.WaitAsync(ct);
        });
        await server.StartAsync(token);
        var endpoint = new Uri($"rb://127.0.0.1:{server.LocalEndPoint.Port}");
        var connected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var connections = 0;
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddRosenBridgeClient(o => { o.Endpoint = endpoint; o.AllowInsecureLoopback = true; },
            async (factory, address, options, ct) =>
            {
                Interlocked.Increment(ref connections);
                connected.TrySetResult();
                await release.Task.WaitAsync(ct);
                return await factory.ConnectAsync(address, options, ct);
            });
        builder.Services.AddTransient<ClientConsumer>();
        using var host = builder.Build();
        await host.StartAsync(token);
        var client = host.Services.GetRequiredService<ClientConsumer>().Client;
        using (var scope = host.Services.CreateScope())
            Check(ReferenceEquals(client, scope.ServiceProvider.GetRequiredService<IRosenBridgeClient>()), "Client is not singleton");
        Check(connections == 0, "Resolving client opened a connection");
        using var cancelledCaller = CancellationTokenSource.CreateLinkedTokenSource(token);
        var cancelled = client.RequestChannelAsync("/echo", cancelledCaller.Token);
        await connected.Task.WaitAsync(token);
        var first = client.RequestChannelAsync("/echo", token);
        var second = client.RequestChannelAsync("/echo", token);
        cancelledCaller.Cancel();
        try { await cancelled; throw new Exception("Cancelled caller did not cancel"); }
        catch (OperationCanceledException) when (cancelledCaller.IsCancellationRequested) { }
        release.TrySetResult();
        await using var a = await first;
        await using var b = await second;
        Check(connections == 1, "Concurrent requests opened multiple sessions");
        using var payload = new MemoryStream();
        a.Send(new byte[] { 1, 2, 3 });
        await a.ReadPipe(payload).WaitAsync(token);
        Check(payload.ToArray().SequenceEqual(new byte[] { 1, 2, 3 }), "DI client echo failed");
        b.OnData((_, _) => ValueTask.CompletedTask);
        await host.StopAsync(token);
        try { await b.Completion.WaitAsync(token); throw new Exception("Host shutdown left channel alive"); }
        catch (IOException) { }
        catch (OperationCanceledException) when (!token.IsCancellationRequested) { }
        try { await client.RequestChannelAsync("/echo", token); throw new Exception("Disposed client accepted a request"); }
        catch (ObjectDisposedException) { }
        await client.DisposeAsync(); // Multiple DI aliases and host disposal must be safe.

        var attempts = 0;
        var services = new ServiceCollection();
        services.AddRosenBridgeClient(o => { o.Endpoint = endpoint; o.AllowInsecureLoopback = true; },
            (factory, address, options, ct) => ++attempts == 1
                ? Task.FromException<RosenBridgeClient>(new IOException("Expected connection failure"))
                : factory.ConnectAsync(address, options, ct));
        await using var provider = services.BuildServiceProvider();
        var retry = provider.GetRequiredService<IRosenBridgeClient>();
        try { await retry.RequestChannelAsync("/echo", token); throw new Exception("Initial failure hidden"); }
        catch (IOException error) when (error.Message == "Expected connection failure") { }
        await using var recovered = await retry.RequestChannelAsync("/echo", token);
        Check(attempts == 2, "Initial connection was not retried");
    }

    internal static async Task PendingShutdownAsync(CancellationToken token)
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var services = new ServiceCollection();
        services.AddRosenBridgeClient(o => o.Endpoint = new("rbs://localhost:7001"), async (_, _, _, ct) =>
        {
            started.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            throw new Exception("Unreachable");
        });
        await using var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<IRosenBridgeClient>();
        var pending = client.RequestChannelAsync("/echo", token);
        await started.Task.WaitAsync(token);
        await client.DisposeAsync().AsTask().WaitAsync(token);
        try { await pending; throw new Exception("Pending connect survived disposal"); }
        catch (OperationCanceledException) when (!token.IsCancellationRequested) { }
    }

    private sealed class ClientConsumer(IRosenBridgeClient client)
    {
        public IRosenBridgeClient Client { get; } = client;
    }
    private static void Check(bool condition, string message)
    {
        if (!condition) throw new Exception(message);
    }
}

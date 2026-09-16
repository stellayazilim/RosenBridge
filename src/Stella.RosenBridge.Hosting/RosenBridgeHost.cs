using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Stella.RosenBridge.Channels;

namespace Stella.RosenBridge.Hosting;

/// <summary>Owns the server and one asynchronous dependency scope per channel connection.</summary>
public sealed class RosenBridgeHost : IHostedService, IDisposable, IAsyncDisposable
{
    private readonly IHostApplicationLifetime _lifetime;
    private readonly IServiceScopeFactory _scopes;
    private CancellationTokenRegistration _stopping;

    public RosenBridgeServer Server { get; }
    public bool UsesTcp { get; }
    public bool AllowInsecureLoopback { get; }

    public RosenBridgeHost(RosenBridgeBuilder builder, RosenBridgeFactory factory,
        IServiceScopeFactory scopes, IHostApplicationLifetime lifetime, ILogger<RosenBridgeHost> logger)
    {
        builder.Freeze();
        _lifetime = lifetime;
        _scopes = scopes;
        UsesTcp = builder.TcpEndpoint is not null;
        var options = builder.Options.Server;
        AllowInsecureLoopback = options.AllowInsecureLoopback;
        Server = UsesTcp ? factory.CreateServer(builder.TcpEndpoint!, options) : factory.CreateServer(options);
        Server.OnError(error => logger.LogError(error, "RosenBridge connection failed."));
    }

    internal void MapChannel(string path, Func<Channel, IServiceProvider, CancellationToken, Task> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        Server.MapChannel(path, async (channel, token) =>
        {
            await using var scope = _scopes.CreateAsyncScope();
            try
            {
                await handler(channel, scope.ServiceProvider, token).ConfigureAwait(false);
                await channel.Completion.ConfigureAwait(false);
            }
            finally
            {
                // Callbacks must stop before their scoped dependencies are disposed.
                await channel.DisposeAsync().ConfigureAwait(false);
            }
        });
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await Server.StartAsync(cancellationToken).ConfigureAwait(false);
        _stopping = _lifetime.ApplicationStopping.Register(() => { _ = Server.DisposeAsync(); });
    }

    public Task StopAsync(CancellationToken cancellationToken)
        => Server.DisposeAsync().AsTask().WaitAsync(cancellationToken);

    public async ValueTask DisposeAsync()
    {
        _stopping.Dispose();
        await Server.DisposeAsync().ConfigureAwait(false);
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
}

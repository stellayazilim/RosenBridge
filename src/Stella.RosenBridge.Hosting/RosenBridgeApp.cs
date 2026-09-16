using Microsoft.Extensions.Hosting;

namespace Stella.RosenBridge.Hosting;

/// <summary>A standalone RB application. Map channels after Build and before StartAsync or RunAsync.</summary>
public sealed class RosenBridgeApp : IHost, IAsyncDisposable
{
    private readonly IHost _host;
    private readonly object _gate = new();
    private Task? _dispose;

    internal RosenBridgeApp(IHost host) => _host = host;

    public static RosenBridgeAppBuilder CreateBuilder(string[]? args = null) => new(args);
    public IServiceProvider Services => _host.Services;
    public Task StartAsync(CancellationToken cancellationToken = default) => _host.StartAsync(cancellationToken);
    public Task StopAsync(CancellationToken cancellationToken = default) => _host.StopAsync(cancellationToken);

    /// <summary>Runs until cancellation or application shutdown. The caller owns application disposal.</summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        await StartAsync(cancellationToken).ConfigureAwait(false);
        await _host.WaitForShutdownAsync(cancellationToken).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync()
    {
        lock (_gate) { return new(_dispose ??= DisposeCoreAsync()); }
    }

    private async Task DisposeCoreAsync()
    {
        if (_host is IAsyncDisposable asyncHost) await asyncHost.DisposeAsync().ConfigureAwait(false);
        else _host.Dispose();
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
}

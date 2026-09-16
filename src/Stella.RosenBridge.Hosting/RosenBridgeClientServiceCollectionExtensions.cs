using System.Net.Security;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Stella.RosenBridge.Channels;

namespace Stella.RosenBridge.Hosting;

public sealed class RosenBridgeClientRegistrationOptions
{
    public Uri Endpoint { get; set; } = new("rbs://localhost:7001");
    public string? Credential { get; set; }
    public bool AllowInsecureLoopback { get; set; }
    public TimeSpan OpenTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxPendingRequests { get; set; } = 64;
    public RemoteCertificateValidationCallback? CertificateValidation { get; set; }
}

public static class RosenBridgeClientServiceCollectionExtensions
{
    /// <summary>Registers one lazily connected singleton client, owned by the host/container.</summary>
    public static IServiceCollection AddRosenBridgeClient(this IServiceCollection services,
        Action<RosenBridgeClientRegistrationOptions> configure)
        => services.AddRosenBridgeClient(configure, static (factory, endpoint, options, ct) =>
            factory.ConnectAsync(endpoint, options, ct));

    /// <summary>Registers a client using an explicit transport adapter, such as HTTP Upgrade.</summary>
    public static IServiceCollection AddRosenBridgeClient(this IServiceCollection services,
        Action<RosenBridgeClientRegistrationOptions> configure,
        Func<RosenBridgeFactory, Uri, RosenBridgeClientOptions, CancellationToken, Task<RosenBridgeClient>> connector)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        ArgumentNullException.ThrowIfNull(connector);
        if (services.Any(d => d.ServiceType == typeof(IRosenBridgeClient)))
            throw new InvalidOperationException("A RosenBridge client is already registered.");
        var registration = new RosenBridgeClientRegistrationOptions();
        configure(registration);
        var endpoint = registration.Endpoint;
        if (endpoint is null || !endpoint.IsAbsoluteUri) throw new ArgumentException("An absolute endpoint is required.");
        if (registration.OpenTimeout <= TimeSpan.Zero || registration.OpenTimeout.TotalMilliseconds > int.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(registration.OpenTimeout));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(registration.MaxPendingRequests);
        var options = new RosenBridgeClientOptions
        {
            Credential = registration.Credential,
            AllowInsecureLoopback = registration.AllowInsecureLoopback,
            OpenTimeout = registration.OpenTimeout,
            MaxPendingRequests = registration.MaxPendingRequests,
            CertificateValidation = registration.CertificateValidation
        };
        services.TryAddSingleton<RosenBridgeFactory>();
        services.AddSingleton<ManagedRosenBridgeClient>(sp => new(ct => connector(
            sp.GetRequiredService<RosenBridgeFactory>(), endpoint, options, ct)));
        services.AddSingleton<IRosenBridgeClient>(sp => sp.GetRequiredService<ManagedRosenBridgeClient>());
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<ManagedRosenBridgeClient>());
        return services;
    }
}

internal sealed class ManagedRosenBridgeClient(Func<CancellationToken, Task<RosenBridgeClient>> connect)
    : IRosenBridgeClient, IHostedService, IDisposable
{
    private readonly object _gate = new();
    private readonly CancellationTokenSource _lifetime = new();
    private Task<RosenBridgeClient>? _connection;
    private Task? _dispose;
    private bool _closed;

    public async Task<Channel> RequestChannelAsync(string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Task<RosenBridgeClient> pending;
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_closed, this);
            // A failed initial connection is retried on the next operation. An established
            // session is never silently replaced or an application operation replayed.
            if (_connection is null || _connection.IsFaulted || _connection.IsCanceled)
                _connection = ConnectAsync();
            pending = _connection;
        }
        // Cancelling one caller must not cancel a connection shared by other callers.
        var client = await pending.WaitAsync(cancellationToken).ConfigureAwait(false);
        lock (_gate) ObjectDisposedException.ThrowIf(_closed, this);
        return await client.RequestChannelAsync(path, cancellationToken).ConfigureAwait(false);
    }

    private async Task<RosenBridgeClient> ConnectAsync() => await connect(_lifetime.Token).ConfigureAwait(false);
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StopAsync(CancellationToken cancellationToken) => DisposeAsync().AsTask().WaitAsync(cancellationToken);

    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            _closed = true;
            return new(_dispose ??= DisposeCoreAsync());
        }
    }

    private async Task DisposeCoreAsync()
    {
        try
        {
            await _lifetime.CancelAsync().ConfigureAwait(false);
            if (_connection is { } pending)
            {
                RosenBridgeClient client;
                try { client = await pending.ConfigureAwait(false); }
                catch { return; } // Connection failure remains observable by its callers.
                await client.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally { _lifetime.Dispose(); }
    }

    // Generic Host also supports synchronous container disposal.
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
}

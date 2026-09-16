using Microsoft.Extensions.DependencyInjection;

namespace Stella.RosenBridge.Hosting;

/// <summary>Configures services and transport before the application is built.</summary>
public sealed class RosenBridgeBuilder
{
    internal RosenBridgeBuilder(IServiceCollection services) => Services = services;
    public IServiceCollection Services { get; }
    internal RosenBridgeHostingOptions Options { get; } = new();
    internal Uri? TcpEndpoint { get; private set; }
    private bool _built;

    public RosenBridgeBuilder UseTcp(Uri endpoint)
    {
        EnsureConfigurable();
        ArgumentNullException.ThrowIfNull(endpoint);
        if (TcpEndpoint is not null) throw new InvalidOperationException("A TCP endpoint is already configured.");
        TcpEndpoint = endpoint;
        return this;
    }

    internal void Freeze() => _built = true;
    internal void EnsureConfigurable()
    {
        if (_built) throw new InvalidOperationException("RosenBridge has already been built.");
    }
}

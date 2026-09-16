using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Stella.RosenBridge.Hosting;

/// <summary>Builds a standalone RB application using the standard .NET Generic Host.</summary>
public sealed class RosenBridgeAppBuilder
{
    private readonly HostApplicationBuilder _host;
    private readonly RosenBridgeBuilder _registration;
    private bool _built;

    internal RosenBridgeAppBuilder(string[]? args)
    {
        _host = Host.CreateApplicationBuilder(args);
        _registration = _host.Services.AddRosenBridge();
    }

    public IServiceCollection Services => _host.Services;
    public ConfigurationManager Configuration => _host.Configuration;
    public IHostEnvironment Environment => _host.Environment;
    public ILoggingBuilder Logging => _host.Logging;
    public RosenBridgeHostingOptions Options => _registration.Options;

    public RosenBridgeAppBuilder UseTcp(Uri endpoint)
    {
        _registration.UseTcp(endpoint);
        return this;
    }

    public RosenBridgeApp Build()
    {
        if (_built) throw new InvalidOperationException("The application has already been built.");
        if (_registration.TcpEndpoint is null)
            throw new InvalidOperationException("Configure UseTcp before building a standalone RosenBridgeApp.");
        _built = true;
        _registration.Freeze();
        var host = _host.Build();
        try
        {
            _ = host.Services.GetRequiredService<RosenBridgeHost>();
            return new RosenBridgeApp(host);
        }
        catch { host.Dispose(); throw; }
    }
}

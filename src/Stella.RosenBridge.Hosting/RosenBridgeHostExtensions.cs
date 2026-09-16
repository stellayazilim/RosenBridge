using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Stella.RosenBridge.Channels;

namespace Stella.RosenBridge.Hosting;

/// <summary>Maps logical RB endpoints on a built host before startup, including WebApplication.</summary>
public static class RosenBridgeHostExtensions
{
    public static IHost MapChannel(this IHost host, string path, Func<Channel, CancellationToken, Task> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return host.MapChannel(path, (channel, _, token) => handler(channel, token));
    }

    public static IHost MapChannel(this IHost host, string path,
        Func<Channel, IServiceProvider, CancellationToken, Task> handler)
    {
        ArgumentNullException.ThrowIfNull(host);
        host.Services.GetRequiredService<RosenBridgeHost>().MapChannel(path, handler);
        return host;
    }

    /// <summary>Resolves a service registered before Build from the channel's dependency scope.</summary>
    public static IHost MapChannel<TService>(this IHost host, string path,
        Func<Channel, TService, CancellationToken, Task> handler) where TService : class
    {
        ArgumentNullException.ThrowIfNull(host);
        ArgumentNullException.ThrowIfNull(handler);
        if (host.Services.GetRequiredService<IServiceProviderIsService>().IsService(typeof(TService)) is false)
            throw new InvalidOperationException($"Register {typeof(TService).Name} in builder.Services before Build.");
        return host.MapChannel(path, (channel, services, token) =>
            handler(channel, services.GetRequiredService<TService>(), token));
    }
}

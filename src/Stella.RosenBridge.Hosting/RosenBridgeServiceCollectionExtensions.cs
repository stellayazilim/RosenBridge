using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Stella.RosenBridge.Hosting;

public static class RosenBridgeServiceCollectionExtensions
{
    public static RosenBridgeBuilder AddRosenBridge(this IServiceCollection services,
        Action<RosenBridgeHostingOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        var existing = services.FirstOrDefault(d => d.ServiceType == typeof(RosenBridgeBuilder))?.ImplementationInstance as RosenBridgeBuilder;
        var builder = existing ?? new RosenBridgeBuilder(services);
        builder.EnsureConfigurable();
        configure?.Invoke(builder.Options);
        if (existing is not null) return builder;
        services.AddSingleton(builder);
        services.TryAddSingleton<RosenBridgeFactory>();
        services.AddSingleton<RosenBridgeHost>();
        services.AddHostedService(sp => sp.GetRequiredService<RosenBridgeHost>());
        return builder;
    }
}

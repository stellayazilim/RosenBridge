using Microsoft.Extensions.DependencyInjection;

namespace Stella.RosenBridge.Hosting.AspNetCore;

public static class RosenBridgeHttpClientServiceCollectionExtensions
{
    /// <summary>Registers a lazily connected, DI-owned client using HTTP/1.1 Upgrade.</summary>
    public static IServiceCollection AddRosenBridgeHttpClient(this IServiceCollection services,
        Action<RosenBridgeClientRegistrationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return services.AddRosenBridgeClient(options =>
        {
            options.Endpoint = new Uri("https://localhost:7001/rb");
            configure(options);
            if (options.Endpoint is null || !options.Endpoint.IsAbsoluteUri || options.Endpoint.Scheme is not ("http" or "https"))
                throw new ArgumentException("An HTTP(S) endpoint is required.");
        }, static (factory, endpoint, options, ct) => factory.ConnectOverHttpAsync(endpoint, options, ct));
    }
}

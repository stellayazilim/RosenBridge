using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Stella.RosenBridge.Hosting.AspNetCore;

public static class RosenBridgeEndpointExtensions
{
    internal const string UpgradeProtocol = "rosenbridge";

    /// <summary>Maps an HTTP/1.1 upgrade entry point for management and ticket-bound data connections.</summary>
    public static IEndpointConventionBuilder MapRosenBridge(this IEndpointRouteBuilder endpoints, string pattern = "/rb")
    {
        var host = endpoints.ServiceProvider.GetRequiredService<RosenBridgeHost>();
        if (host.UsesTcp) throw new InvalidOperationException("HTTP Upgrade hosting cannot be combined with UseTcp on the same server.");
        return endpoints.MapGet(pattern, async context =>
        {
            // Inspect the actual TLS feature, not forwarded scheme headers.
            var secure = context.Features.Get<ITlsConnectionFeature>() is not null;
            var remote = context.Connection.RemoteIpAddress;
            var local = context.Connection.LocalIpAddress;
            static bool IsLoopback(IPAddress? ip) => ip is not null &&
                IPAddress.IsLoopback(ip.IsIPv4MappedToIPv6 ? ip.MapToIPv4() : ip);
            if (!secure && !(host.AllowInsecureLoopback && IsLoopback(remote) && IsLoopback(local)))
            {
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                return;
            }
            var upgrade = context.Features.Get<IHttpUpgradeFeature>();
            if (context.Request.Protocol != "HTTP/1.1" || upgrade?.IsUpgradableRequest != true ||
                !string.Equals(context.Request.Headers.Upgrade.ToString(), UpgradeProtocol, StringComparison.OrdinalIgnoreCase) ||
                context.Request.ContentLength is > 0 || context.Request.Headers.ContainsKey("Transfer-Encoding"))
            {
                context.Response.StatusCode = StatusCodes.Status426UpgradeRequired;
                context.Response.Headers.Upgrade = UpgradeProtocol;
                return;
            }
            context.Response.StatusCode = StatusCodes.Status101SwitchingProtocols;
            context.Response.Headers.Upgrade = UpgradeProtocol;
            var stream = await upgrade.UpgradeAsync().ConfigureAwait(false);
            var connection = new HttpStreamConnection(stream,
                new IPEndPoint(local ?? IPAddress.None, context.Connection.LocalPort),
                new IPEndPoint(remote ?? IPAddress.None, context.Connection.RemotePort));
            try { await host.Server.ProcessConnectionAsync(connection, context.RequestAborted).ConfigureAwait(false); }
            catch (RosenBridgeException) { context.Abort(); }
            catch (ObjectDisposedException) { context.Abort(); }
            catch (OperationCanceledException) { context.Abort(); }
        });
    }
}

using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Security.Authentication;
using Stella.RosenBridge.Transport;
using Stella.RosenBridge.Transport.Tcp;

namespace Stella.RosenBridge.Hosting.AspNetCore;

public static class RosenBridgeHttpClientExtensions
{
    /// <summary>Opens a management session and a fresh HTTP/1.1 upgraded connection for each channel.</summary>
    public static Task<RosenBridgeClient> ConnectOverHttpAsync(this RosenBridgeFactory factory, Uri endpoint,
        RosenBridgeClientOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(endpoint);
        options ??= new();
        if (!endpoint.IsAbsoluteUri || endpoint.Scheme is not ("http" or "https") ||
            endpoint.UserInfo.Length != 0 || endpoint.Fragment.Length != 0 || endpoint.Query.Length != 0)
            throw new ArgumentException("Use an HTTP(S) endpoint without credentials, query, or fragment.", nameof(endpoint));
        if (endpoint.Scheme == "http" && !options.AllowInsecureLoopback)
            throw new ArgumentException("HTTP requires explicit AllowInsecureLoopback.", nameof(options));
        var wireOptions = new RosenBridgeClientOptions
        {
            OpenTimeout = options.OpenTimeout, MaxPendingRequests = options.MaxPendingRequests,
            AllowInsecureLoopback = options.AllowInsecureLoopback, CertificateValidation = options.CertificateValidation
        };
        return factory.ConnectUsingAsync(
            ct => UpgradeAsync(endpoint, options, options.Credential, ct),
            ct => UpgradeAsync(endpoint, options, null, ct), wireOptions, cancellationToken);
    }

    private static async ValueTask<ITransportConnection> UpgradeAsync(Uri endpoint, RosenBridgeClientOptions options, string? credential, CancellationToken token)
    {
        ITransportConnection? raw = null;
        var handler = new SocketsHttpHandler
        {
            AllowAutoRedirect = false,
            UseProxy = false,
            UseCookies = false,
            MaxResponseHeadersLength = 8,
            SslOptions = new()
            {
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                RemoteCertificateValidationCallback = options.CertificateValidation
            },
            ConnectCallback = async (context, ct) =>
            {
                var addresses = await Dns.GetHostAddressesAsync(context.DnsEndPoint.Host, ct).ConfigureAwait(false);
                Exception? last = null;
                foreach (var address in addresses.OrderBy(ip => ip.AddressFamily == AddressFamily.InterNetwork ? 0 : 1))
                {
                    if (endpoint.Scheme == "http" && !IPAddress.IsLoopback(address))
                        throw new IOException("Insecure HTTP is limited to loopback addresses.");
                    try
                    {
                        raw = await new TcpTransportFactory().ConnectAsync(new(address, context.DnsEndPoint.Port), ct).ConfigureAwait(false);
                        return raw.Stream;
                    }
                    catch (SocketException error) { last = error; }
                }
                throw new IOException("Unable to connect to HTTP endpoint.", last);
            }
        };
        var client = new HttpClient(handler) { Timeout = Timeout.InfiniteTimeSpan };
        HttpResponseMessage? response = null;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, endpoint)
            {
                Version = HttpVersion.Version11,
                VersionPolicy = HttpVersionPolicy.RequestVersionExact
            };
            if (credential is not null) request.Headers.Add("Authorization", credential);
            request.Headers.Connection.Add("Upgrade");
            request.Headers.Upgrade.Add(new ProductHeaderValue(RosenBridgeEndpointExtensions.UpgradeProtocol));
            response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
            if (response.StatusCode != HttpStatusCode.SwitchingProtocols ||
                !response.Headers.Connection.Any(value => value.Equals("Upgrade", StringComparison.OrdinalIgnoreCase)) ||
                response.Headers.Upgrade.Count != 1 ||
                !response.Headers.Upgrade.Single().ToString().Equals(RosenBridgeEndpointExtensions.UpgradeProtocol, StringComparison.OrdinalIgnoreCase))
                throw new IOException($"RosenBridge HTTP Upgrade rejected ({(int)response.StatusCode}).");
            var stream = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false);
            return new HttpStreamConnection(stream, raw!.LocalEndPoint, raw.RemoteEndPoint,
                new HttpOwner(response, client), raw);
        }
        catch
        {
            response?.Dispose();
            client.Dispose();
            if (raw is not null) await raw.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private sealed class HttpOwner(HttpResponseMessage response, HttpClient client) : IDisposable
    {
        public void Dispose() { response.Dispose(); client.Dispose(); }
    }
}

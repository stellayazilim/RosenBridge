using System.Net;
using System.Net.Security;
using System.Security.Authentication;
using Stella.RosenBridge.Transport;

namespace Stella.RosenBridge.Internal;

internal sealed class TlsConnection(ITransportConnection inner, SslStream stream) : ITransportConnection
{
    private int _disposed;
    public EndPoint LocalEndPoint => inner.LocalEndPoint;
    public EndPoint RemoteEndPoint => inner.RemoteEndPoint;
    public Stream Stream => stream;

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        try { await stream.DisposeAsync().ConfigureAwait(false); }
        finally { await inner.DisposeAsync().ConfigureAwait(false); }
    }

    internal static async Task<ITransportConnection> ClientAsync(ITransportConnection connection,
        Uri uri, RosenBridgeClientOptions options, CancellationToken token)
    {
        if (uri.Scheme == "rb") return connection;
        var ssl = new SslStream(connection.Stream, leaveInnerStreamOpen: true, options.CertificateValidation);
        try
        {
            await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = uri.DnsSafeHost,
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13
            }, token).ConfigureAwait(false);
            return new TlsConnection(connection, ssl);
        }
        catch { await ssl.DisposeAsync().ConfigureAwait(false); throw; }
    }

    internal static async Task<ITransportConnection> ServerAsync(ITransportConnection connection,
        Uri uri, RosenBridgeServerOptions options, CancellationToken token)
    {
        if (uri.Scheme == "rb") return connection;
        var ssl = new SslStream(connection.Stream, leaveInnerStreamOpen: true);
        try
        {
            await ssl.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
            {
                ServerCertificate = options.Certificate,
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13
            }, token).ConfigureAwait(false);
            return new TlsConnection(connection, ssl);
        }
        catch { await ssl.DisposeAsync().ConfigureAwait(false); throw; }
    }
}

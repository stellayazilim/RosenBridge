using Stella.RosenBridge.Transport;
using Stella.RosenBridge.Transport.Tcp;

namespace Stella.RosenBridge;

public sealed class RosenBridgeFactory(ITransportFactory? transport = null)
{
    private readonly ITransportFactory _transport = transport ?? new TcpTransportFactory();

    public RosenBridgeServer CreateServer(Uri uri, RosenBridgeServerOptions options)
    {
        ArgumentNullException.ThrowIfNull(uri);
        return new(uri, options, _transport);
    }

    /// <summary>Creates a server whose connections are accepted and secured by an external adapter.</summary>
    public RosenBridgeServer CreateServer(RosenBridgeServerOptions options)
        => new(null, options, _transport);

    /// <summary>Connects through an adapter that supplies a new, secured duplex connection on every call.</summary>
    public Task<RosenBridgeClient> ConnectUsingAsync(Func<CancellationToken, ValueTask<ITransportConnection>> connect,
        RosenBridgeClientOptions? options = null, CancellationToken cancellationToken = default)
        => RosenBridgeClient.ConnectAsync(connect, options ?? new(), cancellationToken);

    public Task<RosenBridgeClient> ConnectAsync(Uri uri, RosenBridgeClientOptions? options = null,
        CancellationToken cancellationToken = default)
        => RosenBridgeClient.ConnectAsync(uri, options ?? new(), _transport, cancellationToken);
}

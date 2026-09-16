using System.Net;

namespace Stella.RosenBridge.Transport;

/// <summary>Creates listeners and outgoing connections for an IP endpoint.</summary>
public interface ITransportFactory
{
    /// <summary>Binds immediately. Port zero requests an OS-assigned port.</summary>
    ITransportListener Listen(IPEndPoint endPoint, int backlog = 128);

    /// <summary>Connects to the endpoint. The caller owns the returned connection.</summary>
    ValueTask<ITransportConnection> ConnectAsync(
        IPEndPoint endPoint, CancellationToken cancellationToken = default);
}

using System.Net;

namespace Stella.RosenBridge.Transport;

/// <summary>A listening endpoint. Disposal stops acceptance, not accepted connections.</summary>
public interface ITransportListener : IAsyncDisposable
{
    EndPoint LocalEndPoint { get; }

    /// <summary>
    /// Accepts one connection, owned by the caller. Cancellation affects only this accept.
    /// Keep one accept operation outstanding per listener.
    /// </summary>
    ValueTask<ITransportConnection> AcceptAsync(CancellationToken cancellationToken = default);
}

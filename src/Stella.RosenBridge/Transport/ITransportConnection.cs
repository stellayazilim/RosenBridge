using System.Net;

namespace Stella.RosenBridge.Transport;

/// <summary>An owned, full-duplex byte connection without application framing.</summary>
public interface ITransportConnection : IAsyncDisposable
{
    EndPoint LocalEndPoint { get; }
    EndPoint RemoteEndPoint { get; }

    /// <summary>
    /// Supports one reader and one writer concurrently. Reads may be partial.
    /// Disposing this stream or the connection closes the underlying transport.
    /// </summary>
    Stream Stream { get; }
}

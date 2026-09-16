using System.Net;
using Stella.RosenBridge.Transport;

namespace Stella.RosenBridge.Hosting.AspNetCore;

internal sealed class HttpStreamConnection(Stream stream, EndPoint local, EndPoint remote,
    IDisposable? owner = null, ITransportConnection? transport = null) : ITransportConnection
{
    private int _disposed;
    public Stream Stream => stream;
    public EndPoint LocalEndPoint => local;
    public EndPoint RemoteEndPoint => remote;
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        try { await stream.DisposeAsync().ConfigureAwait(false); }
        finally
        {
            owner?.Dispose();
            if (transport is not null) await transport.DisposeAsync().ConfigureAwait(false);
        }
    }
}

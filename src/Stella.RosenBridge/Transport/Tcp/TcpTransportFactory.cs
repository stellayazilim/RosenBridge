using System.Net;
using System.Net.Sockets;

namespace Stella.RosenBridge.Transport.Tcp;

/// <summary>Creates raw TCP transports using the platform socket implementation.</summary>
public sealed class TcpTransportFactory : ITransportFactory
{
    public ITransportListener Listen(IPEndPoint endPoint, int backlog = 128)
    {
        ArgumentNullException.ThrowIfNull(endPoint);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backlog);

        var socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            socket.Bind(endPoint);
            socket.Listen(backlog);
            return new TcpTransportListener(socket);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    public async ValueTask<ITransportConnection> ConnectAsync(
        IPEndPoint endPoint, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(endPoint);
        cancellationToken.ThrowIfCancellationRequested();

        var socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            await socket.ConnectAsync(endPoint, cancellationToken).ConfigureAwait(false);
            return new TcpTransportConnection(socket);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }
}

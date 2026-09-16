using System.Net;
using System.Net.Sockets;

namespace Stella.RosenBridge.Transport.Tcp;

internal sealed class TcpTransportConnection : ITransportConnection
{
    internal TcpTransportConnection(Socket socket)
    {
        LocalEndPoint = socket.LocalEndPoint!;
        RemoteEndPoint = socket.RemoteEndPoint!;
        Stream = new NetworkStream(socket, ownsSocket: true);
    }

    public EndPoint LocalEndPoint { get; }
    public EndPoint RemoteEndPoint { get; }
    public Stream Stream { get; }

    public ValueTask DisposeAsync() => Stream.DisposeAsync();
}

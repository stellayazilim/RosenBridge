using System.Net;
using System.Net.Sockets;

namespace Stella.RosenBridge.Transport.Tcp;

internal sealed class TcpTransportListener : ITransportListener
{
    private readonly Socket _socket;

    internal TcpTransportListener(Socket socket)
    {
        _socket = socket;
        LocalEndPoint = socket.LocalEndPoint!;
    }

    public EndPoint LocalEndPoint { get; }

    public async ValueTask<ITransportConnection> AcceptAsync(CancellationToken cancellationToken = default)
    {
        var accepted = await _socket.AcceptAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return new TcpTransportConnection(accepted);
        }
        catch
        {
            accepted.Dispose();
            throw;
        }
    }

    public ValueTask DisposeAsync()
    {
        _socket.Dispose();
        return ValueTask.CompletedTask;
    }
}

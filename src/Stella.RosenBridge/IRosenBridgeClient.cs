using Stella.RosenBridge.Channels;

namespace Stella.RosenBridge;

/// <summary>Requests independent channels from a shared management session.</summary>
public interface IRosenBridgeClient : IAsyncDisposable
{
    Task<Channel> RequestChannelAsync(string path, CancellationToken cancellationToken = default);
}

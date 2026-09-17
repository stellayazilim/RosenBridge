using System.Collections.Concurrent;

namespace Stella.RosenBridge;

/// <summary>Transport session state shared by all channels on one management connection.</summary>
public sealed class RosenBridgeSession
{
    private readonly Action _close;
    internal RosenBridgeSession(string id, CancellationToken closed, Action close)
    {
        Id = id;
        Closed = closed;
        _close = close;
    }
    public string Id { get; }
    /// <summary>Upper-layer state. Values are caller-owned and are not automatically disposed.</summary>
    public ConcurrentDictionary<object, object?> Items { get; } = new();
    public CancellationToken Closed { get; }
    /// <summary>Closes management and invalidates tickets and channels. Safe to call repeatedly.</summary>
    public void Close() => _close();
}

/// <summary>Trusted adapter metadata; when supplied, it replaces credentials from the management envelope.</summary>
public sealed record RosenBridgeConnectionContext(string? Credential);

namespace Stella.RosenBridge.Channels;

public sealed record ChannelOptions
{
    public int WriteBufferBytes { get; init; } = 64 * 1024;
    public ChannelHeader Header { get; init; } = new();
}

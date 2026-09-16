namespace Stella.RosenBridge.Channels;

/// <summary>Application metadata sent once before each outgoing payload.</summary>
public sealed record ChannelHeader(string? ContentType = null);

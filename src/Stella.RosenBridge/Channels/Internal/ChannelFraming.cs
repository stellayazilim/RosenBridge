using System.Buffers.Binary;

namespace Stella.RosenBridge.Channels.Internal;

// Experimental application framing, not a stable wire protocol.
internal static class ChannelFraming
{
    internal const byte Data = 0;
    internal const byte End = 1;
    internal const byte Header = 2;
    internal const int MaxDataBytes = 16 * 1024;
    internal const int MaxHeaderBytes = 8192;

    internal static async ValueTask WriteAsync(
        Stream stream, byte[] header, byte flags, ReadOnlyMemory<byte> payload, CancellationToken token)
    {
        header[0] = flags;
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(1), (uint)payload.Length);
        await stream.WriteAsync(header, token).ConfigureAwait(false);
        if (!payload.IsEmpty)
            await stream.WriteAsync(payload, token).ConfigureAwait(false);
    }

    internal static async ValueTask<(byte Flags, int Length)> ReadAsync(
        Stream stream, byte[] header, CancellationToken token)
    {
        await stream.ReadExactlyAsync(header, token).ConfigureAwait(false);
        var flags = header[0];
        var length = BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(1));
        var maximum = flags == Header ? MaxHeaderBytes : MaxDataBytes;
        if (flags > Header || length > maximum || (flags == Data && length == 0))
            throw new InvalidDataException("Invalid channel frame header.");
        return (flags, (int)length);
    }
}

using System.Buffers.Binary;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text;

namespace Stella.RosenBridge.Internal;

// Experimental length-prefixed management messages. Never log this object: it can contain credentials.
internal sealed class ManagementMessage
{
    public string Type { get; set; } = "";
    public long Id { get; set; }
    public string? Session { get; set; }
    public string? Path { get; set; }
    public string? Ticket { get; set; }
    public string? Credential { get; set; }
    public string? Code { get; set; }
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(ManagementMessage))]
internal partial class ManagementJsonContext : JsonSerializerContext;

internal static class ManagementWire
{
    private const int Limit = 8192;
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    internal static async Task WriteAsync(Stream stream, ManagementMessage message, CancellationToken token)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(message, ManagementJsonContext.Default.ManagementMessage);
        if (bytes.Length > Limit) throw new InvalidDataException("Management message is too large.");
        var prefix = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(prefix, bytes.Length);
        await stream.WriteAsync(prefix, token).ConfigureAwait(false);
        await stream.WriteAsync(bytes, token).ConfigureAwait(false);
    }

    internal static async Task<ManagementMessage> ReadAsync(Stream stream, CancellationToken token)
    {
        var prefix = new byte[4];
        await stream.ReadExactlyAsync(prefix, token).ConfigureAwait(false);
        var length = BinaryPrimitives.ReadInt32BigEndian(prefix);
        if (length is < 1 or > Limit) throw new InvalidDataException("Invalid management message length.");
        var bytes = new byte[length];
        await stream.ReadExactlyAsync(bytes, token).ConfigureAwait(false);
        Validate(bytes);
        return JsonSerializer.Deserialize(bytes, ManagementJsonContext.Default.ManagementMessage)
            ?? throw new InvalidDataException("Missing management message.");
    }

    private static void Validate(byte[] bytes)
    {
        _ = StrictUtf8.GetCharCount(bytes);
        // The management envelope is flat. Reject duplicate names and structured values,
        // including unknown fields, before source-generated deserialization.
        var reader = new Utf8JsonReader(bytes, new JsonReaderOptions { MaxDepth = 2 });
        var names = new HashSet<string>(StringComparer.Ordinal);
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            throw new InvalidDataException("Expected management object.");
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName || !names.Add(reader.GetString()!))
                throw new InvalidDataException("Invalid or duplicate management field.");
            if (!reader.Read() || reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray)
                throw new InvalidDataException("Invalid management value.");
            if (reader.TokenType == JsonTokenType.String) _ = reader.GetString();
        }
        if (reader.TokenType != JsonTokenType.EndObject || reader.Read())
            throw new InvalidDataException("Invalid management object ending.");
    }
}

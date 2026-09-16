using System.Text.Json.Serialization;

namespace Stella.RosenBridge.Channels.Internal;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(ChannelHeader))]
internal partial class ChannelJsonContext : JsonSerializerContext;

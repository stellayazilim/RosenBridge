using System.Net.Security;
using System.Security.Claims;
using System.Security.Cryptography.X509Certificates;

namespace Stella.RosenBridge;

public sealed class RosenBridgeClientOptions
{
    public string? Credential { get; init; }
    public bool AllowInsecureLoopback { get; init; }
    public TimeSpan OpenTimeout { get; init; } = TimeSpan.FromSeconds(30);
    public int MaxPendingRequests { get; init; } = 64;
    public RemoteCertificateValidationCallback? CertificateValidation { get; init; }
}

public sealed record RosenBridgeServerOptions
{
    public X509Certificate2? Certificate { get; init; }
    public bool AllowInsecureLoopback { get; init; }
    public bool AllowAnonymous { get; init; }
    public Func<string?, CancellationToken, ValueTask<ClaimsPrincipal?>>? AuthenticateAsync { get; init; }
    public Func<ClaimsPrincipal, string, bool>? AuthorizeChannel { get; init; }
    public int MaxConnectionsPerSession { get; init; } = 8;
    public int MaxSockets { get; init; } = 256;
    public TimeSpan HandshakeTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan TicketLifetime { get; init; } = TimeSpan.FromSeconds(10);
}

public sealed class RosenBridgeException(string code) : IOException($"RosenBridge request failed: {code}.")
{
    public string Code { get; } = code;
}

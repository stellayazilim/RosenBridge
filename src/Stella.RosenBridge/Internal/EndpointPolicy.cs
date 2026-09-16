using System.Net;
using System.Text;

namespace Stella.RosenBridge.Internal;

internal static class EndpointPolicy
{
    internal static void Validate(Uri uri, bool server, bool allowInsecure)
    {
        ArgumentNullException.ThrowIfNull(uri);
        if (!uri.IsAbsoluteUri || uri.Scheme is not ("rb" or "rbs") ||
            uri.Port < (server ? 0 : 1) || uri.UserInfo.Length != 0 || uri.Query.Length != 0 ||
            uri.Fragment.Length != 0 || uri.AbsolutePath is not ("" or "/"))
            throw new ArgumentException("Use a root rb/rbs URI with an explicit port and no query or credentials.", nameof(uri));
        if (uri.Scheme == "rb" && !allowInsecure)
            throw new ArgumentException("Raw TCP requires AllowInsecureLoopback.", nameof(uri));
    }

    internal static void ValidatePath(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        if (!path.StartsWith('/') || path == "/" || path.Contains('\\') || path.Any(char.IsControl) ||
            path.Split('/').Skip(1).Any(segment => segment is "" or "." or "..") ||
            new UTF8Encoding(false, true).GetByteCount(path) > 1024)
            throw new ArgumentException("Use a decoded, non-root channel path without empty or dot segments.", nameof(path));
    }

    internal static void CheckAddress(Uri uri, IPAddress address)
    {
        if (uri.Scheme == "rb" && !IPAddress.IsLoopback(address))
            throw new ArgumentException("Insecure TCP is limited to loopback addresses.");
    }

    internal static void PositiveTimeout(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero || value > TimeSpan.FromMinutes(5))
            throw new ArgumentOutOfRangeException(name);
    }
}

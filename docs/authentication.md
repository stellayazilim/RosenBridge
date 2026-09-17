# Session acceptance and channel tickets

RB transports credentials but does not authenticate users or implement an identity provider.
The upper layer decides whether to accept a management connection. Once accepted, RB
owns ticket issuance, one-time binding, expiry, capacity and transport-session cleanup.

## Upper-layer policy

Configure `RosenBridgeServerOptions.AcceptSessionAsync` to validate an opaque credential
and optionally attach application state. Returning false rejects the connection with
`unauthorized`. Without this callback, management sessions are accepted without identity
checks. TLS and connection limits still apply.

```csharp
var identityKey = new object();
var options = new RosenBridgeServerOptions
{
    Certificate = certificate,
    AcceptSessionAsync = async (session, credential, ct) =>
    {
        var identity = await identityProvider.ValidateAsync(credential, ct);
        if (identity is null) return false;
        session.Items[identityKey] = identity;
        return true;
    },
    AuthorizeChannelAsync = (session, path, ct) =>
        endpointPolicy.CheckAsync(session.Items[identityKey], path, ct)
};
```

The example's identity provider and endpoint policy are application services. RB neither
parses schemes nor constructs a ClaimsPrincipal. `AuthorizeChannelAsync` is optional,
receives the accepted session and logical channel path, and runs before each ticket is
issued. Returning false rejects that request with `forbidden` while keeping the session
usable. Without it, registered endpoints are allowed, subject to capacity.

Acceptance and endpoint-policy waits are bounded by `HandshakeTimeout`. A callback
exception or timeout closes the connection/session; no ticket is granted. Callbacks
must honor cancellation; timeout does not forcibly stop application code.

## Credential transport

- TCP/TLS: `RosenBridgeClientOptions.Credential` travels in the first management envelope.
- HTTP Upgrade: the same property is the complete `Authorization` header value, such as
  `Bearer <token>` or `Basic <base64>`, on the initial management upgrade only.
- The HTTP adapter passes the header to the acceptance callback after upgrade and reading
  the management role. It does not repeat the credential in the RB envelope or on data
  upgrades. A trusted adapter context overrides envelope credentials even when its value
  is null. Missing headers cannot be bypassed by supplying an envelope credential.
- Credentials are never put in URLs. TLS protects credentials and tickets in transit;
  cleartext requires explicitly enabled loopback development mode.

```csharp
services.AddRosenBridgeHttpClient(options =>
{
    options.Endpoint = new Uri("https://localhost:7001/rb");
    options.Credential = $"Bearer {accessToken}";
});
```

RB does not validate JWTs, decode Basic credentials, acquire or refresh tokens, implement
SASL, or infer identity from HttpContext.User. Requiring HTTP authentication on every
upgrade also gates ticket-bound data connections, which the provided client opens without
Authorization headers. Apply the upper-layer policy through session acceptance when using
this client flow; blanket RequireAuthorization on the upgrade route is not equivalent.

## Shared state and lifetime

Server-side `channel.Session` exposes the same `RosenBridgeSession` for channels belonging
to one management connection. It contains `Id`, `Items`, `Closed`, and `Close()`.
Raw channels and client-side channels do not have a server session context.

`Items` is a concurrent dictionary. Stored objects belong to the application and are not
automatically disposed or cloned; mutable objects need their own concurrency control.
Application identity, authorization state and expiry policy belong here or in an external
session service. The application can call `session.Close()` on revocation or expiry.
Repeated calls are safe. Closing signals `Closed`, invalidates outstanding tickets and
cancels active channels. Management disconnect and server shutdown use the same cleanup.
Physical connections and handlers finish asynchronously and must honor cancellation.

A channel connects with an expiring, single-use ticket bound to the session/reservation.
It does not repeat user authentication. Invalid, expired or replayed tickets reject that
data connection without granting access. A closed session cannot issue or bind tickets.

## Migration

`AllowAnonymous`, `AuthenticateAsync`, `InitializeSessionAsync`, synchronous
`AuthorizeChannel`, and `RosenBridgeSession.User` are replaced by `AcceptSessionAsync`,
`AuthorizeChannelAsync`, and application-owned `Items`. In particular, applications that
require authentication must explicitly install an acceptance policy; the RB default has
no identity checks. HTTP clients must supply the complete Authorization header value.
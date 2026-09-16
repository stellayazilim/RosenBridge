using System.Collections.Concurrent;
using System.Net;
using System.Security.Claims;
using Stella.RosenBridge.Channels;
using Stella.RosenBridge.Internal;
using Stella.RosenBridge.Transport;

namespace Stella.RosenBridge;

/// <summary>Processes management sessions and per-request connections from TCP or an external adapter.</summary>
public sealed class RosenBridgeServer : IAsyncDisposable
{
    private readonly Uri? _uri;
    private readonly RosenBridgeServerOptions _options;
    private readonly ITransportFactory _transport;
    private readonly Dictionary<string, Func<Channel, CancellationToken, Task>> _handlers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ServerSession> _sessions = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<long, Task> _workers = new();
    private readonly CancellationTokenSource _lifetime = new();
    private readonly SemaphoreSlim _socketSlots;
    private readonly object _gate = new();
    private ITransportListener? _listener;
    private Task? _acceptLoop;
    private Task? _dispose;
    private Action<Exception>? _onError;
    private long _nextWorker;
    private bool _started;

    internal RosenBridgeServer(Uri? uri, RosenBridgeServerOptions options, ITransportFactory transport)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (uri is not null) EndpointPolicy.Validate(uri, server: true, options.AllowInsecureLoopback);
        EndpointPolicy.PositiveTimeout(options.HandshakeTimeout, nameof(options.HandshakeTimeout));
        EndpointPolicy.PositiveTimeout(options.TicketLifetime, nameof(options.TicketLifetime));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxConnectionsPerSession);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.MaxSockets, 2);
        if (uri?.Scheme == "rbs" && options.Certificate?.HasPrivateKey != true)
            throw new ArgumentException("TLS server requires a certificate with a private key.", nameof(options));
        if (options.AuthenticateAsync is null && !options.AllowAnonymous)
            throw new ArgumentException("Configure authentication or explicitly allow anonymous sessions.", nameof(options));
        _uri = uri;
        _options = options;
        _transport = transport;
        _socketSlots = new(options.MaxSockets, options.MaxSockets);
    }

    public IPEndPoint LocalEndPoint => (IPEndPoint)(_listener?.LocalEndPoint
        ?? throw new InvalidOperationException("Server has not started."));

    public RosenBridgeServer MapChannel(string path, Func<Channel, CancellationToken, Task> handler)
    {
        EndpointPolicy.ValidatePath(path);
        ArgumentNullException.ThrowIfNull(handler);
        lock (_gate)
        {
            EnsureConfigurable();
            _handlers.Add(path, handler);
        }
        return this;
    }

    public RosenBridgeServer OnError(Action<Exception> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        lock (_gate) { EnsureConfigurable(); _onError += handler; }
        return this;
    }

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            EnsureConfigurable();
            cancellationToken.ThrowIfCancellationRequested();
            if (_uri is null)
            {
                _started = true;
                return Task.CompletedTask;
            }
            var host = _uri.DnsSafeHost;
            var address = host.Equals("localhost", StringComparison.OrdinalIgnoreCase) ? IPAddress.Loopback :
                IPAddress.TryParse(host, out var ip) ? ip :
                throw new ArgumentException("Bind to an IP literal or localhost.");
            EndpointPolicy.CheckAddress(_uri, address);
            _listener = _transport.Listen(new IPEndPoint(address, _uri.Port));
            _started = true;
            _acceptLoop = Task.Run(AcceptAsync);
            return Task.CompletedTask;
        }
    }

    private void EnsureConfigurable()
    {
        ObjectDisposedException.ThrowIf(_dispose is not null, this);
        if (_started) throw new InvalidOperationException("Server has already started.");
    }

    /// <summary>Processes an externally accepted connection until it closes.</summary>
    /// <remarks>
    /// Only supported by a server created without a TCP endpoint. The adapter must enforce TLS
    /// or an explicit loopback-only development policy before calling. Ownership transfers on entry,
    /// including rejection. The cancellation token covers the entire connection lifetime.
    /// </remarks>
    public async Task ProcessConnectionAsync(ITransportConnection connection, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        Task worker;
        try
        {
            lock (_gate)
            {
                ObjectDisposedException.ThrowIf(_dispose is not null, this);
                if (!_started || _uri is not null)
                    throw new InvalidOperationException("Start a server configured for externally accepted connections.");
                cancellationToken.ThrowIfCancellationRequested();
                if (!_socketSlots.Wait(0)) throw new RosenBridgeException("busy");
                worker = TrackConnection(connection, cancellationToken);
            }
        }
        catch { await connection.DisposeAsync().ConfigureAwait(false); throw; }
        await worker.ConfigureAwait(false);
    }

    private Task TrackConnection(ITransportConnection connection, CancellationToken token)
    {
        var id = Interlocked.Increment(ref _nextWorker);
        var worker = Task.Run(() => HandleAsync(connection, token));
        _workers.TryAdd(id, worker);
        _ = worker.ContinueWith(_ => { _workers.TryRemove(id, out var ignored); },
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return worker;
    }

    private async Task AcceptAsync()
    {
        try
        {
            while (true)
            {
                await _socketSlots.WaitAsync(_lifetime.Token).ConfigureAwait(false);
                ITransportConnection connection;
                try { connection = await _listener!.AcceptAsync(_lifetime.Token).ConfigureAwait(false); }
                catch { _socketSlots.Release(); throw; }
                _ = TrackConnection(connection, CancellationToken.None);
            }
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
        catch (ObjectDisposedException) when (_lifetime.IsCancellationRequested) { }
        catch (Exception error)
        {
            Report(error);
            await _lifetime.CancelAsync().ConfigureAwait(false);
        }
    }

    private async Task HandleAsync(ITransportConnection connection, CancellationToken cancellationToken)
    {
        try
        {
            using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token, cancellationToken);
            using var setup = CancellationTokenSource.CreateLinkedTokenSource(lifetime.Token);
            setup.CancelAfter(_options.HandshakeTimeout);
            if (_uri is not null)
                connection = await TlsConnection.ServerAsync(connection, _uri, _options, setup.Token).ConfigureAwait(false);
            var message = await ManagementWire.ReadAsync(connection.Stream, setup.Token).ConfigureAwait(false);
            switch (message.Type)
            {
                case "control": await ControlAsync(connection, message, setup.Token, lifetime.Token).ConfigureAwait(false); break;
                case "bind": await BindAsync(connection, message, setup.Token, lifetime.Token).ConfigureAwait(false); break;
                default: await RejectAsync(connection.Stream, message.Id, "invalid-role", setup.Token).ConfigureAwait(false); break;
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception error) { Report(error); }
        finally
        {
            try { await connection.DisposeAsync().ConfigureAwait(false); }
            catch (Exception error) { Report(error); }
            _socketSlots.Release();
        }
    }

    private async Task ControlAsync(ITransportConnection connection, ManagementMessage hello, CancellationToken setupToken, CancellationToken lifetimeToken)
    {
        ClaimsPrincipal? identity = _options.AuthenticateAsync is not null
            ? await _options.AuthenticateAsync(hello.Credential, setupToken).ConfigureAwait(false)
            : new ClaimsPrincipal(new ClaimsIdentity());
        if (identity is null)
        {
            await RejectAsync(connection.Stream, 0, "unauthorized", setupToken).ConfigureAwait(false);
            return;
        }
        using var session = new ServerSession(identity, _options, lifetimeToken);
        if (!_sessions.TryAdd(session.Id, session)) throw new InvalidOperationException("Session identity collision.");
        try
        {
            await ManagementWire.WriteAsync(connection.Stream,
                new() { Type = "ready", Session = session.Id }, setupToken).ConfigureAwait(false);
            while (true)
            {
                var request = await ManagementWire.ReadAsync(connection.Stream, session.Token).ConfigureAwait(false);
                if (request.Id <= 0) throw new InvalidDataException("Invalid request identifier.");
                if (request.Type == "cancel") { session.Cancel(request.Id); continue; }
                if (request.Type != "open") throw new InvalidDataException("Expected channel request.");
                string? rejection = null;
                try { EndpointPolicy.ValidatePath(request.Path!); }
                catch (ArgumentException) { rejection = "invalid-path"; }
                if (rejection is null && !_handlers.ContainsKey(request.Path!)) rejection = "not-found";
                if (rejection is null && _options.AuthorizeChannel?.Invoke(identity, request.Path!) == false)
                    rejection = "forbidden";
                var reservation = rejection is null ? session.Reserve(request.Id, request.Path!) : null;
                if (reservation is null)
                    await RejectAsync(connection.Stream, request.Id, rejection ?? "busy", session.Token).ConfigureAwait(false);
                else
                    await ManagementWire.WriteAsync(connection.Stream,
                        new() { Type = "grant", Id = request.Id, Ticket = reservation.Ticket }, session.Token).ConfigureAwait(false);
            }
        }
        catch (EndOfStreamException) { /* Management peer closed the session. */ }
        finally { _sessions.TryRemove(session.Id, out _); }
    }

    private async Task BindAsync(ITransportConnection connection, ManagementMessage bind, CancellationToken setupToken, CancellationToken lifetimeToken)
    {
        var session = bind.Session is not null && _sessions.TryGetValue(bind.Session, out var found) ? found : null;
        var reservation = session?.Bind(bind.Id, bind.Ticket);
        if (reservation is null)
        {
            await RejectAsync(connection.Stream, bind.Id, "invalid-ticket", setupToken).ConfigureAwait(false);
            return;
        }
        try
        {
            using var bindTimeout = CancellationTokenSource.CreateLinkedTokenSource(setupToken, reservation.Token);
            await ManagementWire.WriteAsync(connection.Stream,
                new() { Type = "bound", Id = bind.Id }, bindTimeout.Token).ConfigureAwait(false);
            using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(reservation.Token, lifetimeToken);
            await using var channel = new Channel(reservation.Path, connection, cancellationToken: lifetime.Token);
            await _handlers[reservation.Path](channel, lifetime.Token).ConfigureAwait(false);
            await channel.Completion.ConfigureAwait(false);
        }
        finally
        {
            // Release only after the physical connection has been closed.
            try { await connection.DisposeAsync().ConfigureAwait(false); }
            finally { session!.Release(reservation); }
        }
    }

    private static Task RejectAsync(Stream stream, long id, string code, CancellationToken token)
        => ManagementWire.WriteAsync(stream, new() { Type = "reject", Id = id, Code = code }, token);

    private void Report(Exception error)
    {
        try { _onError?.Invoke(error); }
        catch { /* Diagnostics must not interrupt cleanup. */ }
    }

    public ValueTask DisposeAsync()
    {
        lock (_gate) { return new(_dispose ??= Task.Run(DisposeCoreAsync)); }
    }

    private async Task DisposeCoreAsync()
    {
        await _lifetime.CancelAsync().ConfigureAwait(false);
        if (_listener is not null) await _listener.DisposeAsync().ConfigureAwait(false);
        if (_acceptLoop is not null) await _acceptLoop.ConfigureAwait(false);
        await Task.WhenAll(_workers.Values).ConfigureAwait(false);
        _lifetime.Dispose();
        _socketSlots.Dispose();
    }
}

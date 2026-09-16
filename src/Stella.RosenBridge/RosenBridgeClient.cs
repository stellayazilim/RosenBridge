using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Stella.RosenBridge.Channels;
using Stella.RosenBridge.Internal;
using Stella.RosenBridge.Transport;

namespace Stella.RosenBridge;

/// <summary>Owns one management session and its independently opened channels.</summary>
public sealed class RosenBridgeClient : IRosenBridgeClient
{
    private readonly RosenBridgeClientOptions _options;
    private readonly Func<CancellationToken, ValueTask<ITransportConnection>> _connect;
    private readonly ITransportConnection _control;
    private readonly CancellationTokenSource _lifetime = new();
    private readonly SemaphoreSlim _writer = new(1, 1);
    private readonly SemaphoreSlim _pendingSlots;
    private readonly ConcurrentDictionary<long, TaskCompletionSource<ManagementMessage>> _pending = new();
    private readonly object _gate = new();
    private readonly Dictionary<long, Channel> _channels = new();
    private Task _reader = Task.CompletedTask;
    private Task? _dispose;
    private bool _closed;
    private long _nextId;
    private int _acquisitions;
    private TaskCompletionSource _acquisitionsDrained = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private RosenBridgeClient(Func<CancellationToken, ValueTask<ITransportConnection>> connect, RosenBridgeClientOptions options,
        ITransportConnection control, string session)
    {
        _options = options;
        _connect = connect;
        _control = control;
        _pendingSlots = new(options.MaxPendingRequests, options.MaxPendingRequests);
        SessionId = session;
    }

    public string SessionId { get; }

    internal static async Task<RosenBridgeClient> ConnectAsync(Uri uri, RosenBridgeClientOptions options,
        ITransportFactory transport, CancellationToken token)
    {
        EndpointPolicy.Validate(uri, server: false, options.AllowInsecureLoopback);
        EndpointPolicy.PositiveTimeout(options.OpenTimeout, nameof(options.OpenTimeout));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxPendingRequests);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(options.OpenTimeout);
        var addresses = await Dns.GetHostAddressesAsync(uri.DnsSafeHost, deadline.Token).ConfigureAwait(false);
        ITransportConnection? connection = null;
        Exception? lastError = null;
        foreach (var address in addresses.OrderBy(a => a.AddressFamily == AddressFamily.InterNetwork ? 0 : 1))
        {
            EndpointPolicy.CheckAddress(uri, address);
            try
            {
                connection = await transport.ConnectAsync(new(address, uri.Port), deadline.Token).ConfigureAwait(false);
                break;
            }
            catch (SocketException error) { lastError = error; }
        }
        if (connection is null) throw new IOException("Unable to connect to server.", lastError);
        var endpoint = (IPEndPoint)connection.RemoteEndPoint;
        async ValueTask<ITransportConnection> Connect(CancellationToken ct)
        {
            var next = await transport.ConnectAsync(endpoint, ct).ConfigureAwait(false);
            try { return await TlsConnection.ClientAsync(next, uri, options, ct).ConfigureAwait(false); }
            catch { await next.DisposeAsync().ConfigureAwait(false); throw; }
        }
        try
        {
            connection = await TlsConnection.ClientAsync(connection, uri, options, deadline.Token).ConfigureAwait(false);
            return await EstablishAsync(connection, Connect, options, deadline.Token).ConfigureAwait(false);
        }
        catch { await connection.DisposeAsync().ConfigureAwait(false); throw; }
    }

    internal static async Task<RosenBridgeClient> ConnectAsync(
        Func<CancellationToken, ValueTask<ITransportConnection>> connect, RosenBridgeClientOptions options, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(connect);
        EndpointPolicy.PositiveTimeout(options.OpenTimeout, nameof(options.OpenTimeout));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxPendingRequests);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(options.OpenTimeout);
        var connection = await connect(deadline.Token).ConfigureAwait(false);
        try { return await EstablishAsync(connection, connect, options, deadline.Token).ConfigureAwait(false); }
        catch { await connection.DisposeAsync().ConfigureAwait(false); throw; }
    }

    private static async Task<RosenBridgeClient> EstablishAsync(ITransportConnection connection,
        Func<CancellationToken, ValueTask<ITransportConnection>> connect, RosenBridgeClientOptions options, CancellationToken token)
    {
        await ManagementWire.WriteAsync(connection.Stream,
            new() { Type = "control", Credential = options.Credential }, token).ConfigureAwait(false);
        var ready = await ManagementWire.ReadAsync(connection.Stream, token).ConfigureAwait(false);
        if (ready.Type == "reject") throw new RosenBridgeException(ready.Code ?? "rejected");
        if (ready.Type != "ready" || string.IsNullOrEmpty(ready.Session))
            throw new InvalidDataException("Expected session readiness.");
        var client = new RosenBridgeClient(connect, options, connection, ready.Session);
        client._reader = Task.Run(client.ReceiveAsync);
        return client;
    }

    /// <summary>Requests a ticket, opens a new connection, and returns a ready, caller-owned channel.</summary>
    /// <remarks>The token covers acquisition; session closure or channel disposal ends the acquired channel.</remarks>
    public async Task<Channel> RequestChannelAsync(string path, CancellationToken cancellationToken = default)
    {
        EndpointPolicy.ValidatePath(path);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_closed, this);
            if (!_pendingSlots.Wait(0, cancellationToken)) throw new RosenBridgeException("busy");
            if (_acquisitions++ == 0)
                _acquisitionsDrained = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        long id = Interlocked.Increment(ref _nextId);
        var result = new TaskCompletionSource<ManagementMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        ITransportConnection? connection = null;
        bool succeeded = false;
        bool requested = false;
        try
        {
            if (id <= 0) throw new InvalidOperationException("Request identifiers exhausted.");
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
            deadline.CancelAfter(_options.OpenTimeout);
            _pending.TryAdd(id, result);
            deadline.Token.ThrowIfCancellationRequested();
            requested = true;
            await SendControlAsync(new() { Type = "open", Id = id, Path = path }).ConfigureAwait(false);
            var grant = await result.Task.WaitAsync(deadline.Token).ConfigureAwait(false);
            if (grant.Type == "reject") throw new RosenBridgeException(grant.Code ?? "rejected");
            if (grant.Type != "grant" || string.IsNullOrEmpty(grant.Ticket))
                throw new InvalidDataException("Expected connection grant.");
            connection = await _connect(deadline.Token).ConfigureAwait(false);
            await ManagementWire.WriteAsync(connection.Stream,
                new() { Type = "bind", Session = SessionId, Id = id, Ticket = grant.Ticket }, deadline.Token).ConfigureAwait(false);
            var ready = await ManagementWire.ReadAsync(connection.Stream, deadline.Token).ConfigureAwait(false);
            if (ready.Type == "reject") throw new RosenBridgeException(ready.Code ?? "rejected");
            if (ready.Type != "bound" || ready.Id != id)
                throw new InvalidDataException("Expected matching connection readiness.");
            deadline.Token.ThrowIfCancellationRequested();
            lock (_gate)
            {
                ObjectDisposedException.ThrowIf(_closed, this);
                var channel = new Channel(path, connection, cancellationToken: _lifetime.Token);
                channel.OnClose(() => { lock (_gate) { _channels.Remove(id); } });
                _channels.Add(id, channel);
                succeeded = true;
                return channel;
            }
        }
        finally
        {
            _pending.TryRemove(id, out _);
            try
            {
                if (!succeeded)
                {
                    if (connection is not null) await connection.DisposeAsync().ConfigureAwait(false);
                    if (requested && !_lifetime.IsCancellationRequested)
                    {
                        try { await SendControlAsync(new() { Type = "cancel", Id = id }).ConfigureAwait(false); }
                        catch { /* Session shutdown releases outstanding reservations. */ }
                    }
                }
            }
            finally
            {
                _pendingSlots.Release();
                lock (_gate) { if (--_acquisitions == 0) _acquisitionsDrained.TrySetResult(); }
            }
        }
    }

    private async Task SendControlAsync(ManagementMessage message)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
        timeout.CancelAfter(_options.OpenTimeout);
        await _writer.WaitAsync(timeout.Token).ConfigureAwait(false);
        try { await ManagementWire.WriteAsync(_control.Stream, message, timeout.Token).ConfigureAwait(false); }
        catch
        {
            // A cancelled partial write invalidates the management stream.
            await _lifetime.CancelAsync().ConfigureAwait(false);
            throw;
        }
        finally { _writer.Release(); }
    }

    private async Task ReceiveAsync()
    {
        Exception reason = new IOException("Management session closed.");
        try
        {
            while (true)
            {
                var message = await ManagementWire.ReadAsync(_control.Stream, _lifetime.Token).ConfigureAwait(false);
                if (message.Type is not ("grant" or "reject") || message.Id <= 0)
                    throw new InvalidDataException("Unexpected management response.");
                if (_pending.TryGetValue(message.Id, out var waiter)) waiter.TrySetResult(message);
            }
        }
        catch (Exception error) { reason = error; }
        finally
        {
            Channel[] channels;
            lock (_gate) { _closed = true; channels = _channels.Values.ToArray(); }
            foreach (var waiter in _pending.Values) waiter.TrySetException(reason);
            await _lifetime.CancelAsync().ConfigureAwait(false);
            await _control.DisposeAsync().ConfigureAwait(false);
            await Task.WhenAll(channels.Select(channel => channel.DisposeAsync().AsTask())).ConfigureAwait(false);
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_gate) { return new(_dispose ??= Task.Run(DisposeCoreAsync)); }
    }

    private async Task DisposeCoreAsync()
    {
        Task acquisitions;
        lock (_gate)
        {
            _closed = true;
            acquisitions = _acquisitions == 0 ? Task.CompletedTask : _acquisitionsDrained.Task;
        }
        await _lifetime.CancelAsync().ConfigureAwait(false);
        await _control.DisposeAsync().ConfigureAwait(false);
        await _reader.ConfigureAwait(false);
        await acquisitions.ConfigureAwait(false);
        _lifetime.Dispose();
        _writer.Dispose();
        _pendingSlots.Dispose();
    }
}

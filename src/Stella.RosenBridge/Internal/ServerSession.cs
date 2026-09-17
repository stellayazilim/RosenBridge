using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace Stella.RosenBridge.Internal;

internal sealed class ServerSession : IDisposable
{
    private readonly object _gate = new();
    private readonly Dictionary<long, Reservation> _reservations = new();
    private readonly RosenBridgeServerOptions _options;
    private readonly CancellationTokenSource _lifetime;
    private bool _closed;
    internal string Id { get; } = Secret();
    internal RosenBridgeSession Context { get; }
    internal CancellationToken Token { get; }

    internal ServerSession(RosenBridgeServerOptions options, CancellationToken token)
    {
        _options = options;
        _lifetime = CancellationTokenSource.CreateLinkedTokenSource(token);
        Token = _lifetime.Token;
        Context = new RosenBridgeSession(Id, Token, Dispose);
    }

    internal Reservation? Reserve(long id, string path)
    {
        lock (_gate)
        {
            PruneExpired();
            if (_closed || _reservations.Count >= _options.MaxConnectionsPerSession || _reservations.ContainsKey(id))
                return null;
            var reservation = new Reservation(id, path, Token);
            _reservations.Add(id, reservation);
            return reservation;
        }
    }

    internal Reservation? Bind(long id, string? ticket)
    {
        lock (_gate)
        {
            PruneExpired();
            if (_closed || ticket is null || !_reservations.TryGetValue(id, out var reservation) ||
                reservation.Bound || !CryptographicOperations.FixedTimeEquals(
                    Encoding.UTF8.GetBytes(reservation.Ticket), Encoding.UTF8.GetBytes(ticket)))
                return null;
            reservation.Bound = true;
            return reservation;
        }
    }

    internal void Cancel(long id)
    {
        Reservation? reservation;
        lock (_gate)
        {
            if (!_reservations.TryGetValue(id, out reservation)) return;
            if (!reservation.Bound) _reservations.Remove(id);
        }
        reservation.Cancel();
        if (!reservation.Bound) reservation.Dispose();
    }

    internal void Release(Reservation reservation)
    {
        lock (_gate)
        {
            if (_reservations.TryGetValue(reservation.Id, out var current) && ReferenceEquals(current, reservation))
                _reservations.Remove(reservation.Id);
        }
        reservation.Dispose();
    }

    private void PruneExpired()
    {
        // Expiration is authoritative at reserve/bind; no timer task per ticket is needed.
        foreach (var reservation in _reservations.Values.Where(r => !r.Bound &&
                     Stopwatch.GetElapsedTime(r.Created) >= _options.TicketLifetime).ToArray())
        {
            _reservations.Remove(reservation.Id);
            reservation.Dispose();
        }
    }

    public void Dispose()
    {
        Reservation[] reservations;
        lock (_gate)
        {
            if (_closed) return;
            _closed = true;
            reservations = _reservations.Values.ToArray();
            _reservations.Clear();
        }
        _lifetime.Cancel();
        foreach (var reservation in reservations)
        {
            reservation.Cancel();
            if (!reservation.Bound) reservation.Dispose();
        }
        _lifetime.Dispose();
    }

    private static string Secret() => Convert.ToBase64String(RandomNumberGenerator.GetBytes(32))
        .TrimEnd('=').Replace('+', '-').Replace('/', '_');

    internal sealed class Reservation : IDisposable
    {
        private readonly CancellationTokenSource _cancellation;
        private readonly object _gate = new();
        private bool _disposed;
        internal long Id { get; }
        internal string Path { get; }
        internal string Ticket { get; } = Secret();
        internal long Created { get; } = Stopwatch.GetTimestamp();
        internal bool Bound { get; set; }
        internal CancellationToken Token { get; }

        internal Reservation(long id, string path, CancellationToken token)
        {
            Id = id;
            Path = path;
            _cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
            Token = _cancellation.Token;
        }

        internal void Cancel()
        {
            lock (_gate) { if (!_disposed) _cancellation.Cancel(); }
        }

        public void Dispose()
        {
            lock (_gate)
            {
                if (_disposed) return;
                _disposed = true;
                _cancellation.Dispose();
            }
        }
    }
}

using System.Text.Json;
using Stella.RosenBridge.Channels.Internal;
using Stella.RosenBridge.Transport;

namespace Stella.RosenBridge.Channels;

/// <summary>
/// Owns one connection with independently finishing incoming and outgoing payloads.
/// Configure observers before starting I/O. Source and destination streams remain caller-owned.
/// </summary>
public sealed class Channel : IAsyncDisposable
{
    public const int MaxWriteBytes = ChannelFraming.MaxDataBytes;
    private readonly object _gate = new();
    private readonly ITransportConnection _connection;
    private readonly int _capacity;
    private readonly byte[] _outgoingHeader;
    private readonly CancellationTokenSource _lifetime;
    private readonly CancellationToken _token;
    private readonly Queue<byte[]> _queue = new();
    private readonly SemaphoreSlim _available = new(0);
    private readonly TaskCompletionSource _consumerReady = NewSignal();
    private readonly TaskCompletionSource _completion = NewSignal();
    private TaskCompletionSource _space = NewSignal();
    private Task? _run;
    private int _buffered;
    private int _drainThreshold;
    private bool _writesCompleted;
    private bool _producerSelected;
    private bool _manualWriteInProgress;
    private TaskCompletionSource? _manualWriteCompleted;
    private bool _disposed;
    private Exception? _failure;
    private Func<CancellationToken, Task>? _producer;
    private Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask>? _consumer;
    private Action<ChannelHeader>? _onHeader;
    private Action? _onDrain;
    private Action? _onEnd;
    private Action? _onFinish;
    private Action<Exception>? _onError;
    private Action? _onClose;

    public Channel(string path, ITransportConnection connection,
        ChannelOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(connection);
        options ??= new();
        ArgumentOutOfRangeException.ThrowIfLessThan(options.WriteBufferBytes, MaxWriteBytes);
        ArgumentNullException.ThrowIfNull(options.Header);
        _outgoingHeader = JsonSerializer.SerializeToUtf8Bytes(options.Header, ChannelJsonContext.Default.ChannelHeader);
        if (_outgoingHeader.Length > ChannelFraming.MaxHeaderBytes)
            throw new ArgumentException("Header exceeds the size limit.", nameof(options));
        Path = path;
        _connection = connection;
        _capacity = options.WriteBufferBytes;
        _lifetime = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _token = _lifetime.Token;
    }

    public string Path { get; }

    /// <summary>The shared server session; null for client-side and directly constructed channels.</summary>
    public RosenBridgeSession? Session { get; internal set; }

    /// <summary>Completes after both payload directions and connection cleanup. Starts I/O.</summary>
    public Task Completion { get { Start(); return _completion.Task; } }

    public Channel OnHeader(Action<ChannelHeader> callback) => Configure(() => _onHeader += callback, callback);
    public Channel OnDrain(Action callback) => Configure(() => _onDrain += callback, callback);
    public Channel OnEnd(Action callback) => Configure(() => _onEnd += callback, callback);
    public Channel OnFinish(Action callback) => Configure(() => _onFinish += callback, callback);
    public Channel OnError(Action<Exception> callback) => Configure(() => _onError += callback, callback);
    public Channel OnClose(Action callback) => Configure(() => _onClose += callback, callback);

    /// <summary>Attaches the sole incoming consumer. Data is valid only until its callback returns.</summary>
    public Channel OnData(Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask> callback)
    {
        ArgumentNullException.ThrowIfNull(callback);
        lock (_gate)
        {
            ThrowIfUnavailable();
            if (_consumer is not null)
                throw new InvalidOperationException("Choose one incoming consumer: OnData or ReadPipe.");
            _consumer = callback;
            _consumerReady.TrySetResult();
        }
        return this;
    }

    /// <summary>Starts copying source to the outgoing payload and returns this channel immediately.</summary>
    public Channel WritePipe(Stream source)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (!source.CanRead) throw new ArgumentException("Source must be readable.", nameof(source));
        return SelectProducer(async token =>
        {
            var buffer = new byte[MaxWriteBytes];
            int count;
            while ((count = await source.ReadAsync(buffer, token).ConfigureAwait(false)) != 0)
                await EnqueueAsync(buffer.AsMemory(0, count), token).ConfigureAwait(false);
        });
    }

    /// <summary>The source memory must remain unchanged until Completion.</summary>
    public Channel Send(ReadOnlyMemory<byte> source) => SelectProducer(async token =>
    {
        while (!source.IsEmpty)
        {
            var count = Math.Min(source.Length, MaxWriteBytes);
            await EnqueueAsync(source[..count], token).ConfigureAwait(false);
            source = source[count..];
        }
    });

    public Channel Send(IAsyncEnumerable<ReadOnlyMemory<byte>> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        return SelectProducer(async token =>
        {
            await foreach (var chunk in source.WithCancellation(token).ConfigureAwait(false))
            {
                var remaining = chunk;
                while (!remaining.IsEmpty)
                {
                    var count = Math.Min(remaining.Length, MaxWriteBytes);
                    await EnqueueAsync(remaining[..count], token).ConfigureAwait(false);
                    remaining = remaining[count..];
                }
            }
        });
    }

    /// <summary>Copies incoming bytes to destination and awaits both directions and cleanup.</summary>
    public Task ReadPipe(Stream destination)
    {
        ArgumentNullException.ThrowIfNull(destination);
        if (!destination.CanWrite) throw new ArgumentException("Destination must be writable.", nameof(destination));
        OnData((data, token) => destination.WriteAsync(data, token));
        return Completion;
    }

    /// <summary>
    /// Copies up to MaxWriteBytes into the bounded queue. False means no bytes were accepted.
    /// After a capacity rejection OnDrain reports enough space for the rejected write size.
    /// </summary>
    public bool TryWrite(ReadOnlyMemory<byte> data)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(data.Length, MaxWriteBytes);
        bool accepted;
        lock (_gate)
        {
            ThrowIfUnavailable();
            if (_producerSelected) throw new InvalidOperationException("A source producer already owns outgoing writes.");
            if (_manualWriteInProgress) throw new InvalidOperationException("Await the current manual write first.");
            if (_writesCompleted) throw new InvalidOperationException("Outgoing payload has ended.");
            accepted = TryEnqueue(data);
            if (!accepted) _drainThreshold = Math.Max(_drainThreshold, data.Length);
            StartLocked();
        }
        return accepted;
    }

    /// <summary>Copies manual outgoing bytes, waiting for buffer capacity. Await before writing again or ending.</summary>
    public async ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            ThrowIfUnavailable();
            if (_producerSelected || _writesCompleted || _manualWriteInProgress)
                throw new InvalidOperationException("Outgoing writes are owned, ended, or already in progress.");
            _manualWriteInProgress = true;
            _manualWriteCompleted = NewSignal();
            StartLocked();
        }
        try
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(_token, cancellationToken);
            linked.Token.ThrowIfCancellationRequested();
            while (!data.IsEmpty)
            {
                var count = Math.Min(data.Length, MaxWriteBytes);
                await EnqueueAsync(data[..count], linked.Token).ConfigureAwait(false);
                data = data[count..];
            }
        }
        catch
        {
            // A cancelled write may already have queued a prefix of its payload.
            // End this operation rather than allowing a silently truncated successful stream.
            await _lifetime.CancelAsync().ConfigureAwait(false);
            throw;
        }
        finally
        {
            lock (_gate)
            {
                _manualWriteInProgress = false;
                _manualWriteCompleted!.TrySetResult();
            }
        }
    }

    /// <summary>Queues END after manual writes. Incoming data remains readable.</summary>
    public Channel CompleteWrites()
    {
        lock (_gate)
        {
            ThrowIfUnavailable();
            if (_producerSelected) throw new InvalidOperationException("The source producer sends END automatically.");
            if (_manualWriteInProgress) throw new InvalidOperationException("Await the current manual write before ending.");
            EndWritesLocked();
            StartLocked();
        }
        return this;
    }

    private Channel Configure(Action configure, Delegate callback)
    {
        ArgumentNullException.ThrowIfNull(callback);
        lock (_gate)
        {
            ThrowIfUnavailable();
            if (_run is not null) throw new InvalidOperationException("Register observers before starting I/O.");
            configure();
        }
        return this;
    }

    private Channel SelectProducer(Func<CancellationToken, Task> producer)
    {
        lock (_gate)
        {
            ThrowIfUnavailable();
            if (_run is not null || _producerSelected || _writesCompleted)
                throw new InvalidOperationException("Select one outgoing source before starting I/O.");
            _producerSelected = true;
            _producer = producer;
            StartLocked();
        }
        return this;
    }

    private void Start()
    {
        lock (_gate)
        {
            // Completion remains observable after cleanup/disposal.
            if (_run is not null) return;
            ThrowIfUnavailable();
            StartLocked();
        }
    }

    private void StartLocked() => _run ??= Task.Run(RunAsync);

    private async Task RunAsync()
    {
        try
        {
            await Task.WhenAll(GuardAsync(ReadAsync), GuardAsync(WriteAsync), GuardAsync(ProduceAsync))
                .ConfigureAwait(false);
        }
        finally
        {
            try { await _connection.DisposeAsync().ConfigureAwait(false); }
            catch (Exception error) { RecordFailure(error); }
            lock (_gate) { _queue.Clear(); _buffered = 0; }

            if (_failure is not null)
            {
                try { _onError?.Invoke(_failure); }
                catch { /* Preserve the original operation error. */ }
            }
            try { _onClose?.Invoke(); }
            catch (Exception error) { RecordFailure(error); }

            if (_failure is not null) _completion.TrySetException(_failure);
            else if (_token.IsCancellationRequested) _completion.TrySetCanceled(_token);
            else _completion.TrySetResult();
        }
    }

    private async Task GuardAsync(Func<Task> operation)
    {
        try { await operation().ConfigureAwait(false); }
        catch (OperationCanceledException) when (_token.IsCancellationRequested) { }
        catch (Exception error) { RecordFailure(error); }
        finally
        {
            if (_failure is not null || _token.IsCancellationRequested)
            {
                try { await _lifetime.CancelAsync().ConfigureAwait(false); }
                catch (Exception error) { RecordFailure(error); }
            }
        }
    }

    private void RecordFailure(Exception error)
    {
        lock (_gate) { _failure ??= error; }
    }

    private async Task ProduceAsync()
    {
        if (_producer is null) return;
        await _producer(_token).ConfigureAwait(false);
        lock (_gate) { EndWritesLocked(); }
    }

    private async Task EnqueueAsync(ReadOnlyMemory<byte> data, CancellationToken token)
    {
        while (true)
        {
            Task wait;
            lock (_gate)
            {
                token.ThrowIfCancellationRequested();
                if (TryEnqueue(data)) return;
                wait = _space.Task;
            }
            await wait.WaitAsync(token).ConfigureAwait(false);
        }
    }

    private bool TryEnqueue(ReadOnlyMemory<byte> data)
    {
        if (data.IsEmpty) return true;
        if (data.Length > _capacity - _buffered) return false;
        _queue.Enqueue(data.ToArray());
        _buffered += data.Length;
        _available.Release();
        return true;
    }

    private void EndWritesLocked()
    {
        if (_writesCompleted) return;
        _writesCompleted = true;
        _drainThreshold = 0;
        _available.Release();
    }

    private async Task WriteAsync()
    {
        var header = new byte[5];
        await ChannelFraming.WriteAsync(_connection.Stream, header, ChannelFraming.Header, _outgoingHeader, _token)
            .ConfigureAwait(false);
        while (true)
        {
            await _available.WaitAsync(_token).ConfigureAwait(false);
            byte[]? payload;
            lock (_gate)
            {
                payload = _queue.Count != 0 ? _queue.Dequeue() : null;
                if (payload is null && !_writesCompleted) continue;
            }
            if (payload is null) break;
            await ChannelFraming.WriteAsync(_connection.Stream, header, ChannelFraming.Data, payload, _token)
                .ConfigureAwait(false);
            bool notifyDrain;
            lock (_gate)
            {
                _buffered -= payload.Length;
                var signal = _space;
                _space = NewSignal();
                signal.TrySetResult();
                notifyDrain = _drainThreshold > 0 && _capacity - _buffered >= _drainThreshold;
                if (notifyDrain) _drainThreshold = 0;
            }
            if (notifyDrain) _onDrain?.Invoke();
        }
        await ChannelFraming.WriteAsync(_connection.Stream, header, ChannelFraming.End, ReadOnlyMemory<byte>.Empty, _token)
            .ConfigureAwait(false);
        _onFinish?.Invoke();
    }

    private async Task ReadAsync()
    {
        var header = new byte[5];
        var first = await ChannelFraming.ReadAsync(_connection.Stream, header, _token).ConfigureAwait(false);
        if (first.Flags != ChannelFraming.Header)
            throw new InvalidDataException("Expected channel metadata before payload.");
        var metadata = new byte[first.Length];
        await _connection.Stream.ReadExactlyAsync(metadata, _token).ConfigureAwait(false);
        var parsed = JsonSerializer.Deserialize(metadata, ChannelJsonContext.Default.ChannelHeader)
            ?? throw new InvalidDataException("Missing channel metadata.");
        _onHeader?.Invoke(parsed);

        var buffer = new byte[MaxWriteBytes];
        while (true)
        {
            var frame = await ChannelFraming.ReadAsync(_connection.Stream, header, _token).ConfigureAwait(false);
            if (frame.Flags == ChannelFraming.Header)
                throw new InvalidDataException("Duplicate channel metadata.");
            var remaining = frame.Length;
            if (remaining > 0) await _consumerReady.Task.WaitAsync(_token).ConfigureAwait(false);
            while (remaining > 0)
            {
                var read = await _connection.Stream.ReadAsync(buffer.AsMemory(0, remaining), _token).ConfigureAwait(false);
                if (read == 0) throw new EndOfStreamException("Channel payload was truncated.");
                await _consumer!(buffer.AsMemory(0, read), _token).ConfigureAwait(false);
                remaining -= read;
            }
            if (frame.Flags == ChannelFraming.End) break;
        }
        _onEnd?.Invoke();
    }

    private void ThrowIfUnavailable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _token.ThrowIfCancellationRequested();
        if (_completion.Task.IsCompleted) throw new InvalidOperationException("Channel has completed.");
    }

    public async ValueTask DisposeAsync()
    {
        Task run;
        Task manualWrite;
        bool first;
        lock (_gate)
        {
            first = !_disposed;
            _disposed = true;
            StartLocked();
            run = _run!;
            manualWrite = _manualWriteCompleted?.Task ?? Task.CompletedTask;
        }
        if (first)
        {
            try { await _lifetime.CancelAsync().ConfigureAwait(false); }
            catch (Exception error) { RecordFailure(error); }
        }
        await run.ConfigureAwait(false);
        await manualWrite.ConfigureAwait(false);
        if (first)
        {
            _lifetime.Dispose();
            _available.Dispose();
        }
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}

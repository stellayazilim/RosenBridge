using System.Net;
using System.Runtime.CompilerServices;
using Stella.RosenBridge.Channels;
using Stella.RosenBridge.Transport;
using Stella.RosenBridge.Transport.Tcp;

internal static class ChannelSmokeTests
{
    internal static async Task EarlyResponseAsync(CancellationToken token)
    {
        var (clientConnection, serverConnection) = await ConnectAsync(token);
        await using var client = new Channel("/files", clientConnection, cancellationToken: token);
        await using var server = new Channel("/files", serverConnection,
            new ChannelOptions { Header = new("application/octet-stream") }, token);
        var responseArrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = new List<byte>();
        var events = new List<string>();
        int finish = 0, close = 0;
        client.OnHeader(header =>
            {
                Check(header.ContentType == "application/octet-stream", "Response header missing.");
                events.Add("header");
            })
            .OnData((bytes, _) =>
            {
                Check(bytes.Span.SequenceEqual(new byte[] { 8 }), "Unexpected early response.");
                events.Add("data");
                responseArrived.TrySetResult();
                return ValueTask.CompletedTask;
            })
            .OnEnd(() => events.Add("end"))
            .OnFinish(() => Interlocked.Increment(ref finish))
            .OnClose(() => Interlocked.Increment(ref close));

        server.OnData((bytes, _) =>
            {
                received.AddRange(bytes.ToArray());
                if (received.Count == 1)
                    Check(server.TryWrite(new byte[] { 8 }), "Small response should fit.");
                return ValueTask.CompletedTask;
            })
            .OnEnd(() => server.CompleteWrites());

        client.Send(ProduceAsync(responseArrived.Task, token));
        await Task.WhenAll(client.Completion, server.Completion).WaitAsync(token);
        Check(received.SequenceEqual(new byte[] { 1, 2 }), "Request was not fully consumed.");
        Check(events.SequenceEqual(new[] { "header", "data", "end" }), "Incoming event ordering failed.");
        Check(finish == 1 && close == 1, "Terminal events must fire exactly once.");
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> ProduceAsync(
        Task response, [EnumeratorCancellation] CancellationToken token)
    {
        yield return new byte[] { 1 };
        // The request cannot finish until the response is processed.
        await response.WaitAsync(token);
        yield return new byte[] { 2 };
    }

    internal static async Task PipesAsync(CancellationToken token)
    {
        var (clientConnection, serverConnection) = await ConnectAsync(token);
        await using var client = new Channel("/files", clientConnection, cancellationToken: token);
        await using var server = new Channel("/files", serverConnection, cancellationToken: token);
        var bytes = Enumerable.Range(0, 300_000).Select(i => (byte)i).ToArray();
        using var source = new MemoryStream(bytes);
        using var received = new MemoryStream();
        using var response = new MemoryStream();
        Check(ReferenceEquals(client.WritePipe(source), client), "WritePipe must return this.");
        server.Send(new byte[] { 4, 5, 6 });
        await Task.WhenAll(client.ReadPipe(response), server.ReadPipe(received)).WaitAsync(token);
        Check(received.ToArray().AsSpan().SequenceEqual(bytes), "Pipe payload differs.");
        Check(response.ToArray().AsSpan().SequenceEqual(new byte[] { 4, 5, 6 }), "Response differs.");
        await client.DisposeAsync();
        Check(source.CanRead && response.CanWrite && received.CanWrite, "Caller streams must remain open.");
    }

    internal static async Task DrainAsync(CancellationToken token)
    {
        var (clientConnection, serverConnection) = await ConnectAsync(token);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var client = new Channel("/files", new GatedConnection(clientConnection, gate.Task),
            new ChannelOptions { WriteBufferBytes = Channel.MaxWriteBytes }, token);
        await using var server = new Channel("/files", serverConnection, cancellationToken: token);
        using var received = new MemoryStream();
        var bytes = Enumerable.Repeat((byte)17, Channel.MaxWriteBytes).ToArray();
        int drains = 0;
        client.OnDrain(() =>
            {
                Interlocked.Increment(ref drains);
                Check(client.TryWrite(new byte[] { 99 }), "Retry after drain should fit.");
                client.CompleteWrites();
            })
            .OnData((_, _) => ValueTask.CompletedTask);
        Check(client.TryWrite(bytes), "Initial write should fit.");
        Check(!client.TryWrite(new byte[] { 99 }), "Full buffer must reject without consuming data.");
        Array.Fill(bytes, (byte)0); // TryWrite owns a copy.
        server.CompleteWrites();
        var reading = server.ReadPipe(received);
        gate.TrySetResult();
        await Task.WhenAll(client.Completion, reading).WaitAsync(token);
        var actual = received.ToArray();
        Check(drains == 1, "Expected exactly one capacity-recovery event.");
        Check(actual.Length == Channel.MaxWriteBytes + 1 && actual[^1] == 99 &&
            actual.AsSpan(0, Channel.MaxWriteBytes).IndexOfAnyExcept((byte)17) == -1,
            "TryWrite ownership or rejection semantics failed.");
    }

    internal static async Task MalformedAsync(CancellationToken token)
    {
        var (clientConnection, serverConnection) = await ConnectAsync(token);
        await using var peer = serverConnection;
        await using var client = new Channel("/files", clientConnection, cancellationToken: token);
        int errors = 0, closes = 0;
        client.OnError(_ => errors++).OnClose(() => closes++);
        var completion = client.Completion;
        await peer.Stream.WriteAsync(new byte[] { 255, 0, 0, 0, 0 }, token);
        await ExpectFailure<InvalidDataException>(completion, token);
        Check(errors == 1 && closes == 1, "Failure notifications must be singular.");
    }

    internal static async Task ConsumerFailureAsync(CancellationToken token)
    {
        var (clientConnection, serverConnection) = await ConnectAsync(token);
        await using var client = new Channel("/files", clientConnection, cancellationToken: token);
        await using var server = new Channel("/files", serverConnection, cancellationToken: token);
        var failure = new InvalidOperationException("Consumer failed.");
        Exception? observed = null;
        client.OnData((_, _) => throw failure).OnError(error => observed = error).CompleteWrites();
        server.Send(new byte[] { 1 });
        await ExpectFailure<InvalidOperationException>(client.Completion, token);
        // The failing consumer closes the connection. Depending on whether the peer
        // has already read END, it may finish normally or observe a TCP reset/EOF.
        try { await server.Completion.WaitAsync(token); }
        catch (IOException) { }
        Check(ReferenceEquals(observed, failure), "Consumer error should reach Completion and OnError.");
    }

    internal static async Task DisposalAsync(CancellationToken token)
    {
        var (clientConnection, serverConnection) = await ConnectAsync(token);
        await using var peer = serverConnection;
        await using var client = new Channel("/files", clientConnection, cancellationToken: token);
        int closes = 0;
        client.OnClose(() => closes++);
        var completion = client.Completion;
        await client.DisposeAsync();
        await ExpectFailure<OperationCanceledException>(completion, token);
        Check(closes == 1, "Dispose should close once.");
    }

    internal static async Task ManualWriteAsync(CancellationToken token)
    {
        var (clientConnection, serverConnection) = await ConnectAsync(token);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var client = new Channel("/files", new GatedConnection(clientConnection, gate.Task),
            new ChannelOptions { WriteBufferBytes = Channel.MaxWriteBytes }, token);
        await using var server = new Channel("/files", serverConnection, cancellationToken: token);
        using var output = new MemoryStream();
        var writing = client.WriteAsync(new byte[Channel.MaxWriteBytes * 2], token).AsTask();
        Check(!writing.IsCompleted, "Async write must wait when the queue is full.");
        try { client.CompleteWrites(); throw new Exception("END overtook pending data."); }
        catch (InvalidOperationException) { }
        server.CompleteWrites();
        var reading = server.ReadPipe(output);
        gate.TrySetResult();
        await writing.WaitAsync(token);
        client.CompleteWrites();
        await Task.WhenAll(reading, client.ReadPipe(Stream.Null)).WaitAsync(token);
        Check(output.Length == Channel.MaxWriteBytes * 2, "Manual write lost data.");
    }

    internal static async Task CancelManualWriteAsync(CancellationToken token)
    {
        var (clientConnection, serverConnection) = await ConnectAsync(token);
        await using var peer = serverConnection;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var client = new Channel("/files", new GatedConnection(clientConnection, gate.Task),
            new ChannelOptions { WriteBufferBytes = Channel.MaxWriteBytes }, token);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        var writing = client.WriteAsync(new byte[Channel.MaxWriteBytes * 2], cancellation.Token).AsTask();
        cancellation.Cancel();
        var disposing = client.DisposeAsync().AsTask();
        await ExpectFailure<OperationCanceledException>(writing, token);
        await disposing.WaitAsync(token);
        await ExpectFailure<OperationCanceledException>(client.Completion, token);
    }

    private static async Task ExpectFailure<T>(Task task, CancellationToken token) where T : Exception
    {
        try { await task.WaitAsync(token); }
        catch (T) { return; }
        throw new Exception($"Expected {typeof(T).Name}.");
    }

    private static async Task<(ITransportConnection Client, ITransportConnection Server)> ConnectAsync(CancellationToken token)
    {
        var factory = new TcpTransportFactory();
        await using var listener = factory.Listen(new IPEndPoint(IPAddress.Loopback, 0));
        var client = await factory.ConnectAsync((IPEndPoint)listener.LocalEndPoint, token);
        try { return (client, await listener.AcceptAsync(token)); }
        catch { await client.DisposeAsync(); throw; }
    }

    private static void Check(bool condition, string message)
    {
        if (!condition) throw new Exception(message);
    }

    private sealed class GatedConnection(ITransportConnection inner, Task gate) : ITransportConnection
    {
        public EndPoint LocalEndPoint => inner.LocalEndPoint;
        public EndPoint RemoteEndPoint => inner.RemoteEndPoint;
        public Stream Stream { get; } = new GatedStream(inner.Stream, gate);
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    private sealed class GatedStream(Stream inner, Task gate) : Stream
    {
        public override bool CanRead => true;
        public override bool CanWrite => true;
        public override bool CanSeek => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => inner.ReadAsync(buffer, cancellationToken);
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await gate.WaitAsync(cancellationToken);
            await inner.WriteAsync(buffer, cancellationToken);
        }
    }
}

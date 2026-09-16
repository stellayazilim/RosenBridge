# Channel API — First Working Layer

`Stella.RosenBridge.Channels.Channel` owns the supplied `ITransportConnection`. Applications normally acquire a ready channel through [client.RequestChannelAsync](client-server-api.md), which performs management messaging, endpoint authorization, and ticket binding. Constructing a Channel directly does not perform those checks; its path is only a label.

## Pipe usage

```csharp
// connection: an ITransportConnection obtained from the TCP layer.
await using var channel = new Channel("/files", connection,
    cancellationToken: cancellationToken);

channel
    .OnHeader(header => Console.WriteLine(header.ContentType))
    .OnError(error => Console.Error.WriteLine(error.Message))
    .OnClose(() => Console.WriteLine("Closed"))
    .WritePipe(source);

await channel.ReadPipe(destination);
```

`WritePipe` starts transmission and immediately returns the same Channel. `ReadPipe` copies the incoming direction, then waits for both directions to finish and the connection to be cleaned up. Source/destination streams remain caller-owned; they are neither closed nor automatically flushed.

## Chunk callbacks

```csharp
channel
    .OnHeader(header => { /* metadata */ })
    .OnData(async (data, ct) =>
    {
        await destination.WriteAsync(data, ct);
    })
    .OnEnd(() => { /* incoming payload ended */ })
    .OnFinish(() => { /* local payload and END written */ })
    .OnError(error => { /* operation error */ })
    .OnClose(() => { /* connection cleaned up */ })
    .Send(chunks);

await channel.Completion;
```

`Send` accepts either `ReadOnlyMemory<byte>` or `IAsyncEnumerable<ReadOnlyMemory<byte>>` and returns the same Channel. The byte-memory overload does not create an enumerator; this is not a zero-allocation claim. The memory source must remain unchanged until Completion. Each enumerated chunk is consumed and any required copies are made before the next MoveNext.

## Event and ownership rules

- Register `OnHeader`, `OnDrain`, `OnEnd`, `OnFinish`, `OnError`, and `OnClose` observers before the first I/O operation. Multiple observers are invoked in registration order.
- Select one incoming consumer: `OnData` or `ReadPipe`. The consumer may be attached after sending starts; incoming data is not buffered without a bound. Payload reading waits until a consumer is available.
- Incoming callbacks run sequentially; `OnData` is awaited before the next incoming chunk. Supplied memory is valid only during the callback; copy it to retain it. Read chunks are not frame or application-record boundaries.
- Header/data/end are ordered on the read path. Finish/drain originate on the write path; callbacks from the two directions may run concurrently. Protect shared application state accordingly.
- A callback must not await its own Channel.Completion or DisposeAsync; those operations wait for the callback to return. Long-running work must honor the supplied cancellation token.
- Callback/I/O/source failures cancel the opposite direction and fault Completion. Exceptions from an OnError observer do not replace the original error. Exceptions from an OnClose observer propagate to Completion.
- Normal cancellation cancels Completion; it does not necessarily produce OnError. OnClose runs once after resource cleanup.
- DisposeAsync cancels ongoing work and waits for cleanup. Observe the operation outcome through Completion. Repeated disposal is safe.
- OnEnd marks the incoming payload's end. OnFinish means END was written to the local socket; it does not acknowledge peer consumption or business success. The two events have no relative ordering guarantee.

## Manual writes and drain

`TryWrite(data)` copies accepted bytes; the caller may reuse the source after the call returns. A single call may contain at most `Channel.MaxWriteBytes` (16 KiB). Larger writes throw; use Send/WritePipe for larger sources.

If capacity is unavailable, it returns false without accepting any bytes. OnDrain fires when enough space becomes available for the rejected size. The notification does not reserve capacity; TryWrite must be checked again. Multiple rejections may coalesce into one notification. Notification occurs when the largest rejected size can fit.

A manual producer calls `CompleteWrites()` when finished; END is sent after queued writes. Send/WritePipe send END automatically. Manual writing and a source producer cannot share the same outgoing direction. Send/WritePipe wait for capacity internally and do not require OnDrain.

`WriteAsync(data, ct)` is the capacity-waiting alternative for manual writes; it splits larger inputs into chunks. Source memory can be reused when the call completes. Await it before another write in the same direction or CompleteWrites. Because cancellation may occur after a prefix has been sent, cancelling a write ends the entire channel operation. `OnData((data, ct) => channel.WriteAsync(data, ct))` supports incremental echo/processing.

The default outgoing byte budget is 64 KiB, including in-flight socket writes. The pipe read buffer and incoming buffer are each bounded by an additional 16 KiB. This budget excludes OS socket buffers, the user's source/destination, and higher-level session budgets.

## Experimental framing

The implementation uses a 5-byte header: a 1-byte type/flag followed by a 4-byte unsigned big-endian length.

| Value | Meaning | Upper limit |
|---|---|---|
| 0 | DATA; must not be empty | 16 KiB |
| 1 | END; may also carry the final payload chunk | 16 KiB |
| 2 | HEADER; first frame, UTF-8 JSON ChannelHeader | 8 KiB |

The writer emits HEADER, DATA chunks, then an empty END. JSON source generation is used. The reader does not scan payloads for magic bytes and validates chunk lengths before reading/allocating. Partial frames and EOF before END are errors. Incoming reading stops after END; this first version does not check for additional bytes sent afterward. Peer-consumption ACKs, remote error messages, and full RB wire compatibility are not implemented yet.

This encoding was chosen for local experimentation and is not a fixed/public wire standard. It may evolve as TLS and session management are developed.

## Running

```text
dotnet run --project tests/Stella.RosenBridge.Transport.SmokeTests
```

Runs raw transport and channel tests over real loopback TCP. Channel tests include early responses, pipe ownership, deterministic full-buffer/drain behavior, malformed frames, consumer failures, and disposal/cancellation.

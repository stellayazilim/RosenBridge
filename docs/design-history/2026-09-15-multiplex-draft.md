# Archive — Superseded Channel/Socket Model

This document is retained solely as design history. Its one-socket-per-channel and in-socket multiplexing assumptions are no longer current. Current decisions are in the root spec.md file.

# RosenBridge — TCP Communication Library

Status: v0.1 draft / experimental design. Updated: 2026-09-15.

Settled API direction: all payloads are streams, and request and response may progress concurrently. This behavior is broker-independent. The frame format, numeric codes, and multiplexing details below remain implementation proposals; the stream model does not mandate a headerless socket or a specific framing choice.

## 1. Purpose and architecture

RosenBridge is an independent TCP abstraction library offering server, client, session, channel, message, and stream APIs. It uses the TCP facilities provided by the operating system and language/runtime, managing connection lifetimes, security, concurrent transfers, and resource use at the application level. RB/1 is the version of the application-level framing and control contract that implements these APIs.

```text
Client                                      Server:5501
  ├── Control connection ───────────────────────┤
  ├── Channel /files data connection ───────────┤
  └── Channel /updates data connection ─────────┤
```

A session consists of one control connection and a bounded number of channel data connections. The control connection handles authentication, channel opening, capacity management, and session closure. Each channel uses its own TCP/TLS connection. Message and stream transfers within a channel are multiplexed over the same data connection; both ends can send data.

The server accepts control and data connections through one listening endpoint. By default, the client opens every physical connection. The server can ask the client to open a channel through the control connection. The client need not listen on an externally accessible port.

MUST / MUST NOT indicate mandatory behavior, SHOULD indicates recommended behavior unless an exception is justified, and MAY indicates optional behavior. Numeric defaults are initial policies, not measured performance guarantees. Parser, buffer, and scheduler implementations may be chosen independently while preserving language-independent wire rules.

### 1.1 Transport boundary and TCP standard

Transport follows [TCP — RFC 9293](https://www.rfc-editor.org/rfc/rfc9293.html). TCP connection establishment/closure, segmentation, ordering, retransmission after loss, acknowledgements, receive windows, and congestion control belong to the platform's TCP stack. RosenBridge neither redefines nor implements these; it uses the language/runtime's connect, accept, read, write, and close facilities. TLS likewise belongs to the platform or a selected standard TLS library.

RosenBridge's transport interface is a bidirectional byte stream. Payload bytes are written to that stream; with rbs, the same application bytes travel over TLS. Payloads require no mandatory JSON/Base64 conversion. Control-message encoding is separate from payload encoding.

TCP does not preserve message boundaries: one write may arrive across several reads, and several writes may arrive in one read. In this draft, which multiplexes transfers within a channel, section 7's application framing identifies the transfer each byte belongs to and where messages end. Raw payload here means that a data frame's body is carried unchanged, not that the whole socket stream is headerless.

Authentication, session/channel mapping, payload boundaries, transfer cancellation, and application memory budgets belong to RosenBridge. hello/data-bind perform application startup, not the TCP handshake. window reports application consumption capacity and complete reports application-level completion; they are not TCP window/ACK. This document defines these application behaviors only; transport rules are delegated to the TCP standard above.

## 2. URI and channel address

```text
rbs://server.example:5501
rbs://server.example:5501/files
rbs://server.example:5501/some/other/path
rb://127.0.0.1:5500
```

- The host and mandatory port identify the server endpoint. All physical connections use the same port.
- An empty path or `/` selects only the control session. No data channel is automatically opened for the root.
- `/some/other/path` is one channel address; its segments do not create separate channels.
- For a non-root URI, ConnectAsync opens the session first, then the specified initial channel.
- `rb` uses TCP directly; `rbs` uses TLS immediately over TCP. No RB frame is sent before TLS completes.
- Fragments and userinfo are prohibited. Ports 5500/5501 are examples, not claims of registered ports.
- Channel paths are case-sensitive and must match exactly; Unicode normalization is not performed.
- URI path segments are percent-decoded once using strict UTF-8. Invalid encoding, encoded slash/backslash, literal backslash, and control characters are rejected.
- Empty segments, trailing slashes, `.`, and `..` are rejected. The client validates before silent normalization by the URI library. A decoded path may contain at most 1024 UTF-8 bytes.
- The wire path is a decoded JSON string; the server does not percent-decode it again.
- The same path may be opened as separate channel instances identified by channelId. Each new channel consumes a separate pool slot.

Queries use UTF-8 percent-encoding; `+` is not a space. Duplicate, unknown, or options-conflicting parameters are rejected. Defined options: auth (`none`, `sasl`, `oidc`), authority, client_id, scope, redirect_uri. These are client integration settings, not part of the channel address.

```text
rbs://server.example:5501/files?auth=oidc&authority=https%3A%2F%2Fidentity.example&client_id=desktop&scope=connection.open
```

A credential provider supplies tokens/passwords/secrets; they are not written into the URI. Authority is checked against the application's trusted HTTPS issuer configuration. The RB schemes are private schemes defined by this draft.

## 3. Concepts and identities

| Concept | Lifetime / identity |
|---|---|
| Session | Lifetime of the control connection; random server-generated sessionId |
| Channel request | Session-scoped requestId; clients generate odd and servers even monotonically increasing IDs |
| Channel | Monotonically increasing channelId assigned by the server within the session |
| Transfer | Data-connection-scoped ID; clients generate odd and servers even monotonically increasing IDs |
| Ticket | Short-lived, single-use credential binding a data connection to a session/channel |

Request, channel, and transfer IDs occupy separate identity spaces. Their range is 1–2^63−1; they are not reused within their respective lifetimes. JSON IDs are canonical decimal strings with no sign, whitespace, or leading zeros. sessionId and ticket are unpredictable Base64url strings. Header ID=0 is reserved for connection control.

Closing a session closes every channel. A new session does not inherit old IDs, tickets, or handles.

## 4. Application startup over TCP

```text
Platform TCP connect/accept → [standard TLS] → application startup: hello(role) → authentication or data-bind → READY
```

The platform completes TCP connection establishment according to RFC 9293. The following steps identify the application, role, and resource limits over an established connection. On every socket, the client sends hello as its first RB frame. The role is `control` or `data`, allowing one listener to distinguish the connection types; the TCP handshake does not carry this application information.

Common hello fields in both directions:

```json
{
  "version": 1,
  "role": "control",
  "maxDataBytes": 16384,
  "maxIncomingTransfers": 128,
  "connectionWindow": 1048576,
  "idleTimeoutMs": 60000
}
```

Fields describe the sender's receive capacity; each direction is independent. maxDataBytes ranges from 1024–65536, maxIncomingTransfers from 1–4096, connectionWindow from maxDataBytes to 67108864, and idleTimeoutMs from 10000–300000. The smaller idle proposal is used. On a control connection, transfer fields are not used for capacity accounting; data frames are prohibited.

Missing required fields and incorrect types are rejected. Unknown optional JSON fields are ignored. version must equal 1, and both hellos must have matching roles.

### 4.1 Control connection

The client adds these fields to its control hello:

```json
{
  "maxDataConnections": 8,
  "maxPendingChannelRequests": 64,
  "applicationProtocols": ["raw/1"]
}
```

The server's control hello reports authRequired, mechanisms, the selected applicationProtocol, and effective maxDataConnections/maxPendingChannelRequests. Effective values are the minimum of the client proposal and server policy. Valid ranges are 1–1024 and 0–4096 respectively. A connection with no shared application profile is rejected. The general profile is raw/1.

After SASL completes, the server sends ready:

```json
{"sessionId":"random-session-id","expiresAt":null}
```

expiresAt is a UTC RFC3339 string or null. No channel request may be sent before the session is READY. The control connection does not consume a pool slot but counts toward the server's total socket and TLS/application-startup limits.

### 4.2 Data connection

After receiving a pool reservation, the client opens a socket to the same server endpoint, applies the same security level, and sends a role=data hello. The server replies with a data hello containing its receive limits. The client then sends data-bind:

```json
{"sessionId":"random-session-id","channelId":"1","ticket":"single-use-ticket"}
```

The server validates the active session, reservation, ticket, channelId, and expiry. The ticket is consumed atomically; a second socket cannot bind to the same reservation. The server sends data-ready, then channel-ready on the control connection. Arrival order at the client is not guaranteed across the two sockets.

### 4.2.1 Data-connection application readiness and API readiness

The client runs its data reader from the hello stage onward. After data-ready is validated, the data connection's application state becomes DATA_READY; the reader continues processing subsequent open/data/window/cancel frames without waiting for channel-ready. The server may initiate transfers after its data-ready write completes. TCP/TLS establishment has already completed at this stage.

The client tracks `dataReady` and `managementReady` separately. channel-ready only sets managementReady and finalizes local channel settings; it does not start or stop the data reader. OpenChannelAsync succeeds exactly once only when both signals are present for the same session/channel/request and the operation remains active. If channel-ready arrives first, it waits for dataReady; if data-ready arrives first, data processing continues while waiting for managementReady.

Incoming-transfer callbacks may run before OpenChannelAsync completes. Therefore the client configures receive/admission callbacks before requesting the channel; the callback's channel information may represent a handle still awaiting management approval. Callback scheduling and credit accounting remain bounded between readiness signals. A receive callback must not block the reader or transport progress by waiting for its own channel-opening Task.

A missing second notification is subject to the channel request's remaining deadline; transport traffic does not extend it. Failure, cancellation, timeout, or channel closure is terminal; a late readiness signal cannot revive the operation. The client closes the data socket and notifies cancellation over the control connection; the server releases the reservation/slot through the normal exactly-once cleanup path. Incorrect session/channel/request correlation is a protocol error on the affected socket. Each ready is valid only once on its own socket; duplicate frames are protocol errors.

Tickets contain at least 256 bits of cryptographic randomness, expire after 10 seconds by default, and can bind only to their assigned session/channel reservation. Network use requires rbs. Ticket use with rb is limited to explicitly enabled trusted loopback development mode. Ticket/session secrets are not logged. TLS is never downgraded to plaintext.

A data connection does not initiate another user login; it inherits session identity through its ticket. Data hello does not negotiate a separate application profile; the session profile applies.

## 5. Channel opening and the pool

The server is the sole authority scheduling the session pool. Client- and server-originated requests share the same reservation accounting.

```text
REQUESTED → QUEUED → RESERVED → CONNECTING → OPEN → CLOSING → CLOSED
    └────────────── error / cancellation ────────────────────────┘
```

### 5.1 Client-initiated opening

1. The client sends channel-request: requestId, path, timeoutMs.
2. The server evaluates the path, identity, policy, and capacity.
3. If a slot is available, it reserves it and sends channel-grant: requestId, channelId, ticket, expiresInMs.
4. Otherwise it places the request in a bounded queue and sends channel-queued.
5. After the grant, the client opens and binds the data connection.
6. data-ready and channel-ready make the handle ready for use.

Every path goes through the server's channel-admission policy. Undefined paths return not-found; unauthorized access returns forbidden. channel-reject terminates only the corresponding request.

### 5.2 Server-initiated opening

The server sends channel-offer on the control connection with a server-generated requestId, path, and timeoutMs. The client replies according to local policy with channel-offer-result (`accepted`: boolean, plus code on rejection). Accepted requests join the same server-side pool queue. A rejected or unanswered offer receives no reservation. The client still opens the physical data socket.

Pending records for server offers and local client requests are also bounded; applications may wait for capacity or receive busy. The control connection must not accumulate unlimited offers/requests.

### 5.3 Slot and waiting rules

- A pool slot remains held in RESERVED, CONNECTING, OPEN, and CLOSING.
- Defaults are 8 data connections per session and 64 requests in the server wait queue.
- Requests wait FIFO when the pool is full; expired/cancelled entries are removed.
- A full queue returns channel-reject/busy. Waiter Tasks are not created without a bound; a local API admission limit of 64 is also recommended.
- The slot and server-wide socket/memory reservation are acquired together. If global capacity is unavailable, the request waits without receiving a ticket.
- The server schedules eligible FIFO queues from different sessions round-robin. One session cannot monopolize capacity with waiting records.
- timeoutMs ranges from 1–300000. The client enforces a total local deadline; the server tracks the remaining request lifetime with a monotonic clock. An offer's timeout does not restart after acceptance.
- Ticket lifetime cannot exceed the request's remaining lifetime. Expired reservations are released exactly once.
- channel-request-cancel cancels a pending request; the server replies with channel-reject/cancelled. If grant races with cancellation, the grant is invalidated and any started data socket is closed.
- A terminal request outcome is not repeated. A late grant cannot revive a cancelled request; the client does not use its ticket and reports cancellation.
- Bind failure, connection deadline, or channel closure releases capacity exactly once. Channels are not reopened automatically.

The pool manages bounded connection capacity and lifetimes. In v0.1, a closed channel socket is not rebound to a different path. A channel that remains in use keeps its own socket open.

## 6. Channel lifetime and closure

A channel is bidirectional and may carry multiple transfers. Using a channel does not require a new socket for each message.

Recommended idle policy: a channel with no active transfers or locally pending sends may close after 60 seconds. Ping/pong does not extend idle channel use. The server reports effective channelIdleTimeoutMs in channel-ready (0 disables it; otherwise 1000–300000 ms). Sending through a closed handle produces channel-closed.

Either end may request channel-close (`channelId`, `code`) on the control connection. The initiator immediately stops new transfers; the peer does so on receipt. The server sends channel-closing to move both ends to CLOSING. In-flight new transfer opens receive cancel/channel-closing.

Existing transfers finish within a bounded time or terminate through transfer cancellation. The data connection uses goaway; the drain period is at most 30 seconds. After local closure, the client sends channel-detached on the control connection. The server releases resources when its socket closes and sends channel-closed. A delayed peer notification does not release the slot again. The server may forcibly close the socket at the closure deadline.

Unexpected data EOF/reset terminates only that channel and its transfers. The server reports channel-closed/connection-lost over the control connection. Other channels continue.

Losing the control connection ends the session; both ends close data connections and invalidate tickets/reservations. Heartbeat timeout terminates a half-open control connection. Authentication expiry follows the same session-closure path.

## 7. Application framing: fixed 16-byte header

| Offset | Size | Field | Encoding |
|---|---:|---|---|
| 0 | 2 | Magic | 0x52 0x42 |
| 2 | 1 | Version | 0x01 |
| 3 | 1 | Type | Numeric code below |
| 4 | 8 | TransferID | Unsigned big-endian, at most 2^63−1 |
| 12 | 4 | Length | Unsigned big-endian, body byte count |

Header and body are contiguous, with no padding or line separator. Total length is 16+Length. The body limit is 65536 bytes; data uses the negotiated maxDataBytes, and other frames are limited to 8192 bytes. Transfer end/closed have no body.

```text
Data, ID=1, payload=hello:
52 42 01 12 00 00 00 00 00 00 00 01 00 00 00 05
68 65 6C 6C 6F
```

The reader first obtains the full 16-byte header, validates magic/version/type/ID/state/length, and then reads the exact body length. One read is not assumed to equal one frame. EOF within a frame is truncated-frame. Invalid headers are not recovered by scanning for magic bytes. Limits and overflow are checked before allocation. Native struct layout/endianness is not used; 64-bit IDs must be represented losslessly.

The most significant bit of TransferID must be zero: reject the frame if `(byte & 0x80) != 0` at header[4]. Equivalently, the unsigned value must not exceed `0x7FFFFFFFFFFFFFFF`. Clearing the top bit with `id & 0x7FFFFFFFFFFFFFFF` and continuing is PROHIBITED because it maps different wire IDs to the same valid identity. Convert to a signed representation only after validating the range. Implementations without an unsigned 64-bit type may check the first byte and use a lossless signed or two-part representation. ID=0 and odd/even rules are then validated separately according to frame type and initiating end.

Control bodies are UTF-8 JSON objects without a BOM. Duplicate keys, incorrect types, and JSON deeper than 16 levels are rejected. reason is limited to 256 Unicode characters. Numeric IDs in control fields are decimal strings. Transfer payloads are application-defined byte sequences.

### 7.1 Implementing control JSON

Control messages have fixed schemas determined by frame type; JSON property order is insignificant. Field names are case-sensitive. Receivers validate the types and presence requirements of known fields and may skip unknown optional fields within body/depth limits. Duplicate-key checks also apply to unknown fields. Runtime type names and serializer-specific object metadata are not added to the wire.

Implementations may read JSON directly from UTF-8 bytes and write into bounded buffers. They need not build a DOM, copy the whole body into a string, or use reflection. Language, JSON library, and code-generation technique are not part of the wire contract. Zero allocation is not guaranteed; implementation measurements report allocations per frame, parsing time, and the cost of frequent channel opening/closure.

### 7.2 Frame codes

C: control socket, D: data socket, B: both socket types. Header ID is 0 except for transfer frames.

| Code | Name | Connection | Body |
|---|---|---|---|
| 0x01 | hello | B | Role and receive limits |
| 0x02 | auth-start | C | mechanism, initialResponse |
| 0x03 | auth-challenge | C | data |
| 0x04 | auth-response | C | data |
| 0x05 | auth-ok | C | data or null |
| 0x06 | ready | C | sessionId, expiresAt |
| 0x07 | data-bind | D | sessionId, channelId, ticket |
| 0x08 | data-ready | D | channelId |
| 0x10 | open | D | Transfer metadata; header ID>0 |
| 0x11 | accept | D | window; header ID>0 |
| 0x12 | data | D | Raw bytes; header ID>0 |
| 0x13 | window | D | increment; header ID=0 or transfer ID |
| 0x14 | end | D | Empty; header ID>0 |
| 0x15 | complete | D | bytes (decimal string); header ID>0 |
| 0x16 | cancel | D | code, optional reason; header ID>0 |
| 0x17 | closed | D | Empty; header ID>0 |
| 0x20 | ping | B | nonce: 16 lowercase hex characters |
| 0x21 | pong | B | Same nonce |
| 0x22 | goaway | B | code, drainTimeoutMs |
| 0x23 | error | B | code, optional reason |
| 0x30 | channel-request | C | requestId, path, timeoutMs |
| 0x31 | channel-queued | C | requestId |
| 0x32 | channel-grant | C | requestId, channelId, ticket, expiresInMs |
| 0x33 | channel-reject | C | requestId, code |
| 0x34 | channel-offer | C | requestId, path, timeoutMs |
| 0x35 | channel-offer-result | C | requestId, accepted, code on rejection |
| 0x36 | channel-request-cancel | C | requestId |
| 0x37 | channel-ready | C | requestId, channelId, channelIdleTimeoutMs |
| 0x38 | channel-close | C | channelId, code |
| 0x39 | channel-closing | C | channelId, code |
| 0x3A | channel-detached | C | channelId |
| 0x3B | channel-closed | C | channelId, code |

Unlisted codes are invalid. A frame received in the wrong role/state is a protocol error. Channel management is server-authoritative: queued/grant/reject/ready/closing/closed are sent by the server. offer originates at the server, offer-result at the client. The client sends request; the server places its own offers in its internal scheduler. Local cancellation is internal to the server and uses request-cancel for the client.

## 8. One payload model: streams

All payloads are sent and received as byte streams. Individual messages, byte arrays, and input streams use the same transfer path. An individual message is a finite stream; there is no separate message wire type, mandatory full-buffer reception, or distinct completion rule. Length may be known in advance or determined at the end. Payloads are not converted to another codec.

This behavior operates directly between two RosenBridge ends. It requires no broker, queue, routing, persistent storage, or broker acknowledgement; such features, if present, belong to an upper layer.

### 8.1 Unidirectional payload transfer

The following flow describes one stream direction in the current framing proposal:

```text
open → accept → data* → end → complete
```

data-bind fixes the channel to its socket; transfer open bodies do not repeat channelId or path. Header TransferID is meaningful only on that data connection. The same transfer ID may exist on another channel.

```json
{"kind":"request","contentType":"application/json","totalBytes":"123","replyTo":null,"metadata":{"name":"sample.json"}}
```

In this wire proposal, kind identifies the request/response relationship role; both payloads are streams. metadata is an object, and totalBytes is a canonical decimal string or null. replyTo is null for a request and references the request's transfer ID on the same data connection for a response. Request and response lengths may independently be unknown.

The receiver returns accept/window or cancel/code according to capacity and local callback policy. data/end before accept is prohibited. Data contains at least 1 byte; an empty transfer sends end after accept. A known totalBytes must match exactly; a mismatch cancels the transfer with size-mismatch.

Complete acknowledges the stream's end and the application's consumption of all payload bytes. Complete bytes is the total received byte count. It does not indicate successful business processing or response completion.

Stream WriteAsync reports acceptance into bounded local sending infrastructure; the source memory may be reused once it completes. CompleteWritesAsync closes the local writing direction and waits for end to be sent, not for the peer to consume all data. A separate Completion operation awaits peer complete. This separation avoids making response reading depend on peer consumption. Any message helpers use the same stream API; materializing an entire received payload is an explicitly selected helper with a size limit.

### 8.2 Concurrent request and response

Either end may initiate a request. A request/response operation consists of two independently completing unidirectional payload streams. Once request metadata is accepted, the receiver exposes the request stream to the application without waiting for the entire payload. The application may open and write its response while reading the request. A response may start and finish before request end or complete arrives.

The response recipient may also process bytes incrementally before the whole response arrives. The goal is to overlap sending, application processing, and response consumption to reduce time to the first usable result and buffering requirements; no unmeasured throughput or total-duration guarantee is made. The application payload format must support incremental processing; read-chunk boundaries are not application-record/message boundaries.

```text
Client → Server: request  open ── bytes ── bytes ── bytes ── end
Server → Client: response       open ── bytes ── bytes ── end
```

In the current wire proposal, a response uses its own transfer ID and replyTo identifies the request. Each operation has one response stream; multiple results may be represented inside that stream in an application-defined format. Empty responses are valid. Unknown requests, incorrect direction, responses to responses, and second responses are rejected. Completing a request payload does not remove an operation mapping that is still awaiting its response.

The request-opening API returns an operation handle after accept. The handle exposes a request writer and an independent ResponseAsync waiter. ResponseAsync completes when response metadata and its reader are ready, without waiting for the entire body. The caller can write the request and read the response concurrently. Helpers accepting input streams must also return this handle without waiting for the entire send.

Request and response credits, end notifications, and payload completions are independent. Normal completion of one direction does not close the other or the TCP socket. Operation success requires both directions to complete successfully. An early response end does not automatically cancel the request; stopping the request early requires explicit cancellation.

Operation cancellation or the overall deadline terminates both payload directions and the response waiter. A failure in one direction is reported as the operation result and cancels the open opposite direction. Transfer cancel/closed rules apply to each open direction. The operation remains terminal even if its response has not yet opened; a late response cannot revive it. Terminal/correlation records and response waiters have bounded capacity and finite deadlines.

New request admission must not consume the record/buffer capacity needed to open responses for accepted requests; response capacity is accounted for when admitting the request. The reader and response notifications do not wait for request transmission or the request handler to finish. Applications must not keep writing while deferring reads of incoming data; if both ends do so with bounded buffers, they can deadlock.

## 9. Application backpressure and resource limits

Each data socket has independent connection credits in both directions. Each transfer also has transfer credit. Initial connection credit is peer hello.connectionWindow; initial transfer credit is accept.window. Sending N data bytes requires at least N of both credits, and N is deducted from each when the data is accepted for sending.

window ID=0 increases connection credit; ID>0 increases the corresponding transfer's credit. increment must be positive and the result no greater than 2^31−1. Overspending credit is a connection error. Only data payload bytes count. Control frames do not wait for data credit and have separate limits.

The receiver replenishes credit only when actual capacity is released. Moving data into another unbounded queue is insufficient. Every payload receives a bounded stream buffer, not a totalBytes-sized memory reservation. Credit is returned as the application consumes bytes. A full-buffer reading helper also consumes the stream incrementally and enforces its own total size limit. Request and response buffer/credit accounting are independent.

| Resource | Recommended default |
|---|---:|
| Session data-connection slots | 8 |
| Pending channel requests per session | 64 |
| Local channel-admission waiters | 64 |
| Data frame | 16 KiB |
| Data-connection receive window / direction | 1 MiB |
| Stream window | 64 KiB |
| Optional full-buffer reading helper limit | 1 MiB |
| Data-connection payload reservation | 8 MiB |
| Data-connection outgoing byte queue | 4 MiB |
| Incoming transfers per data connection | 128 |
| Outgoing admission waiters per data connection | 256 |
| Socket control queue | 256 frames / 256 KiB |
| Concurrent receive callbacks per data connection | 32 |

Count and byte limits apply together. SendAsync waits for capacity and returns busy if the waiter limit is reached. TrySend may fail immediately. Cancellation removes the waiting record. Accepted data is never silently dropped.

Host and session-wide memory/socket budgets apply in addition to connection limits. Opening a channel does not grant unlimited memory. The client also enforces its socket/memory limits and cancels grants it cannot accommodate.

### 9.1 Bandwidth

The connection pool limits concurrent resource use. Byte/s limiting is a separate optional policy, for example a token bucket per session or channel. Data writes require both credit and rate-limit tokens. The control connection is separate from the data byte/s budget and has its own flood limits. Rate limiting is disabled by default; when enabled, the burst must accommodate at least one maxDataBytes frame. Local deadlines include this waiting time.

## 10. Readers, writers, and fairness

Each physical socket has one logical reader and writer; dedicated threads are not required. The reader validates headers, receives bodies into allocated buffers, and dispatches by transfer ID. It does not run and await application callbacks. Admission decisions use a separate bounded scheduler; pending opens count toward the incoming-transfer limit.

The writer does not interleave bytes from different frames. Transfer ordering remains open/data/end. Control frames have priority, followed by round-robin service among transfers in the channel that have credit. Recommendation: after 16 control frames, send one data frame if data is ready. Cancellation/closure may receive priority.

Adjacent frames may be combined into one write; batching and read-ahead are bounded by the local byte budget. Completion of a socket write is not peer delivery. Each data socket is a separate ordered TCP stream; byte ordering on one connection is independent of another. Connections share network, CPU, and memory capacity.

## 11. Transfer cancellation and errors

The cancel initiator stops producing new data/end/window frames and removes frames not yet written. The peer does the same and sends closed after any already-started frame write finishes. Until closed arrives, the initiator may read and discard in-flight data, returning released connection credit. If cancellation is simultaneous, both ends send closed and await the other's closed.

Complete/cancel may cross in transit; cancel for a terminal transfer is answered with closed. Late terminal/window frames may be ignored; new data for a terminal ID is an error. An ID that was never opened is an error. Terminal records are cleaned up and IDs are not reused. If the cancellation barrier does not complete within 10 seconds, the affected data socket closes.

| Condition | Result |
|---|---|
| Malformed header, credit violation, invalid frame state | Affected socket closes with error/protocol-error |
| Control-socket error | Session and all channels close |
| Data-socket error | Affected channel closes and its slot is returned |
| Invalid/replayed/expired ticket | Data socket is rejected; the valid session is unaffected |
| Full channel queue | channel-reject/busy |
| Transfer size/capacity problem | cancel/size-mismatch, too-large, or busy |
| Sending to an open channel is unauthorized | cancel/forbidden |
| Transfer deadline/cancellation | cancel/deadline-exceeded or cancelled |

An unverified data-bind cannot cancel another session's reservation. The failed-bind socket closes; the existing reservation ends only through a valid bind, expiry, or authorized cancellation.

## 12. Security

Rbs uses TLS 1.2 or later; TLS 1.3 is preferred. The certificate chain and hostname are validated. Data sockets use the same server identity/validation policy as the control socket. Authentication, tickets, and application data are not sent as TLS early data.

The SASL service name is rosenbridge. The control hello advertises permitted mechanisms; the client selects a shared mechanism according to its own policy. The auth-start/challenge/response/auth-ok exchange is used. Mechanism bytes are standard Base64 JSON strings; null means no initial response, while an empty string means zero bytes. At most 16 challenge rounds and 30 seconds are allowed. The client validates final server data. No common mechanism is an error; there is no automatic downgrade to a weaker mechanism.

SCRAM-SHA-256, OAUTHBEARER, and EXTERNAL providers can be plugged in. TLS provides protection instead of an additional SASL security layer. EXTERNAL may bind to an mTLS identity. Mechanism formats follow their respective standards.

OIDC/OAuth sign-in and token acquisition run over HTTPS in the client integration. The PKCE S256 verifier remains local, the challenge goes in the authorization URL, and the verifier goes to the token endpoint. State and applicable OIDC validations are enforced. User interaction finishes before the connection/application-startup deadline. The access token is presented to RB using OAUTHBEARER. Refresh tokens are not transferred.

The server validates trusted issuer, audience, validity period, and JWT signature/introspection. Session identity applies to all channels, with additional access control per path. The session closes when expiresAt is reached. Tokens, tickets, and authentication bodies are excluded from default logs.

## 13. Timeouts, heartbeat, and shutdown

- Local waiting limit for platform TCP connect and TLS setup: 10 seconds; RB application startup: 30 seconds. These are API deadlines, not TCP retransmission timers.
- Total channel queue/opening timeout: the call's timeoutMs, defaulting to 30 seconds.
- Ticket/bind reservation: at most 10 seconds and no longer than the remaining request lifetime.
- Transfer-open response: 10 seconds; expiry initiates transfer cancellation.
- A started frame: 30 seconds from its first byte; each new byte does not restart the timer.
- Ping: halfway through the outbound idle period. Only one ping is outstanding; pong carries the same nonce.
- Each socket closes if no complete frame arrives from the peer within idleTimeout. Control closure ends the session; data closure ends the channel.
- Initial non-data flood policy: 1000 frames per second per socket, burst 2000; a session-wide limit also applies.

Control goaway stops new channel requests, cancels the queue and unused grants, and drains existing channels. Data goaway drains only its own transfers. drainTimeoutMs is at most 30000; remaining sockets close when it expires. Host shutdown stops the listener first. Normal TLS closure uses close_notify.

## 14. API completion and reconnect

The following names illustrate the API contract:

| Operation | Completion |
|---|---|
| ConnectAsync(rootUri) | Control session READY |
| ConnectAsync(channelUri) | Session READY and initial channel OPEN |
| Session.OpenChannelAsync(path) | dataReady and managementReady exist for the same channel; no failure/cancellation |
| Session.TryOpenChannelAsync(path) | Immediate server capacity decision instead of waiting; may return busy |
| Channel.OpenRequestAsync | Request accept received; request writer and independent response waiter are ready |
| Request.ResponseAsync | Response metadata and stream reader ready; body end is not awaited |
| IncomingRequest.OpenResponseAsync | Response accept received; request end is not awaited |
| Stream.WriteAsync | Acceptance into the bounded local sending pipeline |
| Stream.CompleteWritesAsync | Local writing direction closed and end sent |
| Stream.Completion | Peer complete received for the corresponding payload direction |
| Request.Completion | Request and response payload directions completed successfully, or operation ended with failure/cancellation |
| Channel.CloseAsync | Local data socket closed and server reported channel-closed |
| Session.CloseAsync | Session drain completed or deadline forced closure |

TryOpen adds `wait:false` to channel-request on the wire (default true when absent). If the server cannot reserve a slot immediately, it returns busy without queueing. If ConnectAsync(channelUri) creates a new session but the initial channel fails, it also cleans up that session. OpenChannelAsync failure on an existing session does not close the session.

After connection loss, a transfer without peer complete has an uncertain outcome. Payloads are not replayed automatically. Reconnect is optional: exponential backoff with jitter, recommended starting at 250 ms and capped at 30 seconds. Identity/configuration errors are not retried blindly. Reconnect creates a new session; reopening channels is an explicit application decision.

## 15. Example pool flow

```text
Control: session READY, maxDataConnections=2
Client request /files   → grant #1 → data socket A → OPEN
Server offer /updates  → client accepts → grant #2 → socket B → OPEN
Client request /other  → QUEUED
Socket A closes        → slot released → /other grant #3 → socket C
```

All sockets connect to the same server IP:port; the operating system selects client source ports. The control socket carries no data payload. /updates traffic continues independently of socket A's closure.

## 16. Server and client responsibilities

The server provides the listener, role demultiplexing, TLS/application-startup admission, authentication, session registry, path-admission policy, authoritative pool scheduler, atomic ticket validation, and host resource budget. The global socket limit includes unauthenticated/pending sockets; unowned connections close at a deadline.

The client provides URI/path validation, a credential provider, control connector, server-offer callback, post-grant data connector, and local resource budget. Both ends may share the frame codec, transfer engine, credit accounting, reader/writer, and timeout infrastructure.

Socket/channel closure, cancellation, and timeout release resources exactly once. Callbacks do not block the reader loop. Session, channel, and transfer errors are delivered to the corresponding handles.

## 17. Compatibility and measurement

1. Split the 16-byte header and body at every byte boundary; receive multiple frames in a single read.
2. Shared big-endian hex fixtures; invalid magic/version/type/length and lossless 64-bit IDs.
3. Control/data role selection on one listener and rejection of frames for the wrong role.
4. Root/channel URI distinction, encoding, and prohibited path segments.
5. Pool capacity, FIFO waiting, busy, cancellation, and deadlines.
6. Concurrent client/server requests without granting the same slot twice.
7. Session fairness under global capacity limits and bounded waiting memory.
8. Ticket replay, wrong session/channel, expiry, and competing binds from two sockets.
9. Grant/cancel/bind/ready races; exactly-once slot release.
10. Other channels progress after a data connection fails; control loss cleans up all resources.
11. Idle closure, draining pending transfers, and races with new sends.
12. Credit consumption/replenishment, slow streams, large known/unknown-length payloads, and bounded buffering independent of total payload size.
13. 400 parallel transfers, small/large payloads, and callback/admission limits.
14. TLS/hostname, token audience/expiry, and SASL final-data validation.
15. Control traffic progresses under session rate limits; behavior with rate limiting disabled.
16. Shared wire tests between independent language implementations.
17. channel-ready arrives before or after data-ready: subsequent open/window/cancel frames are processed after data-ready; the opening Task completes exactly once only with both signals and an active operation.
18. Missing/delayed second readiness signal and races between readiness and cancellation/EOF; late signals cannot revive a handle or release a slot twice.
19. Header ID fixtures: 0x7FFFFFFFFFFFFFFF passes range validation; 0x8000000000000000 and 0xFFFFFFFFFFFFFFFF are rejected. 0x8000000000000001 must never become ID=1. Frame-specific parity/state checks are tested separately.
20. Control JSON property reordering, unknown fields, duplicate keys, invalid UTF-8, and size/depth limits; parsing cost and allocation under frequent channel creation/closure.
21. Response opening and receipt of its first byte before request end; both directions progress concurrently with small windows.
22. Early response completion does not truncate the request; early request completion does not lose response correlation. Empty and unknown-length directions are supported.
23. Invalid/duplicate replyTo, second responses, deadline/cancellation while awaiting a response, and late responses; operation resources are cleaned up exactly once.
24. Byte arrays/individual messages and input streams share transfer/completion behavior; two ends work directly without a broker.
25. Accepted requests can open responses while request-admission capacity is exhausted; response reading/notification does not wait for request writing or handler completion.

Measure throughput, p50/p95/p99 latency, queue wait time, open socket count, session/channel memory, and allocations. Implement the codec and state machines first, followed by in-memory paired-end tests, TCP/TLS, the session pool, and authentication integration.

## 18. References

The platform supplies TCP transport according to the TCP standard below. RB lifecycles, application frames, and pool rules are design decisions in this draft. Authentication integrations follow their respective standards.

- [TCP — RFC 9293](https://www.rfc-editor.org/rfc/rfc9293.html)
- [SASL — RFC 4422](https://www.rfc-editor.org/info/rfc4422/)
- [SASL OAuth — RFC 7628](https://www.rfc-editor.org/info/rfc7628/)
- [PKCE — RFC 7636](https://www.rfc-editor.org/info/rfc7636/)
- [Native OAuth — RFC 8252](https://www.rfc-editor.org/info/rfc8252/)
- [OAuth Security BCP — RFC 9700](https://www.rfc-editor.org/info/rfc9700/)
- [OpenID Connect Core](https://openid.net/specs/openid-connect-core-1_0.html)

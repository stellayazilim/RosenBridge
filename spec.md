# RosenBridge — TCP Abstraction Library

Status: v0.1 draft. Updated: 2026-09-15.

This document describes the current architectural decisions. The previous one-socket-per-channel and socket-multiplexing draft is preserved in the [design history](docs/design-history/2026-09-15-multiplex-draft.md); it is not the contract to implement. Wire encoding is not final.

## 1. Core model

RosenBridge provides session, channel, connection, and request/response stream APIs over the platform's TCP facilities. It does not require a broker, persistent queue, or routing service.

- **Session:** The lifetime of an accepted management connection and its operations.
- **Channel:** A logical endpoint such as `/files`, representing an address, handler, and access/resource policy. It is not a TCP socket.
- **Connection:** A physical TCP/TLS data connection routed to a channel. Each connection is dedicated to one complete request/response operation.
- **Request / response:** Two payload streams flowing in opposite directions on the same connection. They can progress concurrently.

```text
Session — accepted management connection
├─ Channel /files
│  ├─ Connection A → request A ⇄ response A
│  ├─ Connection B → request B ⇄ response B
│  └─ Connection C → request C ⇄ response C
└─ Channel /updates
   └─ Connection D → request D ⇄ response D
```

Multiple concurrent connections may target the same channel. There is no single-active-operation limit per channel; configured connection capacity limits concurrency. Completion or loss of a connection does not close its channel endpoint or other connections to that channel.

A second request/response is not started on the same connection; the socket is not reused for another operation or channel. Once both payload directions end, the connection closes and its capacity is released. Long-lived request/response streams are supported.

### 1.1 TCP responsibilities

The transport standard is [TCP — RFC 9293](https://www.rfc-editor.org/rfc/rfc9293.html). Connection establishment, segmentation, ordering, retransmission, TCP ACK/window, and congestion control are delegated to the platform. RosenBridge uses the language/runtime's TCP connect, accept, read, write, and close facilities. TLS uses a standard TLS implementation.

TCP is a byte stream; read/write boundaries are not message boundaries. RosenBridge delegates session acceptance to the upper layer and handles endpoint selection, binding connections to sessions, request/response start and end, cancellation, and application resource limits.

## 2. Addressing and endpoint lifetime

```text
rbs://server.example:5501
rbs://server.example:5501/files
rbs://server.example:5501/some/other/path
rb://127.0.0.1:5500
```

- The host and mandatory port identify the physical server endpoint; management and data connections may arrive at the same listener.
- An empty path or `/` selects the session. Other paths identify logical channel endpoints; path segments do not create separate channels.
- Acquiring a logical channel handle does not allocate a data socket or data-connection slot. Opening a request requires a new connection reservation.
- Calls to the same path target the same logical endpoint; connection identities distinguish individual operations.
- `rb` uses TCP; `rbs` uses TLS over TCP. No authentication or application data is sent before TLS completes.
- Fragments and userinfo are prohibited. Ports 5500/5501 are examples, not claims of registered ports.
- Paths are case-sensitive and must match exactly; no Unicode normalization is performed.
- URI path segments are percent-decoded exactly once using strict UTF-8. Invalid encoding, encoded slash/backslash, literal backslash, and control characters are rejected.
- Empty segments, trailing slashes, `.`, and `..` are rejected. Validation precedes silent normalization by the URI library. A decoded path is at most 1024 UTF-8 bytes.
- The path in a management message is a decoded string; the server does not percent-decode it again.

Credentials and authentication configuration are not URI parameters. The implementation rejects queries. Credentials are supplied separately through client options.

The server owns channel registration/handlers; a channel handle within a session represents access to that endpoint. Closing a session does not remove the server's endpoint registration for other sessions. Disabling an endpoint rejects new requests; existing connections complete or are cancelled according to the configured drain policy.

## 3. Management connection and authentication

```text
Platform TCP connection → [TLS] → application role/version → optional upper-layer acceptance → session ready
```

The platform performs the TCP handshake. Management/data role selection on a shared listener and session identity are application-startup information. Their encoding has not yet been finalized.

The upper layer optionally validates the credential on the management connection. RB does not define user authentication schemes or identity types. No data-connection reservation is granted before the session is ready. The management connection carries endpoint requests, capacity management, tickets, cancellation, and session closure; it does not carry payloads.

A data connection does not initiate another user login; it binds to the existing session with a single-use ticket. Each new physical connection still performs the required TLS setup.

## 4. Opening an operation connection to a channel

1. The client requests a new request/response connection for a channel path over the accepted management connection.
2. The server checks the registered endpoint, accepted session, optional asynchronous upper-layer endpoint policy, and capacity.
3. If capacity is available, it reserves a connection and returns a reservation/connection identity and single-use ticket over the management connection.
4. The client opens a new TCP/TLS connection to the same server and presents the ticket.
5. The server verifies that the ticket matches the active session, endpoint, and reservation, then consumes it atomically.
6. The connection is bound to one request/response operation at that endpoint; request reception and response production can progress independently.

Tickets are not reusable across a channel: every connection to the same endpoint obtains its own reservation and ticket. A ticket contains at least 256 bits of cryptographic randomness; the draft lifetime is 10 seconds and cannot exceed the remaining acquisition deadline. Ticket/session secrets are not logged. TLS is required over the network; plaintext TCP development use is limited to explicitly enabled trusted loopback mode.

Invalid, expired, or replayed tickets cause only the incoming socket to be rejected. An unverified bind cannot cancel another connection's reservation. Bind, deadline, and cancellation races release the slot exactly once; a late notification cannot revive a terminal request.

The server may also offer to open an operation connection over the management connection. If the client accepts, the same reservation flow applies and the client still opens the physical connection. The reservation identifies which end initiates the request; this need not be the end that opens the socket.

Session, acquisition-request, and connection/reservation identities are separate concepts. The endpoint path is the channel key. Identity wire representations and numeric message codes remain open design decisions; in-socket TransferID/replyTo routing is unnecessary.

## 5. Connection capacity and lifecycle

```text
REQUESTED → QUEUED → RESERVED → CONNECTING → ACTIVE → CLOSING → CLOSED
                    error / cancellation / deadline → CLOSED
```

These states belong to an individual operation connection, not its channel endpoint.

- The server is the sole reservation scheduler. RESERVED, CONNECTING, ACTIVE, and CLOSING connections hold slots.
- The session total, per-channel connection limit, and server-wide socket/memory budgets apply together. Channel count is not connection count.
- Initial recommendations are 8 concurrent data connections and 64 pending acquisition requests per session. Per-endpoint limits are configurable and cannot override session/global limits.
- FIFO waiting, deadlines, and cancellation are supported by the design. Eligible session queues receive fair scheduling.
- An immediate attempt returns busy without queueing when capacity is unavailable. Queues and local admission waiters are bounded.
- Tickets are issued only after a slot and the required resources have been reserved. Queueing consumes the total acquisition deadline, not the ticket lifetime.
- The control connection does not consume a data slot but counts toward the total socket budget. Unauthenticated/pending sockets are also bounded.
- When a connection ends, only its reservation, buffers, and waiters are released. Other connections to the same endpoint continue.

## 6. Payloads: streaming in both directions

All payloads are streams. An individual message/byte array is sent as a finite stream; an input stream uses the same path. Payloads are not subject to mandatory JSON/Base64 conversion. Length may be declared when known or determined as data is produced. Full-payload buffering is not the default.

```text
A → B: request   start ── bytes ── bytes ── bytes ── end
B → A: response          start ── bytes ── bytes ── end
```

- The handler receives the request reader without waiting for the entire request.
- A response may start and finish before the request ends. Its recipient also processes incoming data without waiting for the whole response.
- The request writer and response reader must be independently accessible from the same operation handle. An input-stream helper must not wait for the entire upload before returning that handle.
- Request and response end independently. Normal completion of one direction does not interrupt the other or close the socket prematurely.
- Each connection contains one request stream and one response stream; empty streams are valid. Multiple application results may be represented in the response stream using an upper-layer format.
- Completion of a local write does not mean the peer consumed the payload or completed its business operation successfully. Finishing local writes, awaiting peer consumption, and receiving a business result are distinct meanings.
- The connection closes after both directions complete. Operation cancellation or failure terminates both directions.
- Payload formats must support incremental processing; read chunks are not application-record boundaries.

The goal is to overlap sending, processing, and response consumption to reduce time to the first usable result and buffering requirements. Total duration and throughput are evaluated through measurement.

## 7. Framing: remaining decisions

A socket does not multiplex multiple operations. The former 16-byte TransferID header, per-transfer credit messages, replyTo mapping, and inter-transfer writer scheduler are not required by this architecture; the old wire table has been removed from the current contract.

The remaining application-level information to define is:

1. Encoding/boundaries for management messages and ticket presentation.
2. Request/response metadata startup and transition to payload.
3. Distinguishing normal end from truncation for known/unknown-length streams.
4. Representation of cancellation, errors, and peer-consumption acknowledgement if required.

Options for payload termination include lengths, small chunk/end markers, and suitable directional shutdown. The choice must preserve independent completion of both directions and TLS semantics. Unexpected socket EOF/reset is not automatically a successful payload end. An indistinguishable sentinel byte sequence is not inserted into raw payloads.

The final per-payload framing format therefore remains open. The stream model and channel/connection distinction are settled independently of this choice.

### 7.1 Proposal: length-prefixed chunks and an END flag

The payload is not scanned for a magic byte; arbitrary binary data may contain the same byte/sequence. Instead, each chunk starts with a fixed-size header. One candidate format is 1 byte of flags and a 4-byte unsigned big-endian payloadLength. An END bit in flags marks the end of that stream direction after this chunk. The final bit value and maximum chunk size have not yet been selected.

```text
[flags=0,   length=5][ABCDE]
[flags=0,   length=3][XYZ]
[flags=END, length=0]          ← this payload direction has ended
```

If the producer knows the final chunk in advance, it may set END in the header of that payload-bearing chunk. If it discovers the end only on the next input read, it sends an empty END chunk instead of delaying data just to identify the last chunk. An empty stream is represented by a single empty END chunk.

The reader reads and validates the complete header, then consumes exactly the declared payload length; it interprets another header only after that length is satisfied. An END byte inside the payload has no special meaning. Received payload bytes may be exposed to the application without buffering the entire chunk. Chunk limits are validated before allocation/reading; unknown flags, partial headers/bodies, connection loss before END, and additional payload in the same direction after END are errors.

END marks only the normal end of the corresponding request or response payload; it does not acknowledge peer consumption or business success. The other direction continues. TLS/TCP closure does not substitute for END. Metadata startup and error/cancellation representation still require clarification within this proposal. This header has no transfer ID because the connection already belongs to one request/response operation.

## 8. Buffers, backpressure, and cancellation

Read/write buffers for each operation connection are bounded; memory is not reserved for the entire payload. Slow application reads do not trigger unlimited read-ahead, preserving the effect of platform TCP flow control. A second connection/transfer credit protocol designed for multiplexing is not added.

- Session, endpoint, and host-wide memory limits apply in addition to connection limits.
- Callback and connection-admission queues are bounded by count/memory. The management loop does not await application handlers.
- Both directions must be consumable concurrently. If both ends only write and defer reading the other direction, bounded buffers can deadlock; helper APIs must not require that pattern.
- Full-buffer reading is an optional helper with a total size limit.
- If needed, rate limiting is an application policy at connection/channel/session level; TCP congestion control is not reimplemented.
- Cancellation terminates the waiting entry, reservation, or active operation connection. Cleanup is idempotent; slots/buffers are released exactly once.
- A lost data socket affects only its request/response operation. The session and other connections to the same channel are unaffected.
- Control-connection loss or an upper-layer session close terminates all operation connections and reservations for that session.

## 9. Security

Rbs uses TLS 1.2 or later; TLS 1.3 is preferred. The certificate chain and hostname are validated. Data sockets use the same server identity/validation policy as the control socket. Authentication, tickets, and application data are not sent as TLS early data.

Authentication schemes, token acquisition/validation, refresh and identity expiry belong to the upper layer. RB forwards an opaque credential in the first management envelope over TCP/TLS, or in the initial Authorization header over HTTP Upgrade. Data connections use only tickets. The upper layer may store identity in session Items and close the session on expiry or revocation. RB never places credentials in URLs. Tokens, tickets and credential bodies must be excluded from logs.

## 10. Timeouts, closure, and API direction

Separate deadlines are defined for acquisition queueing, TCP/TLS setup, ticket binding, active operations, and shutdown. The acquisition timeout does not restart at each step. A long-lived stream policy is selected separately for active operations. Control heartbeat stays on the management connection; ping bytes are not injected into raw payloads. Data-idle checking is a local timeout policy.

Session shutdown stops new operation requests, cancels pending reservations, and allows active connections a bounded drain period. Disabling an endpoint affects that endpoint; disposing/cancelling one connection affects only that operation. The lifetime of a registered endpoint must not be confused with a session's channel handle.

The following are illustrative API names, not final signatures:

| Operation | Meaning |
|---|---|
| ConnectAsync(serverUri) | Authenticated session is ready |
| Session.GetChannel(path) | Logical endpoint handle; no data socket is allocated |
| Channel.OpenRequestAsync | New connection reserved and ticket-bound; request writer and response waiter are ready |
| Channel.TryOpenRequestAsync | Returns busy without queueing if capacity is unavailable |
| Request.ResponseAsync | Response metadata/reader is ready; body completion is not awaited |
| IncomingRequest.OpenResponseAsync | Response writer is ready; request completion is not awaited |
| Payload.WriteAsync | Accepted into bounded local sending infrastructure; source memory may be reused |
| Payload.CompleteWritesAsync | Local writing direction has ended; the opposite direction stays open |
| Request.Completion | Both directions and connection cleanup have completed; failures/cancellation reach the caller |
| Session.CloseAsync | Session connections have drained or closed at the deadline |

Wire representation of success/peer-consumption acknowledgement remains an open decision in section 7. Socket writes or connection closure alone do not establish business success. Connection loss may leave the outcome uncertain; payloads are not replayed automatically. Reconnect creates a new session; restarting operations is an explicit application decision.

## 11. Verification and measurement goals

1. Multiple concurrent connections to one channel, with each carrying only its own request/response.
2. Completion/reset of one connection does not close other operations at that channel or the endpoint itself.
3. Session/channel/global capacity, FIFO waiting, busy, deadlines, and cancellation; exactly-once slot release.
4. Separate tickets for each connection after session authentication; replay, wrong endpoint/session, expiry, and competing consumption of one ticket by two sockets.
5. Grant/bind/ready races with cancellation; late notifications cannot revive terminal operations.
6. Processing the first response byte before the request ends; early response completion does not truncate the request.
7. Individual messages/byte arrays and unknown-length input streams behave identically; two ends operate without a broker.
8. Bounded memory for large payloads, slow readers, and concurrent bidirectional transfer; helpers do not introduce deadlocks.
9. Reads split at every byte boundary, coalesced reads, empty payloads, and early EOF/reset for the selected metadata/end encoding.
10. TLS/hostname and authentication validation; user login is not repeated for channel connections.
11. Session-operation cleanup after control loss; endpoint registration remains available to other sessions.
12. Input/output stream disposal, deadline, and endpoint/session-drain races.
13. Time to first response byte/processable result, total duration, throughput, allocation, and concurrent socket count.
14. TCP/TLS setup cost per request; CPU/memory and latency as connection count per channel increases.
15. Shared fixtures and independent language implementations after the wire format is selected.

## 12. References

The working Channel API and experimental framing choices are documented in [channel-api.md](docs/channel-api.md). Management/session/ticket and TLS behavior are documented in [client-server-api.md](docs/client-server-api.md). Those documents describe the limits of the initial implementation; the standards below do not imply full implementation compliance.

The platform supplies TCP transport according to the TCP standard below. RB lifecycles, application frames, and pool rules are design decisions in this draft. Authentication integrations follow their respective standards.

- [TCP — RFC 9293](https://www.rfc-editor.org/rfc/rfc9293.html)
- [SASL — RFC 4422](https://www.rfc-editor.org/info/rfc4422/)
- [SASL OAuth — RFC 7628](https://www.rfc-editor.org/info/rfc7628/)
- [PKCE — RFC 7636](https://www.rfc-editor.org/info/rfc7636/)
- [Native OAuth — RFC 8252](https://www.rfc-editor.org/info/rfc8252/)
- [OAuth Security BCP — RFC 9700](https://www.rfc-editor.org/info/rfc9700/)
- [OpenID Connect Core](https://openid.net/specs/openid-connect-core-1_0.html)

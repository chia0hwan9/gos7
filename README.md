# gos7
Implementation of Siemens S7 protocol in golang

Overview
-------------------
For years, numerous drivers/connectors, available in both commercial and open source domains, have supported the connection to S7 family PLC devices. GoS7 fills the gaps in the S7 protocol, implementing it with pure Go (also known as golang). There is a strong belief that low-level communication should be implemented with a low-level programming language that is close to binary and memory.

The minimum supported Go version is 1.21 (see `go.mod`).

Functions
-------------------
AG:
*   Read/Write Data Block (DB) (tested)
*   Read/Write Merkers(MB) (tested)
*   Read/Write IPI (EB) (tested)
*   Read/Write IPU (AB) (tested)
*   Read/Write Timer (TM)  (tested)
*   Read/Write Counter (CT) (tested)
*   Multiple Read/Write Area (tested)
*   Get Block Info (tested)

PG:
*   Hot start/Cold start / Stop PLC
*   Get CPU of PLC status (tested)
*   List available blocks in PLC (tested)
*   Set/Clear password for session
*   Get CPU protection and CPU Order code
*   Get CPU/CP Information (tested)
*   Read/Write clock for the PLC
Helpers:
*   Get/set value for a byte array for types: value(bit/int/word/dword/uint...), real, time, counter

Supported communication
-----------------
*   TCP
*   Serial (PPI, MPI) (under construction)

How to:
----------
following is a simple usage to connect with PLC via TCP
```go
const (
	tcpDevice = "127.0.0.1"
	rack      = 0
	slot      = 2
)
// TCPClient
handler := gos7.NewTCPClientHandler(tcpDevice, rack, slot)
handler.Timeout = 200 * time.Second
handler.IdleTimeout = 200 * time.Second
handler.Logger = log.New(os.Stdout, "tcp: ", log.LstdFlags)
// Connect manually so that multiple requests are handled in one connection session
handler.Connect()
defer handler.Close()
//init client
client := gos7.NewClient(handler)
address := 2710
start := 8
size := 2
buffer := make([]byte, 255)
value := 100
//AGWriteDB to address DB2710 with value 100, start from position 8 with size = 2 (for an integer)
var helper gos7.Helper
helper.SetValueAt(buffer, 0, value)  
err := client.AGWriteDB(address, start, size, buffer)
buf := make([]byte, 255)
//AGReadDB to address DB2710, start from position 8 with size = 2
err := client.AGReadDB(address, start, size, buf)
var s7 gos7.Helper
var result uint16
s7.GetValueAt(buf, 0, &result)	 
  
```
Request/response matching
----------
Every request carries a running PDU reference and the reply has to echo it. A reply with another
reference (for example a late answer after a timeout at a slow gateway) is discarded by the TCP
transporter, which keeps waiting for the matching reply until the timeout expires. Transporters other
than the TCP one get the error `CLI : PDU reference mismatch` from the packager. A read reply whose item
does not hold the requested number of bytes is rejected with `CLI : Invalid data size received` and
leaves the caller's buffer untouched.

References
----------
- libnodave http://libnodave.sourceforge.net/
- snap7 http://snap7.sourceforge.net/ 
- tarm serial library https://github.com/tarm/serial
- Simatic Open TCP/IP Communication via Industrial Ethernet from Siemens(doku)
- SIMATIC NET FDL-Programmierschnittstelle (doku)
- Elementary Data Types from Siemens (doku)

Fork notes (chia0hwan9/gos7)
----------
This fork keeps the upstream module path (`github.com/robinson/gos7`) so it can be used with a
`replace` directive. Upstream 619ce8b landed its own request/response matching (see
"Request/response matching" above), so the fork no longer carries a PDU-reference implementation of
its own; it keeps only the deltas below.

1. **`ErrStaleResponse` sentinel.** `tcpPackager.Verify` returns an error that both keeps upstream's
   `CLI : PDU reference mismatch` text and wraps `ErrStaleResponse` (`errors.Is`-able), so callers
   that treat a mismatched reply as "connection is out of sync, drop and reconnect" keep working.
2. **`TolerateNoEcho`.** Upstream requires the reply to echo the PDU reference. A PLC or gateway that
   does not (returns 0 / a constant / its own counter) would then wait for the timeout on *every*
   exchange, i.e. the channel never works. Setting `handler.TolerateNoEcho = true` accepts any reply
   with a single warning, at the cost of losing late-reply detection (only the drain below remains).
   Default is `false` (upstream behaviour).
   Background: echoing is what a conforming responder does — libnodave's device-side simulator
   (`ibhsim5.c`) copies the request's reference into the response header, and Snap7's client
   auto-increments a sequence per request — but Snap7 never *validates* the echo, so implementations
   that ignore the field exist.
3. **Drain after a failed exchange.** `tcpTransporter.Send` sets `needsDrain` when an exchange does
   not finish cleanly and, before the next exchange, discards whatever bytes have already arrived
   (`drainPendingLocked`). Upstream never cleaned the socket (`flush` was written but never called,
   and it used `time.Now()` as the read deadline, which Go's netpoll treats as "already expired" —
   so it could not have read anything anyway). This complements upstream's discard-until-match loop:
   the drain removes bytes already sitting in the buffer, the loop handles replies that arrive later.
4. **No panics in the read/write paths.** `readArea` sliced `response.Data[25:25+n]` and
   `buffer[offset:offset+n]` unguarded: a truncated response silently zero-padded the caller's
   buffer (upstream now checks the announced item length), and a too-small caller buffer panicked.
   Now:
   * a response shorter than requested (or whose item announces another length) →
     `ErrShortReadResponse`, keeping upstream's `CLI : Invalid data size received` wording;
   * a caller buffer that cannot hold the data → `ErrBufferTooSmall`;
   * `responseError` reads the PDU type at byte 8 and the error class/code at 17-18 (upstream
     af99822) and additionally never touches a frame shorter than TPKT+COTP+S7 header.
5. **`AGWriteDBBit`.** Native single-bit DB write (`transport size = BIT`, no read-modify-write):
   `client.AGWriteDBBit(db, byteAdr, bit, value)` sends one `WriteVar` whose parameter lists
   `TS=BIT(0x01)`, bit-addressed offset `byteAdr*8+bit`, data item `TS=BIT(0x03)`, length 1 (bit) and
   one data byte. Same encoding as Snap7/Sharp7's `WriteArea(S7WLBit)` and S7.Net's `WriteBit`.
   Callers that could only write whole bytes had to do read-modify-write and could overwrite the
   neighbouring bits with a value the PLC changed in between.

Also removed: a leftover `log.Printf` debug line in `Send` (it spammed stderr on every read error).

Tests: upstream's `client_test.go` / `response_test.go` / `control_test.go` / `datetime_test.go` cover
the merged upstream behaviour; fork-only coverage lives in `stale_response_test.go` (the
`ErrStaleResponse` wrapping, `TolerateNoEcho`, and the drain) and `panic_safety_test.go` (short
response, short caller buffers for read and write, and `responseError` over short frames — all
return errors / no-ops instead of panicking), plus `bitwrite_test.go` for `AGWriteDBBit` (exact
36-byte telegram, no extra request after the write, clear path, bit range validation, PLC rejection).

Simatic, Simatic S5, Simatic S7, S7-200, S7-300, S7-400, S7-1200, S7-1500 are registered Trademarks of Siemens

License
----------
https://opensource.org/licenses/BSD-3-Clause

Copyright (c) 2018, robinson

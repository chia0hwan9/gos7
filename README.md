# gos7
Implementation of Siemens S7 protocol in golang

Overview
-------------------
For years, numerous drivers/connectors, available in both commercial and open source domains, have supported the connection to S7 family PLC devices. GoS7 fills the gaps in the S7 protocol, implementing it with pure Go (also known as golang). There is a strong belief that low-level communication should be implemented with a low-level programming language that is close to binary and memory.

The minimum supported Go version is 1.13.

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
`replace` directive, and adds three changes around **stale responses** (a request that timed out
but whose answer arrives later, then gets read as the answer to the *next* request):

1. **Per-request PDU reference.** `readArea` / `writeArea` / `AGReadMulti` / `AGWriteMulti` write an
   incrementing reference into the S7 header (bytes 11-12, `setPduRef` + `client.nextPduRef`).
   Previously every request reused the constant `5,0` from the telegram template, so a response
   could not be attributed to a request at all.
2. **Real `Verify`.** `TCPClientHandler.Verify` (the upstream stub was "reserve for future use")
   classifies the response's reference against the request's:
   * equal → normal (the responder echoes it);
   * equal to one of the **last two** requests' references → `ErrStaleResponse` (`errors.Is`-able):
     that is the late answer of an earlier request being read as ours — without the check gos7
     silently returns the *previous* request's data, or panics on a short payload;
   * anything else (0, a constant, the device's own counter) → treated as a device that does **not**
     echo the reference: allowed through, with a single warning log.

   Echoing is what a conforming responder does — libnodave's device-side simulator (`ibhsim5.c`)
   copies the request's reference into the response header (`resp[21..22] = p1.header[4..5]`), and
   Snap7's client auto-increments a sequence per request (`PDUH_out->Sequence = GetNextWord()`).
   But Snap7 never *validates* the echo, so implementations that ignore the field exist; rejecting
   those outright would make the channel unusable, hence the third case above (residual frames are
   still cleaned up by the drain).
3. **Drain after a failed exchange.** `tcpTransporter.Send` sets `needsDrain` when an exchange does
   not finish cleanly and, before the next exchange, discards whatever bytes have already arrived
   (`drainPendingLocked`). Upstream never cleaned the socket (`flush` was written but never called,
   and it used `time.Now()` as the read deadline, which Go's netpoll treats as "already expired" —
   so it could not have read anything anyway).
4. **No panics in the read/write paths.** `readArea` used to slice `response.Data[25:25+n]` and
   `buffer[offset:offset+n]` unguarded: a truncated response panicked (bringing the whole gateway
   process down) and a too-small caller buffer panicked too. Now:
   * a response carrying fewer bytes than requested → `ErrShortReadResponse`. Note it is *worse*
     than a panic when the answer is long enough to pass the library's own `len < 25` check but
     *longer* than requested — that older behaviour silently consumed the wrong bytes, which is why
     `Verify` (item 2) also checks the reference;
   * a caller buffer that cannot hold the data → `ErrBufferTooSmall`;
   * `responseError` requires a 12-byte header before reading `Data[2..3]` / `Data[10..11]`.

   `Verify` also rejects telegrams shorter than 13 bytes, so `responseError` can no longer be
   reached with a short frame at all.

Also removed: a leftover `log.Printf` debug line in `Send` (it spammed stderr on every read error).

Tests: `stale_response_test.go` (reference counter, `Verify` classification table, and end-to-end
over a loopback TCP pair: reference increments/echoes, a response echoing the *previous* request is
reported as `ErrStaleResponse`, a non-echoing responder is tolerated with one warning, and stale
bytes are drained before the next request) and `panic_safety_test.go` (short response, short caller
buffers for read and write, and `responseError` over short frames — all return errors / no-ops
instead of panicking).

Simatic, Simatic S5, Simatic S7, S7-200, S7-300, S7-400, S7-1200, S7-1500 are registered Trademarks of Siemens

License
----------
https://opensource.org/licenses/BSD-3-Clause

Copyright (c) 2018, robinson

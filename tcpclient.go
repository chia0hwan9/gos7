package gos7

// Copyright 2018 Trung Hieu Le. All rights reserved.
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.
import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// Default TCP timeout is not set
	tcpTimeout     = 10 * time.Second
	tcpIdleTimeout = 60 * time.Second
	tcpMaxLength   = 2084
	//messages
	pduSizeRequested = 480
	isoTCP           = 102 //default isotcp port
	isoHSize         = 7   // TPKT+COTP Header Size
	minPduSize       = 16
	// Client Connection Type
	connectionTypePG    = 1 // Connect to the PLC as a PG
	connectionTypeOP    = 2 // Connect to the PLC as an OP
	connectionTypeBasic = 3 // Basic connection
)

// TCPClientHandler implements Packager and Transporter interface.
type TCPClientHandler struct {
	tcpPackager
	tcpTransporter

	// noEchoWarned 只记一次"该设备不回显 PDU reference"，避免每条报文刷日志
	noEchoWarned atomic.Bool
}

// Verify 校验应答确实属于本次请求（判定见 classifyPduRef）：
//   - 回显本次 reference → 放行；
//   - 回显最近某次请求的 reference → 上一次超时后迟到的应答被读走了 → ErrStaleResponse，
//     调用方应丢弃连接重连（否则会静默解析出上一次请求的数据）；
//   - 其它（0 / 恒定值 / 设备自己的计数）→ 该设备不回显，放行并只警告一次。
func (h *TCPClientHandler) Verify(request []byte, response []byte) error {
	switch classifyPduRef(request, response) {
	case refStale:
		got := binary.BigEndian.Uint16(response[11:])
		want := binary.BigEndian.Uint16(request[11:])
		return fmt.Errorf("%w: got 0x%04X, want 0x%04X", ErrStaleResponse, got, want)
	case refTooShort:
		return fmt.Errorf("%w: short telegram (request %d bytes, response %d bytes)",
			ErrStaleResponse, len(request), len(response))
	case refNoEcho:
		if h.noEchoWarned.CompareAndSwap(false, true) && h.Logger != nil {
			h.Logger.Printf("s7: 该 PLC/网关未回显 PDU reference（应答 0x%04X / 请求 0x%04X），"+
				"已关闭回显校验；残留应答仍由 Send 失败后的清理兜底",
				binary.BigEndian.Uint16(response[11:]), binary.BigEndian.Uint16(request[11:]))
		}
	}
	return nil
}

// NewTCPClientHandler allocates a new TCPClientHandler.
func NewTCPClientHandler(address string, rack int, slot int) *TCPClientHandler {
	h := &TCPClientHandler{}
	h.Address = address
	h.Timeout = tcpTimeout
	h.IdleTimeout = tcpIdleTimeout
	h.ConnectionType = connectionTypePG // Connect to the PLC as a PG
	remoteTSAP := uint16(h.ConnectionType)<<8 + (uint16(rack) * 0x20) + uint16(slot)
	h.setConnectionParameters(address, 0x0100, remoteTSAP)
	return h
}

// NewTCPClientHandlerWithConnectType allocates a new TCPClientHandler with connection type.
func NewTCPClientHandlerWithConnectType(address string, rack int, slot int, connectType int) *TCPClientHandler {
	h := &TCPClientHandler{}
	h.Address = address
	h.Timeout = tcpTimeout
	h.IdleTimeout = tcpIdleTimeout
	h.ConnectionType = connectType
	remoteTSAP := uint16(h.ConnectionType)<<8 + (uint16(rack) * 0x20) + uint16(slot)
	h.setConnectionParameters(address, 0x0100, remoteTSAP)
	return h
}

// TCPClient creator for a TCP client with address, rack and slot, implement from interface client
func TCPClient(address string, rack int, slot int) Client {
	handler := NewTCPClientHandler(address, rack, slot)
	return NewClient(handler)
}

// TCPClientWithConnectType creator for a TCP client with address, rack, slot and connect type, implement from interface client
func TCPClientWithConnectType(address string, rack int, slot int, connectType int) Client {
	handler := NewTCPClientHandlerWithConnectType(address, rack, slot, connectType)
	return NewClient(handler)
}

// tcpPackager implements Packager interface.
type tcpPackager struct {
	//reserve for future use, this package should be pass into trans ID, pack ID
	//or somethingelse to verify the request and response
}

// tcpTransporter implements Transporter interface.
type tcpTransporter struct {
	// Connect string
	Address string
	// Connect & Read timeout
	Timeout time.Duration
	// Idle timeout to close the connection
	IdleTimeout time.Duration
	// Transmission logger
	Logger *log.Logger

	// TCP connection
	mu           sync.Mutex
	conn         net.Conn
	closeTimer   *time.Timer
	lastActivity time.Time

	localTSAP, remoteTSAP uint16

	localTSAPHigh, localTSAPLow   byte
	remoteTSAPHigh, remoteTSAPLow byte
	ConnectionType                int
	LastPDUType                   byte

	PDULength int

	// needsDrain 上一次收发没有干净收尾（超时/IO 错误）后置位：下一次收发前先丢弃 socket 里
	// 已经到达的残留字节（PLC 迟到的应答），避免它被当成本次请求的应答读走。
	needsDrain bool
}

func (mb *tcpTransporter) setConnectionParameters(address string, localTSAP uint16, remoteTSAP uint16) {
	locTSAP := localTSAP & 0x0000FFFF
	remTSAP := remoteTSAP & 0x0000FFFF
	if len(strings.Split(address, ":")) < 2 {
		mb.Address = address + ":" + strconv.Itoa(isoTCP) //ip:102
	} else {
		mb.Address = address
	}
	mb.localTSAPHigh = byte(locTSAP >> 8)
	mb.localTSAPLow = byte(locTSAP & 0x00FF)
	mb.remoteTSAPHigh = byte(remTSAP >> 8)
	mb.remoteTSAPLow = byte(remTSAP & 0x00FF)
}

// Send sends data to server and ensures response length is greater than header length.
func (mb *tcpTransporter) Send(request []byte) (response []byte, err error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	defer func() {
		if err != nil {
			// 本次交换没有干净收尾（超时/IO 错误/非法 PDU）：PLC 的应答可能还在路上。
			// 置位后由下一次 Send 先丢弃，避免迟到的应答被当成下一次请求的应答。
			mb.needsDrain = true
		}
	}()
	// Set timer to close when idle
	mb.lastActivity = time.Now()
	mb.startCloseTimer()
	// Set write and read timeout
	var timeout time.Time
	if mb.Timeout > 0 {
		timeout = mb.lastActivity.Add(mb.Timeout)
	}
	if mb.conn == nil {
		err = fmt.Errorf("Connection to address %s is null", mb.Address)
		return
	}
	// 上一次交换失败过 → 先丢弃已经到达的残留字节（迟到的应答），再设本次超时
	if mb.needsDrain {
		mb.drainPendingLocked()
		mb.needsDrain = false
	}
	if err = mb.conn.SetDeadline(timeout); err != nil {
		return
	}
	// Send data
	mb.logf("s7: sending % x", request)
	if _, err = mb.conn.Write(request); err != nil {
		return
	}
	done := false
	data := make([]byte, tcpMaxLength)
	length := 0
	for !done && err == nil {
		// Get TPKT (4 bytes)
		if _, err = io.ReadFull(mb.conn, data[:4]); err != nil {
			return
		}
		// Read length, ignore transaction & protocol id (4 bytes)
		length = int(binary.BigEndian.Uint16(data[2:]))
		if length == isoHSize {
			_, err = io.ReadFull(mb.conn, data[4:7])
			if err != nil { // Skip remaining 3 bytes and Done is still false
				return
			}
		} else {
			if length > pduSizeRequested+isoHSize || length < minPduSize {
				err = fmt.Errorf("s7: invalid pdu")
				return
			}
			done = true
		}
	}
	// Skip remaining 3 COTP bytes
	_, err = io.ReadFull(mb.conn, data[4:7])
	if err != nil {
		return
	}
	mb.LastPDUType = data[5] // Stores PDU Type, we need it
	// Receives the S7 Payload
	_, err = io.ReadFull(mb.conn, data[7:length])
	if err != nil {
		return
	}
	response = data[0:length]
	mb.logf("s7: received % x\n", response)
	return
}

// Connect establishes a new connection to the address in Address.
// Connect and Close are exported so that multiple requests can be done with one session
func (mb *tcpTransporter) Connect() error {
	return mb.ConnectContext(context.Background())
}

// ConnectContext establishes a new connection to the address in Address.
// It is the same as Connect but accepts a context that can be used to
// cancel or set a deadline on the TCP dial and subsequent protocol handshake.
func (mb *tcpTransporter) ConnectContext(ctx context.Context) error {
	//first stage: TCP connection
	err := mb.tcpConnect(ctx)
	if err != nil {
		return err
	}
	//second stage: ISOTCP (ISO 8073) Connection
	err = mb.isoConnect()
	if err != nil {
		if mb.conn != nil {
			_ = mb.conn.Close()
		}
		return err
	}
	// Third stage : S7 protocol data unit negotiation
	return mb.negotiatePduLength()
}

func (mb *tcpTransporter) tcpConnect(ctx context.Context) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if mb.conn == nil {
		dialer := net.Dialer{Timeout: mb.Timeout}
		conn, err := dialer.DialContext(ctx, "tcp", mb.Address)
		if err != nil {
			if conn != nil {
				_ = conn.Close()
			}
			return err
		}
		mb.conn = conn
	}
	return nil
}

func (mb *tcpTransporter) isoConnect() error {
	msg := make([]byte, len(isoConnectionRequestTelegram))
	copy(msg, isoConnectionRequestTelegram)
	msg[16] = mb.localTSAPHigh
	msg[17] = mb.localTSAPLow
	msg[20] = mb.remoteTSAPHigh
	msg[21] = mb.remoteTSAPLow

	// Sends the connection request telegram
	response, err := mb.Send(msg)
	if size := len(response); size == 22 {
		if mb.LastPDUType != byte(0xD0) { // 0xD0 = CC Connection confirm
			err = fmt.Errorf("errIsoConnect")
		}
	} else {
		err = fmt.Errorf(ErrorText(errIsoInvalidPDU))
	}
	return err
}
func (mb *tcpTransporter) negotiatePduLength() error {
	// Set PDU Size Requested //lth
	pduSizePackage := make([]byte, len(s7PDUNegogiationTelegram))
	copy(pduSizePackage, s7PDUNegogiationTelegram)
	binary.BigEndian.PutUint16(pduSizePackage[23:], uint16(pduSizeRequested))
	// Sends the connection request telegram
	response, err := mb.Send(pduSizePackage)
	length := len(response)
	if length == 27 && response[17] == 0 && response[18] == 0 { // 20 = size of Negotiate Answer
		// Get PDU Size Negotiated
		mb.PDULength = int(binary.BigEndian.Uint16(response[25:]))
		if mb.PDULength <= 0 {
			err = fmt.Errorf(ErrorText(errCliNegotiatingPDU))
		}
	} else {
		err = fmt.Errorf(ErrorText(errCliNegotiatingPDU))
	}
	return err
}
func (mb *tcpTransporter) startCloseTimer() {
	if mb.IdleTimeout <= 0 {
		return
	}

	if mb.closeTimer == nil {
		mb.closeTimer = time.AfterFunc(mb.IdleTimeout, mb.closeIdle)
	} else {
		mb.closeTimer.Reset(mb.IdleTimeout)
	}
}

// Close closes current connection.
func (mb *tcpTransporter) Close() error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	return mb.close()
}

// flush flushes pending data in the connection,
// returns io.EOF if connection is closed.
func (mb *tcpTransporter) flush(b []byte) (err error) {
	if err = mb.conn.SetReadDeadline(time.Now()); err != nil {
		return
	}
	// Timeout setting will be reset when reading
	if _, err = mb.conn.Read(b); err != nil {
		// Ignore timeout error
		if netError, ok := err.(net.Error); ok && netError.Timeout() {
			err = nil
		}
	}
	return
}

// drainPendingLocked 丢弃 socket 里**已经到达**的残留字节：读截止时间设为极短的未来时刻
// （1ms），只吃不等待（循环直到读不到数据 / 出错 / 上限）。
//
// 用在"上一次收发失败（超时等）"之后：PLC 迟到的应答会留在缓冲里，不清掉就会被下一次
// 收发当成自己的应答（gos7 原本没有任何清理，`flush` 也从不被调用）。
// 注意不能用 `time.Now()` 作截止时间：Go 的 netpoll 在截止时间已过时**直接返回超时**、
// 不读缓冲（上游的 flush 正是这么写的，所以它即使被调用也读不到东西）。
// 必须在持有 mb.mu 时调用。
func (mb *tcpTransporter) drainPendingLocked() {
	if mb.conn == nil {
		return
	}
	if err := mb.conn.SetReadDeadline(time.Now().Add(time.Millisecond)); err != nil {
		return
	}
	buf := make([]byte, tcpMaxLength)
	for i := 0; i < 16; i++ {
		n, err := mb.conn.Read(buf)
		if n == 0 || err != nil {
			return // 没有更多残留：超时（正常）/EOF/连接已关
		}
	}
}

func (mb *tcpTransporter) logf(format string, v ...interface{}) {
	if mb.Logger != nil {
		mb.Logger.Printf(format, v...)
	}
}

// closeLocked closes current connection. Caller must hold the mutex before calling this method.
func (mb *tcpTransporter) close() (err error) {
	if mb.conn != nil {
		err = mb.conn.Close()
		mb.conn = nil
	}
	return
}

// closeIdle closes the connection if last activity is passed behind IdleTimeout.
func (mb *tcpTransporter) closeIdle() {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.IdleTimeout <= 0 {
		return
	}
	idle := time.Now().Sub(mb.lastActivity)
	if idle >= mb.IdleTimeout {
		mb.logf("s7: closing connection due to idle timeout: %v", idle)
		mb.close()
	}
}

// refVerdict 应答 PDU reference 的判定结果。
type refVerdict int

const (
	refOK      refVerdict = iota // 回显本次请求的 reference（正常）
	refStale                     // 是"最近某次请求"的 reference：迟到的应答被读走了
	refNoEcho                    // 既不是本次、也不是最近的：该设备不回显 reference
	refTooShort                  // 报文太短，连 reference 字段都没有
)

// staleWindow 往回看多少次请求：本连接器的 reference 由 client.nextPduRef 自增，
// 因此"上 k 次请求的 reference"就等于 want-k，无需额外状态。
const staleWindow = 2

// classifyPduRef 判定应答的 PDU reference（S7 头第 11-12 字节，含 TPKT+COTP 前缀）。
//
// 参考实现（libnodave 的设备侧模拟器 ibhsim5.c）会把请求的 reference 原样写回应答
// （PDUref = 请求 header[4..5] → 应答 header[4..5]），即"回显"是应答方的正常行为。
// 但并非所有实现都依赖它（Snap7 只给每个请求自增、并不校验回显），所以这里按
// 三态处理：回显本次 = 正常；回显最近的某次 = 残留应答（要拦）；其它 = 设备不回显（不拦，
// 由调用方记一条日志），否则这类设备会永远连不上。
func classifyPduRef(request []byte, response []byte) refVerdict {
	if len(request) < 13 || len(response) < 13 {
		return refTooShort
	}
	want := binary.BigEndian.Uint16(request[11:])
	got := binary.BigEndian.Uint16(response[11:])
	if got == want {
		return refOK
	}
	// 自增序列（跳过 0）里往回数：want-1、want-2…
	for k := uint16(1); k <= staleWindow; k++ {
		if got == prevRef(want, k) {
			return refStale
		}
	}
	return refNoEcho
}

// prevRef 返回 reference 序列里往回数第 k 个。序列由 client.nextPduRef 自增且**跳过 0**，
// 所以这里也要跳过 0（第一支请求的"上一次"约定为 0xFFFF，0 永远不会是真正发过的引用）。
func prevRef(want uint16, k uint16) uint16 {
	ref := want
	for i := uint16(0); i < k; i++ {
		if ref <= 1 {
			ref = 0xFFFF
			continue
		}
		ref--
	}
	return ref
}

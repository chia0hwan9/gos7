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
	// S7 PDU header behind TPKT and COTP
	s7ProtocolID   = 0x32 // First byte of every S7 PDU
	s7PDURefOffset = 11   // PDU reference, 2 bytes big endian, echoed by the PLC
)

// TCPClientHandler implements Packager and Transporter interface.
type TCPClientHandler struct {
	tcpPackager
	tcpTransporter
}

// Verify 复核 tcpPackager.Verify，唯一区别是 TolerateNoEcho 生效时不再校验回显
// （否则这类设备在 Send 里已被放行、却会在这里再被判死）。逻辑本身只有一份。
func (h *TCPClientHandler) Verify(request []byte, response []byte) error {
	if h.TolerateNoEcho {
		return nil
	}
	return h.tcpPackager.Verify(request, response)
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

// tcpPackager implements Packager interface. Its Verify matches the response
// to the request by the S7 PDU reference.
type tcpPackager struct{}

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

	// TolerateNoEcho 允许设备不回显 PDU reference（S7 规范要求回显，Snap7 客户端也不校验）。
	// 默认 false = 严格：reference 不符的应答视为"上一次请求的迟到应答"，丢弃后继续等
	// （能在连接落后一个应答时自行重新对齐）。置 true 则接受任何应答并只警告一次，
	// 用于确实不回显该字段的 PLC/网关——否则这类设备每次收发都会等到超时。
	TolerateNoEcho bool

	// noEchoWarned TolerateNoEcho 生效时只记一次日志，避免每条报文刷屏。
	noEchoWarned atomic.Bool
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
	data := make([]byte, tcpMaxLength)
	for {
		var length int
		if length, err = mb.receive(data); err != nil {
			return nil, err
		}
		response = data[0:length]
		mb.logf("s7: received % x\n", response)
		// A reply carrying another PDU reference answers an earlier request,
		// typically one that timed out. Taking it for the answer to this
		// request would hand the caller foreign data, so drop it and keep
		// waiting for the matching reply until the deadline expires.
		if !pduReferenceMismatch(request, response) {
			return response, nil
		}
		if mb.TolerateNoEcho {
			// 该设备不回显 PDU reference：接受应答，只警告一次（旧行为）。
			// 代价是失去"迟到应答"的识别能力，残留字节只由 needsDrain 兜底。
			if mb.noEchoWarned.CompareAndSwap(false, true) {
				mb.logf("s7: 该 PLC/网关未回显 PDU reference（请求 0x%04x，应答 0x%04x），"+
					"已关闭回显校验；残留应答仅由收发失败后的清理兜底",
					pduReference(request), pduReference(response))
			}
			return response, nil
		}
		mb.logf("s7: discarding reply with PDU reference 0x%04x, request has 0x%04x", pduReference(response), pduReference(request))
	}
}

// receive reads one ISO-on-TCP telegram (TPKT, COTP and S7 payload) into data
// and returns its length. Telegrams consisting of a bare COTP header are skipped.
func (mb *tcpTransporter) receive(data []byte) (length int, err error) {
	done := false
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

// isS7PDU reports whether the telegram carries an S7 PDU behind TPKT and COTP.
// COTP connection requests/confirms and truncated telegrams do not.
func isS7PDU(telegram []byte) bool {
	return len(telegram) >= s7PDURefOffset+2 && telegram[isoHSize] == s7ProtocolID
}

// pduReference returns the PDU reference of a telegram for which isS7PDU holds.
func pduReference(telegram []byte) uint16 {
	return binary.BigEndian.Uint16(telegram[s7PDURefOffset:])
}

// pduReferenceMismatch reports whether response is an S7 PDU that does not
// echo the PDU reference of the S7 request, i.e. it answers another request.
func pduReferenceMismatch(request, response []byte) bool {
	return isS7PDU(request) && isS7PDU(response) && pduReference(request) != pduReference(response)
}

// Verify checks that the response answers the request: the PDU reference the
// client stamped into the request header must come back in the response.
// A response with another reference belongs to an earlier request, for
// example a late answer after a timeout, and must not be interpreted as the
// answer to this one.
//
// 错误同时携带两套语义：上游的 errCliPDUReferenceMismatch 文案（`CLI : PDU
// reference mismatch…`）供既有调用方识别，以及 fork 的 ErrStaleResponse 哨兵
// （errors.Is 可判）——iidc 的连接器按它判定"读到残留应答 → 丢弃连接重连"。
func (mb *tcpPackager) Verify(request []byte, response []byte) (err error) {
	if pduReferenceMismatch(request, response) {
		err = fmt.Errorf("%w: %s: request 0x%04x, response 0x%04x", ErrStaleResponse,
			ErrorText(errCliPDUReferenceMismatch), pduReference(request), pduReference(response))
	}
	return
}

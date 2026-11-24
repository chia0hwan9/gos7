package gos7

// ---- fork 新增（合并上游后保留的部分）----
//
// 上游 619ce8b 已自带 PDU reference 的方案，且更完整：
//   - client.send 给每条 S7 请求盖自增 reference（上游实现，fork 原有的
//     setPduRef/nextPduRef 已删除）；
//   - tcpTransporter.Send 丢弃 reference 不符的应答并继续等，落后的连接能自行重新对齐；
//   - tcpPackager.Verify 对非 TCP transporter 做同样的校验。
//
// 本文件只保留上游没有的三件事：
//  1. Verify 的错误同时携带 ErrStaleResponse 哨兵（iidc 连接器按它判定"读到残留应答 → 丢弃重连"）；
//  2. TolerateNoEcho：确实不回显 reference 的 PLC/网关（否则每次收发都等到超时）；
//  3. 收发失败后的 needsDrain 残留清理。
//
// 上游的 reference 自增/跳过/应答归属判定由 client_test.go 覆盖，此处不再重复。

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"testing"
	"time"
)

// Verify 的 reference 不符必须能被 errors.Is(err, ErrStaleResponse) 识别，
// 同时保留上游的错误文案（`CLI : PDU reference mismatch…`）。
func TestVerify_WrapsErrStaleResponse(t *testing.T) {
	mk := func(ref uint16) []byte {
		b := make([]byte, 26)
		b[isoHSize] = s7ProtocolID
		binary.BigEndian.PutUint16(b[s7PDURefOffset:], ref)
		return b
	}

	err := (&tcpPackager{}).Verify(mk(7), mk(6))
	if !errors.Is(err, ErrStaleResponse) {
		t.Fatalf("应能 errors.Is 到 ErrStaleResponse，实际: %v", err)
	}
	if !strings.Contains(err.Error(), ErrorText(errCliPDUReferenceMismatch)) {
		t.Fatalf("错误文案应保留上游的 PDU reference mismatch，实际: %v", err)
	}

	// 回显一致 / 非 S7 报文 → 不报错
	if err := (&tcpPackager{}).Verify(mk(7), mk(7)); err != nil {
		t.Fatalf("回显一致不应报错: %v", err)
	}
	cotp := []byte{3, 0, 0, 22, 17, 224, 0, 0, 0, 1, 0}
	if err := (&tcpPackager{}).Verify(cotp, cotp); err != nil {
		t.Fatalf("COTP 报文（无 S7 头）不应报错: %v", err)
	}
}

// ---- 端到端：真实 TCP 上跑请求/应答（跳过 ISO 握手，直接塞 conn + PDULength） ----

// newFakePLCPair 建一对 loopback 连接：客户端 handler（已塞好 conn/PDULength）+ 服务端 conn。
func newFakePLCPair(t *testing.T) (*TCPClientHandler, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	h := NewTCPClientHandler(ln.Addr().String(), 0, 1)
	h.Timeout = 2 * time.Second
	h.PDULength = 240
	h.conn = conn
	t.Cleanup(func() {
		_ = h.Close()
		_ = server.Close()
		_ = ln.Close()
	})
	return h, server
}

// serveOneRead 读一个完整请求，按 refShift 回显 reference（0=正确回显，0xFFFF=回显上一次），
// 返回收到的请求报文。
func serveOneRead(server net.Conn, data []byte, refShift uint16) ([]byte, error) {
	return serveOneReadRef(server, data, func(want uint16) uint16 { return want + refShift })
}

// serveOneReadRef 同上，但由调用方直接决定应答里的 reference（用于模拟"不回显"的设备）。
func serveOneReadRef(server net.Conn, data []byte, refOf func(want uint16) uint16) ([]byte, error) {
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(server, hdr); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint16(hdr[2:]))
	rest := make([]byte, length-4)
	if _, err := io.ReadFull(server, rest); err != nil {
		return nil, err
	}
	req := append(append([]byte{}, hdr...), rest...)
	want := binary.BigEndian.Uint16(req[s7PDURefOffset:])
	ref := want
	if refOf != nil {
		ref = refOf(want)
	}

	// S7 读应答：TPKT+COTP(7) + 头(12) + 参数(2) + item 头(4) + 数据
	resp := make([]byte, 25+len(data))
	copy(resp[0:], []byte{3, 0, 0, 0, 2, 240, 128}) // TPKT + COTP
	resp[7] = 0x32                                  // Protocol ID
	resp[8] = 0x03                                  // Ack_Data
	binary.BigEndian.PutUint16(resp[11:], ref)      // PDU reference（回显）
	binary.BigEndian.PutUint16(resp[13:], 2)        // 参数长度
	binary.BigEndian.PutUint16(resp[15:], uint16(4+len(data)))
	resp[19] = 0x04 // Function: Read Var
	resp[20] = 0x01 // Items count
	resp[21] = 0xFF // Item return code: success
	resp[22] = 0x04 // Transport size
	binary.BigEndian.PutUint16(resp[23:], uint16(len(data)*8))
	copy(resp[25:], data)
	binary.BigEndian.PutUint16(resp[2:], uint16(len(resp)))
	_, err := server.Write(resp)
	return req, err
}

// 设备不回显 reference（应答恒为 0）时默认会等到超时——这是上游的严格语义。
func TestAGReadDB_NonEchoingDeviceTimesOutByDefault(t *testing.T) {
	h, server := newFakePLCPair(t)
	h.Timeout = 200 * time.Millisecond
	c := NewClient(h)

	go func() { _, _ = serveOneReadRef(server, []byte{0x33, 0x44}, func(uint16) uint16 { return 0 }) }()

	buf := make([]byte, 2)
	if err := c.AGReadDB(1, 0, 2, buf); err == nil {
		t.Fatal("严格模式下不应接受 reference 不符的应答")
	}
}

// TolerateNoEcho=true 时接受这类设备，且只警告一次（否则每次收发都超时，通道永远起不来）。
func TestAGReadDB_NonEchoingDeviceToleratedWithOption(t *testing.T) {
	h, server := newFakePLCPair(t)
	var logs bytes.Buffer
	h.Logger = log.New(&logs, "", 0)
	h.TolerateNoEcho = true
	c := NewClient(h)
	data := []byte{0x33, 0x44}

	for i := 0; i < 2; i++ {
		go func() { _, _ = serveOneReadRef(server, data, func(uint16) uint16 { return 0 }) }()
		buf := make([]byte, 2)
		if err := c.AGReadDB(1, 0, 2, buf); err != nil {
			t.Fatalf("第 %d 次读不应失败（TolerateNoEcho=true）: %v", i+1, err)
		}
		if !bytes.Equal(buf, data) {
			t.Fatalf("第 %d 次读数据不符: % X", i+1, buf)
		}
	}
	if n := strings.Count(logs.String(), "未回显 PDU reference"); n != 1 {
		t.Fatalf("应只警告一次，实际 %d 次：%s", n, logs.String())
	}
}

// 收发失败后置位 needsDrain；下一次 Send 先吃掉残留字节，再正常收本次应答。
func TestSend_DrainsStaleBytesAfterFailure(t *testing.T) {
	h, server := newFakePLCPair(t)
	c := NewClient(h)
	data := []byte{0x11, 0x22}

	// 服务端先塞一段"残留字节"（模拟上一次超时后迟到的应答），等它进到客户端缓冲
	if _, err := server.Write([]byte{3, 0, 0, 0x1A, 2, 240, 128}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	h.needsDrain = true

	go func() { _, _ = serveOneRead(server, data, 0) }()

	buf := make([]byte, 2)
	if err := c.AGReadDB(1, 0, 2, buf); err != nil {
		t.Fatalf("残留字节未被清理，读失败: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Fatalf("数据不符: % X", buf)
	}
	if h.needsDrain {
		t.Fatal("成功收尾后不应保留 needsDrain")
	}

	// 失败（对端直接关闭）→ 置位 needsDrain
	_ = server.Close()
	_ = c.AGReadDB(1, 0, 2, buf)
	if !h.needsDrain {
		t.Fatal("收发失败后应置位 needsDrain")
	}
}

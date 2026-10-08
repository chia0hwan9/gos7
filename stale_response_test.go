package gos7

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// ---- fork 新增：请求 PDU reference 自增 + 应答回显校验 + 失败后清理残留 ----
//
// 背景：gos7 原本不校验应答与请求的对应关系（tcpPackager.Verify 是空桩），也不清理
// socket；一次收发超时后 PLC 迟到的应答会被下一次收发当成本次应答读走——轻则拿到上一次
// 的数据，重则切片越界 panic。修法：① 每个读/写请求写入自增的 PDU reference；
// ② Verify 校验应答回显；③ 收发失败后下一次先丢弃残留字节。

func TestNextPduRef_MonotonicAndSkipsZero(t *testing.T) {
	c := &client{}
	if got := c.nextPduRef(); got != 1 {
		t.Fatalf("第一个 reference 应为 1，实际 %d", got)
	}
	if got := c.nextPduRef(); got != 2 {
		t.Fatalf("第二个 reference 应为 2，实际 %d", got)
	}
	c.pduRef.Store(0xFFFF)
	if got := c.nextPduRef(); got != 1 {
		t.Fatalf("回绕后应跳过 0，实际 %d", got)
	}
}

func TestVerify_DetectsStaleResponse(t *testing.T) {
	mk := func(ref uint16, n int) []byte {
		b := make([]byte, n)
		if n >= 13 {
			binary.BigEndian.PutUint16(b[11:], ref)
		}
		return b
	}

	cases := []struct {
		name     string
		request  []byte
		response []byte
		wantErr  bool
	}{
		{"reference 一致", mk(7, 26), mk(7, 26), false},
		{"reference 不一致（残留应答）", mk(7, 26), mk(6, 26), true},
		{"应答 reference 为 0", mk(7, 26), mk(0, 26), true},
		{"应答过短", mk(7, 26), mk(7, 8), true},
		{"请求过短", mk(7, 8), mk(7, 26), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := (&tcpPackager{}).Verify(tc.request, tc.response)
			if tc.wantErr && !errors.Is(err, ErrStaleResponse) {
				t.Fatalf("应返回 ErrStaleResponse，实际: %v", err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("不应报错，实际: %v", err)
			}
		})
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

// serveOneRead 读一个完整请求，按 refShift 回显 reference（0=正确回显，1=模拟残留应答），
// 返回收到的请求报文。
func serveOneRead(server net.Conn, data []byte, refShift uint16) ([]byte, error) {
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
	ref := binary.BigEndian.Uint16(req[11:]) + refShift

	// S7 读应答：TPKT+COTP(7) + 头(12) + 参数(2) + item 头(4) + 数据
	resp := make([]byte, 25+len(data))
	copy(resp[0:], []byte{3, 0, 0, 0, 2, 240, 128}) // TPKT + COTP
	resp[7] = 0x32                                   // Protocol ID
	resp[8] = 0x03                                   // Ack_Data
	binary.BigEndian.PutUint16(resp[11:], ref)       // PDU reference（回显）
	binary.BigEndian.PutUint16(resp[13:], 2)         // 参数长度
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

// 请求写入自增 reference，应答回显 → 正常读取；两次调用的 reference 必须不同。
func TestAGReadDB_PduReferenceIncrementsAndMatches(t *testing.T) {
	h, server := newFakePLCPair(t)
	c := NewClient(h)
	data := []byte{0xAA, 0xBB}

	readOnce := func() []byte {
		reqCh := make(chan []byte, 1)
		errCh := make(chan error, 1)
		go func() {
			req, err := serveOneRead(server, data, 0)
			if err != nil {
				errCh <- err
				return
			}
			reqCh <- req
		}()
		buf := make([]byte, 2)
		if err := c.AGReadDB(1, 0, 2, buf); err != nil {
			t.Fatalf("读失败: %v", err)
		}
		if !bytes.Equal(buf, data) {
			t.Fatalf("数据不符: % X", buf)
		}
		select {
		case req := <-reqCh:
			return req
		case err := <-errCh:
			t.Fatalf("服务端出错: %v", err)
		}
		return nil
	}

	req1 := readOnce()
	req2 := readOnce()
	ref1 := binary.BigEndian.Uint16(req1[11:])
	ref2 := binary.BigEndian.Uint16(req2[11:])
	if ref1 == 0 {
		t.Fatal("请求未写入 PDU reference")
	}
	if ref1 == ref2 {
		t.Fatalf("两次请求的 reference 应不同，都是 %d", ref1)
	}
}

// 应答回显不一致（残留应答）→ 返回 ErrStaleResponse，而不是把上一次的数据当成本次结果。
func TestAGReadDB_StaleResponseDetected(t *testing.T) {
	h, server := newFakePLCPair(t)
	c := NewClient(h)

	go func() { _, _ = serveOneRead(server, []byte{1, 2}, 1) }() // 回显 ref+1

	buf := make([]byte, 2)
	err := c.AGReadDB(1, 0, 2, buf)
	if !errors.Is(err, ErrStaleResponse) {
		t.Fatalf("应识别为残留应答，实际: %v（buf=% X）", err, buf)
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

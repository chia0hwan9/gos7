package gos7

// 计数器/定时器写的 transport size 回归。
//
// 从 Snap7 C 代码移植时留下 "case S7WLCounter: case S7WLTimer: TS = TS_ResOctet; break;"
// 这种写法，Go 里空 case **不会** fallthrough —— 计数器走进去什么也没做，于是发出去的
// data item transport size 是 0（非法），PLC 会拒。timer 恰好命中下一行所以没事。
// 合并成 `case s7wlcounter, s7wltimer:` 后两者都按 octet string（0x09）传，长度按**字节**。

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

// writeOnce 连一对假 PLC 发一次写，返回 PLC 收到的请求报文。
func writeOnce(t *testing.T, do func(c Client) error) ([]byte, error) {
	t.Helper()
	h, server := newFakePLCPair(t)
	c := NewClient(h)

	reqCh := make(chan []byte, 1)
	go func() {
		req, err := serveOneWrite(server, 0xFF)
		if err != nil {
			reqCh <- nil
			return
		}
		reqCh <- req
	}()

	err := do(c)
	select {
	case req := <-reqCh:
		return req, err
	case <-time.After(2 * time.Second):
		t.Fatal("假 PLC 未收到请求")
		return nil, err
	}
}

// 单个计数器写：参数项 wordlen = 0x1C，data item TS = 0x09（octet string），长度按字节。
func TestAGWriteCT_TransportSizeOctet(t *testing.T) {
	req, err := writeOnce(t, func(c Client) error {
		return c.AGWriteCT(0, 1, []byte{0x42})
	})
	if err != nil {
		t.Fatalf("写计数器失败: %v", err)
	}
	if len(req) != 37 {
		t.Fatalf("报文长度 = %d，应为 35(头)+2(计数器)", len(req))
	}
	if req[17] != 0x05 || req[18] != 0x01 {
		t.Fatalf("function/items = %#x/%#x，应为 WriteVar/1", req[17], req[18])
	}
	if req[22] != byte(s7wlcounter) {
		t.Fatalf("参数项 word length = %#x，应为计数器(%#x)", req[22], s7wlcounter)
	}
	if req[27] != byte(s7areact) {
		t.Fatalf("area = %#x，应为计数器区(%#x)", req[27], s7areact)
	}
	if req[32] != tsResOctet {
		t.Fatalf("data item transport size = %#x，应为 octet string(%#x)；0 表示空 case 没赋值", req[32], tsResOctet)
	}
	if bits := binary.BigEndian.Uint16(req[33:]); bits != 2 {
		t.Fatalf("data length = %d，计数器按字节计应为 2", bits)
	}
	// AGWriteCT 把 value 拆成 2 字节：低字节在前、高字节在后（BCD 计数器的线上格式）
	if want := []byte{0x42, 0x00}; !bytes.Equal(req[35:37], want) {
		t.Fatalf("计数器载荷 = % X，应为 % X", req[35:37], want)
	}
	// 请求带自增 PDU reference（上游 send 统一盖）
	if binary.BigEndian.Uint16(req[s7PDURefOffset:]) == 0 {
		t.Fatal("请求未盖 PDU reference")
	}
}

// 单个定时器写：同样按 octet string 传（timer 原本就命中下一行，这里钉住别被改坏）。
func TestAGWriteTM_TransportSizeOctet(t *testing.T) {
	req, err := writeOnce(t, func(c Client) error {
		return c.AGWriteTM(0, 1, []byte{0x10})
	})
	if err != nil {
		t.Fatalf("写定时器失败: %v", err)
	}
	if req[22] != byte(s7wltimer) {
		t.Fatalf("参数项 word length = %#x，应为定时器(%#x)", req[22], s7wltimer)
	}
	if req[32] != tsResOctet {
		t.Fatalf("data item transport size = %#x，应为 octet string(%#x)", req[32], tsResOctet)
	}
	if bits := binary.BigEndian.Uint16(req[33:]); bits != 2 {
		t.Fatalf("data length = %d，定时器按字节计应为 2", bits)
	}
}

// 多变量写的计数器分支同样必须是 octet string（multi.go 里同一处移植缺陷）。
func TestAGWriteMulti_CounterTransportSizeOctet(t *testing.T) {
	req, err := writeOnce(t, func(c Client) error {
		return c.AGWriteMulti([]S7DataItem{{
			Area:    s7areact,
			WordLen: s7wlcounter,
			Start:   0,
			Amount:  1,
			Data:    []byte{0x42, 0x00},
		}}, 1)
	})
	if err != nil {
		t.Fatalf("多变量写计数器失败: %v", err)
	}
	// 31 = 19(TPKT+COTP+S7 头) + 12(参数项)；数据项：reserved / TS / 长度(2B) / 载荷
	if req[31] != 0x00 {
		t.Fatalf("data item reserved = %#x，应为 0", req[31])
	}
	if req[32] != tsResOctet {
		t.Fatalf("data item transport size = %#x，应为 octet string(%#x)；0 表示空 case 没赋值", req[32], tsResOctet)
	}
	if n := binary.BigEndian.Uint16(req[33:]); n != 2 {
		t.Fatalf("data length = %d，计数器按字节计应为 2", n)
	}
	if want := []byte{0x42, 0x00}; !bytes.Equal(req[35:37], want) {
		t.Fatalf("计数器载荷 = % X，应为 % X", req[35:37], want)
	}
}

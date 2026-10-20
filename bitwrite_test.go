package gos7

// fork 新增：DB 位写（原生 WriteVar，transport size = BIT）的报文级回归测试。
//
// 目的：钉住"位写只发一条 WriteVar、且不读回"这一语义。连接器原来只能整字节写，
// 于是不得不"读整字节 → 改目标位 → 写整字节"，窗口内会覆盖同字节的其它位。

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// serveOneWrite 读一个完整的写请求，按 S7 写应答（22 字节）回应并回显请求的 reference。
// itemReturn = 0xFF 表示该 item 成功，其它值模拟 PLC 拒绝该项。
func serveOneWrite(server net.Conn, itemReturn byte) ([]byte, error) {
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

	// S7 写应答：TPKT+COTP(7) + S7 头(10) + 错误类/码(2) + 参数(2) + item 返回码(1)
	resp := []byte{
		3, 0, 0, 22, 2, 240, 128,
		0x32, 0x03, 0, 0, req[11], req[12],
		0, 2, // 参数长度
		0, 1, // 数据长度
		0, 0, // 错误类 / 错误码
		0x05, 0x01, itemReturn,
	}
	_, err := server.Write(resp)
	return req, err
}

// bitWriteOnce 连一对假 PLC 做一次位写，返回客户端错误和 PLC 收到的请求报文。
func bitWriteOnce(t *testing.T, db, byteAdr, bit int, value bool, itemReturn byte) ([]byte, error) {
	t.Helper()
	h, server := newFakePLCPair(t)
	c := NewClient(h)

	reqCh := make(chan []byte, 1)
	go func() {
		req, err := serveOneWrite(server, itemReturn)
		if err != nil {
			reqCh <- nil
			return
		}
		reqCh <- req
	}()

	err := c.AGWriteDBBit(db, byteAdr, bit, value)
	select {
	case req := <-reqCh:
		return req, err
	case <-time.After(2 * time.Second):
		t.Fatal("假 PLC 未收到请求")
		return nil, err
	}
}

// 一条位写报文：参数项 TS=BIT、地址按位编码、数据项 TS=BIT、长度=1(位)、数据 1 字节。
func TestAGWriteDBBit_Telegram(t *testing.T) {
	req, err := bitWriteOnce(t, 1, 4, 6, true, 0xFF)
	if err != nil {
		t.Fatalf("位写失败: %v", err)
	}

	want := []byte{
		// TPKT(len=36) + COTP DT
		3, 0, 0, 36, 2, 240, 128,
		// S7: Job / PDU ref=1 / 参数长度 14 / 数据长度 5
		0x32, 0x01, 0, 0, 0, 1, 0, 14, 0, 5,
		// 参数：WriteVar(0x05) / 1 项 / var spec / TS=BIT(0x01) / count=1 / DB=1 / DB 区(0x84) / 位地址 38
		0x05, 0x01, 0x12, 0x0A, 0x10, 0x01, 0, 1, 0, 1, 0x84, 0, 0, 38,
		// 数据项：reserved / TS=BIT(0x03) / 长度 1(位) / 值 TRUE
		0x00, 0x03, 0x00, 0x01, 0x01,
	}
	if !bytes.Equal(req, want) {
		t.Fatalf("报文不符\n got: % X\nwant: % X", req, want)
	}
}

// 位写之后 PLC 不应再收到任何报文：读-改-写会先发一条 ReadVar。
func TestAGWriteDBBit_NoReadModifyWrite(t *testing.T) {
	h, server := newFakePLCPair(t)
	c := NewClient(h)

	go func() { _, _ = serveOneWrite(server, 0xFF) }()
	if err := c.AGWriteDBBit(1, 0, 0, true); err != nil {
		t.Fatalf("位写失败: %v", err)
	}

	_ = server.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 1)
	if n, err := server.Read(buf); n > 0 || err == nil {
		t.Fatalf("位写后又收到 %d 字节报文，说明存在额外的读：% X", n, buf[:n])
	}
}

// 复位走同一路径；位地址 = 字节*8+位号，长度字段按"位"计。
func TestAGWriteDBBit_ClearMapsToTheRightBit(t *testing.T) {
	req, err := bitWriteOnce(t, 7, 0, 7, false, 0xFF)
	if err != nil {
		t.Fatalf("位写失败: %v", err)
	}
	if len(req) != 36 {
		t.Fatalf("报文长度 %d，应为 36", len(req))
	}
	if got := binary.BigEndian.Uint16(req[25:]); got != 7 {
		t.Fatalf("DB 号 = %d，应为 7", got)
	}
	if req[27] != s7areadb {
		t.Fatalf("area = %#x，应为 %#x", req[27], s7areadb)
	}
	if addr := int(req[28])<<16 | int(req[29])<<8 | int(req[30]); addr != 7 {
		t.Fatalf("位地址 = %d，应为 0*8+7=7", addr)
	}
	if req[32] != tsResBit {
		t.Fatalf("数据项 transport size = %#x，应为 BIT(%#x)", req[32], tsResBit)
	}
	if bits := binary.BigEndian.Uint16(req[33:]); bits != 1 {
		t.Fatalf("数据长度 = %d，应为 1(位)", bits)
	}
	if req[35] != 0x00 {
		t.Fatalf("复位时数据字节 = %#x，应为 0x00", req[35])
	}
}

// 位号越界/字节地址为负必须在发报文之前就被挡住（一字节只有 0-7 位）。
func TestAGWriteDBBit_RejectsInvalidAddress(t *testing.T) {
	c := NewClient(NewTCPClientHandler("127.0.0.1:102", 0, 1)) // 不连接：非法参数不该走到发送

	for _, bit := range []int{-1, 8} {
		err := c.AGWriteDBBit(1, 0, bit, true)
		if err == nil || !strings.Contains(err.Error(), "bit") {
			t.Fatalf("bit=%d 应报位越界，实际: %v", bit, err)
		}
	}
	if err := c.AGWriteDBBit(1, -1, 0, true); err == nil {
		t.Fatal("byteAdr=-1 应报错")
	}
}

// PLC 对 item 返回错误码时必须报错，不能当成功（否则上层会误以为位写生效）。
func TestAGWriteDBBit_PropagatesPLCRejection(t *testing.T) {
	_, err := bitWriteOnce(t, 1, 4, 6, true, 0x0A) // 0x0A = item not available
	if err == nil {
		t.Fatal("PLC 拒绝时不应返回成功")
	}
	if !strings.Contains(err.Error(), "Item not available") {
		t.Fatalf("错误信息应说明 item 不可用，实际: %v", err)
	}
}

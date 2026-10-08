package gos7

import (
	"bytes"
	"errors"
	"testing"
)

// ---- 读取路径不再 panic：短应答 / 缓冲区不足改为返回错误 ----
//
// 上游在 readArea 里直接切 `response.Data[25:25+n]` 与 `buffer[offset:offset+n]`，
// 对端截断应答或调用方给错缓冲区就会越界 panic（在网关里表现为整个进程被带崩）。
// fork 改成返回 ErrShortReadResponse / ErrBufferTooSmall。

// 对端返回的数据比请求少 → 返回 ErrShortReadResponse（旧版 panic）。
func TestReadArea_ShortResponseReturnsError(t *testing.T) {
	h, server := newFakePLCPair(t)
	c := NewClient(h)

	// 客户端请求 10 字节，服务端只回 2 字节
	go func() { _, _ = serveOneReadRef(server, []byte{0xAA, 0xBB}, nil) }()

	buf := make([]byte, 10)
	err := c.AGReadDB(1, 0, 10, buf)
	if !errors.Is(err, ErrShortReadResponse) {
		t.Fatalf("应返回 ErrShortReadResponse，实际: %v（buf=% X）", err, buf)
	}
	if !bytes.Equal(buf, make([]byte, 10)) {
		t.Fatalf("出错时不应写入调用方缓冲区: % X", buf)
	}
}

// 调用方缓冲区不足 → 返回 ErrBufferTooSmall（旧版 panic）。
func TestReadArea_SmallCallerBufferReturnsError(t *testing.T) {
	h, server := newFakePLCPair(t)
	c := NewClient(h)

	go func() { _, _ = serveOneReadRef(server, []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil) }()

	buf := make([]byte, 2) // 请求 8 字节却只给 2 字节
	err := c.AGReadDB(1, 0, 8, buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("应返回 ErrBufferTooSmall，实际: %v", err)
	}
}

// 写入路径同样：调用方缓冲区不足 → 返回 ErrBufferTooSmall（旧版 panic）。
func TestWriteArea_SmallCallerBufferReturnsError(t *testing.T) {
	h, _ := newFakePLCPair(t)
	c := NewClient(h)

	buf := make([]byte, 2) // 声明写 8 字节却只给 2 字节
	err := c.AGWriteDB(1, 0, 8, buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("应返回 ErrBufferTooSmall，实际: %v", err)
	}
}

// responseError 对短报文不能再越界（旧版 len>0 就取 Data[10]/Data[11]）。
func TestResponseError_ShortFramesDoNotPanic(t *testing.T) {
	frames := [][]byte{
		nil,
		{},
		{3, 3},
		{3, 1, 0, 0},
		{3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{3, 7, 1, 2},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3}, // 12 字节，带 Data[1]=3：可安全访问 Data[10..11]
	}
	for i, data := range frames {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("第 %d 个短报文 panic: %v", i, r)
				}
			}()
			_ = responseError(&ProtocolDataUnit{Data: data})
		}()
	}
}

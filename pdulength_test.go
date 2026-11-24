package gos7

// ---- PDU 长度未协商时的收敛 ----
//
// 分片上限 = (PDULength - 头长) / 元素大小。PDULength 只在 Setup Communication
// 成功后才非 0，所以"没 Connect 就读写"或"Connect 失败"时它是 0：
//   - 读路径：numElements 会算成非正数 → 不消耗 totElements → **死循环**；
//   - 写路径：dataSize 为负 → buffer[offset:offset+dataSize] **切片越界 panic**
//     （test/ 里的集成测试连不上 PLC 时正是这样把整个进程带崩的）。
// 现在两路都在算上限前就报 ErrNoPDULength；transporter 不是 *TCPClientHandler 时
// 也不再对 nil 指针取字段。

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// 未 Connect（PDULength = 0）时读不 panic、不死循环，直接报错。
func TestAGReadDB_NoNegotiatedPDULength(t *testing.T) {
	c := NewClient(NewTCPClientHandler("127.0.0.1:102", 0, 1)) // 未 Connect

	done := make(chan error, 1)
	go func() { done <- c.AGReadDB(1, 0, 2, make([]byte, 2)) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNoPDULength) {
			t.Fatalf("应返回 ErrNoPDULength，实际: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AGReadDB 没有返回：分片上限非正时 numElements 不消耗元素，会死循环")
	}
}

// 未 Connect（PDULength = 0）时写不 panic，直接报错。
func TestAGWriteDB_NoNegotiatedPDULength(t *testing.T) {
	c := NewClient(NewTCPClientHandler("127.0.0.1:102", 0, 1)) // 未 Connect

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("AGWriteDB panic 了（负长度切片）: %v", r)
			}
		}()
		err := c.AGWriteDB(1, 0, 2, make([]byte, 2))
		if !errors.Is(err, ErrNoPDULength) {
			t.Fatalf("应返回 ErrNoPDULength，实际: %v", err)
		}
	}()
}

// 多变量读写在未协商时同样报 ErrNoPDULength（原来对 nil *TCPClientHandler 取字段会 panic）。
func TestAGMulti_NoNegotiatedPDULength(t *testing.T) {
	c := NewClient(NewTCPClientHandler("127.0.0.1:102", 0, 1)) // 未 Connect

	read := []S7DataItem{{Area: s7areadb, WordLen: s7wlbyte, DBNumber: 1, Start: 0, Amount: 2, Data: make([]byte, 2)}}
	write := []S7DataItem{{Area: s7areadb, WordLen: s7wlbyte, DBNumber: 1, Start: 0, Amount: 2, Data: make([]byte, 2)}}

	if err := c.AGReadMulti(read, 1); !errors.Is(err, ErrNoPDULength) {
		t.Fatalf("AGReadMulti 应返回 ErrNoPDULength，实际: %v", err)
	}
	if err := c.AGWriteMulti(write, 1); !errors.Is(err, ErrNoPDULength) {
		t.Fatalf("AGWriteMulti 应返回 ErrNoPDULength，实际: %v", err)
	}
}

// 自定义 transporter（不是 *TCPClientHandler）：原来这里对 nil 指针取 PDULength 会 panic。
type nonTCPHandler struct {
	packager    Packager
	transporter Transporter
}

func (h nonTCPHandler) Verify(request, response []byte) error { return nil }
func (h nonTCPHandler) Send(request []byte) ([]byte, error) {
	return nil, errors.New("not used")
}

func TestReadWrite_NonTCPTransporterDoesNotPanic(t *testing.T) {
	c := NewClient(nonTCPHandler{})

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("非 TCP transporter 上 panic 了: %v", r)
			}
		}()
		err := c.AGReadDB(1, 0, 2, make([]byte, 2))
		if !errors.Is(err, ErrNoPDULength) {
			t.Fatalf("应返回 ErrNoPDULength，实际: %v", err)
		}
		err = c.AGWriteDB(1, 0, 2, make([]byte, 2))
		if !errors.Is(err, ErrNoPDULength) {
			t.Fatalf("应返回 ErrNoPDULength，实际: %v", err)
		}
	}()
}

// 错误信息里带上 transporter 类型，现场能看出是"没协商"还是"用了自定义 transporter"。
func TestNoPDULength_ErrorMentionsTransporter(t *testing.T) {
	c := NewClient(NewTCPClientHandler("127.0.0.1:102", 0, 1))
	err := c.AGReadDB(1, 0, 2, make([]byte, 2))
	if err == nil {
		t.Fatal("应报错")
	}
	if !strings.Contains(err.Error(), "TCPClientHandler") {
		t.Fatalf("错误信息应带 transporter 类型，实际: %v", err)
	}
}

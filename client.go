package gos7

// Copyright 2018 Trung Hieu Le. All rights reserved.
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.
import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
)

const (
	// Area ID
	s7areape = 0x81 //process inputs
	s7areapa = 0x82 //process outputs
	s7areamk = 0x83 //Merkers
	s7areadb = 0x84 //DB
	s7areact = 0x1C //counters
	s7areatm = 0x1D //timers

	// Word Length
	s7wlbit     = 0x01 //Bit (inside a word)
	s7wlbyte    = 0x02 //Byte (8 bit)
	s7wlChar    = 0x03
	s7wlword    = 0x04 //Word (16 bit)
	s7wlint     = 0x05
	s7wldword   = 0x06 //Double Word (32 bit)
	s7wldint    = 0x07
	s7wlreal    = 0x08 //Real (32 bit float)
	s7wlcounter = 0x1C //Counter (16 bit)
	s7wltimer   = 0x1D //Timer (16 bit)

	// PLC Status
	s7CpuStatusUnknown = 0
	s7CpuStatusRun     = 8
	s7CpuStatusStop    = 4

	// S7 PDU types (ROSCTR) whose header carries an error class and code
	s7PDUTypeAck     = 0x02
	s7PDUTypeAckData = 0x03

	//size header
	sizeHeaderRead  int = 31 // Header Size when Reading
	sizeHeaderWrite int = 35 // Header Size when Writing

	// Result transport size
	tsResBit   = 3
	tsResByte  = 4
	tsResInt   = 5
	tsResReal  = 7
	tsResOctet = 9
)

//PDULength variable to store pdu length after connect
//var tt, _ := mb.transporter.(*tcpTransporter)tt, _ := mb.transporter.(*tcpTransporter) int //global variable pdulength

// CliePDULengthntHandler is the interface that groups the Packager and Transporter methods.
type ClientHandler interface {
	Packager
	Transporter
}
type client struct {
	packager    Packager
	transporter Transporter
	pduRef      uint32 // running PDU reference, see setPDUReference
}

// NewClient creates a new s7 client with given backend handler.
func NewClient(handler ClientHandler) Client {
	return &client{packager: handler, transporter: handler}
}

// NewClient2 creates a new s7 client with given backend packager and transporter.
func NewClient2(packager Packager, transporter Transporter) Client {
	return &client{packager: packager, transporter: transporter}
}

//implement of the interface AGReadDB
func (mb *client) AGReadDB(dbnumber int, start int, size int, buffer []byte) (err error) {
	return mb.readArea(s7areadb, dbnumber, start, size, s7wlbyte, buffer)
}

//implement of the interface AGWriteDB
func (mb *client) AGWriteDB(dbNumber int, start int, size int, buffer []byte) (err error) {
	return mb.writeArea(s7areadb, dbNumber, start, size, s7wlbyte, buffer)
}

// AGWriteDBBit 以原生位写的方式写 DB 里的一个位（DBx.DBX<byteAdr>.<bit>）。
//
// 发出去的是单条 WriteVar：参数项 transport size = BIT(0x01)、地址按**位**编码
// （byteAdr*8+bit）、数据项 transport size = BIT(0x03)、长度 = 1(位)、数据 1 字节
// （置位 0x01 / 复位 0x00）。与 Snap7/Sharp7 的 WriteArea(S7WLBit)、S7.Net 的
// WriteBit(BitPackage) 编码一致。
//
// 因此不需要"读整字节 → 改目标位 → 写整字节"：PLC 侧只有一次按位访问，
// 同字节里其它位（包括 PLC 程序/HMI 在此期间改过的位）不会被子写覆盖。
func (mb *client) AGWriteDBBit(dbNumber int, byteAdr int, bit int, value bool) (err error) {
	if bit < 0 || bit > 7 {
		return fmt.Errorf("s7: bit %d is out of range, a byte has bits 0-7", bit)
	}
	if byteAdr < 0 {
		return fmt.Errorf("s7: byte address %d is invalid", byteAdr)
	}
	var payload byte
	if value {
		payload = 0x01
	}
	// writeArea 对 s7wlbit 固定只发 1 个元素，并把 start 当**位地址**解释。
	return mb.writeArea(s7areadb, dbNumber, byteAdr*8+bit, 1, s7wlbit, []byte{payload})
}

//implement of the interface AGReadMB
func (mb *client) AGReadMB(start int, size int, buffer []byte) (err error) {
	return mb.readArea(s7areamk, 0, start, size, s7wlbyte, buffer)
}

//implement of the interface AGWriteMB
func (mb *client) AGWriteMB(start int, size int, buffer []byte) (err error) {
	return mb.writeArea(s7areamk, 0, start, size, s7wlbyte, buffer)
}

//implement of the interface AGReadEB
func (mb *client) AGReadEB(start int, size int, buffer []byte) (err error) {
	return mb.readArea(s7areape, 0, start, size, s7wlbyte, buffer)
}

//implement of the interface AGWriteEB
func (mb *client) AGWriteEB(start int, size int, buffer []byte) (err error) {
	return mb.writeArea(s7areape, 0, start, size, s7wlbyte, buffer)
}

//implement of the interface AGReadAB
func (mb *client) AGReadAB(start int, size int, buffer []byte) (err error) {
	return mb.readArea(s7areapa, 0, start, size, s7wlbyte, buffer)
}

//implement of the interface AGWriteAB
func (mb *client) AGWriteAB(start int, size int, buffer []byte) (err error) {
	return mb.writeArea(s7areapa, 0, start, size, s7wlbyte, buffer)
}

//implement of the interface AGReadTM - read timer
func (mb *client) AGReadTM(start int, amount int, buffer []byte) (err error) {
	sbuffer := make([]byte, amount*2)
	err = mb.readArea(s7areatm, 0, start, amount, s7wltimer, sbuffer)
	if err == nil {
		for c := 0; c < amount; c++ {
			buffer[c] = byte(uint16(sbuffer[c*2+1])<<8 + uint16(sbuffer[c*2]))
		}
	}
	return err
}

//implement of the interface AGWriteTM - write timer
func (mb *client) AGWriteTM(start int, amount int, buffer []byte) (err error) {
	sbuffer := make([]byte, amount*2)
	for c := 0; c < amount; c++ {
		sbuffer[c*2+1] = byte((uint(buffer[c]) & uint(0xFF00)) >> 8)
		sbuffer[c*2] = byte(buffer[c] & 0x00FF)
	}
	err = mb.writeArea(s7areatm, 0, start, amount, s7wltimer, sbuffer)
	return err
}

//implement of the interface AGReadCT - read counter
func (mb *client) AGReadCT(start int, amount int, buffer []byte) (err error) {
	sbuffer := make([]byte, amount*2)
	err = mb.readArea(s7areact, 0, start, amount, s7wlcounter, sbuffer)
	if err == nil {
		for c := 0; c < amount; c++ {
			buffer[c] = byte(uint(sbuffer[c*2+1])<<8 + uint(sbuffer[c*2]))
		}
	}
	return err
}

//implement of the interface AGWriteCT - write counter
func (mb *client) AGWriteCT(start int, amount int, buffer []byte) (err error) {
	sbuffer := make([]byte, amount*2)
	for c := 0; c < amount; c++ {
		sbuffer[c*2+1] = byte((uint(buffer[c]) & uint(0xFF00)) >> 8)
		sbuffer[c*2] = byte(buffer[c] & 0x00FF)
	}
	err = mb.writeArea(s7areact, 0, start, amount, s7wlcounter, sbuffer)
	return err
}

//read generic area, pass result into a buffer
func (mb *client) readArea(area int, dbNumber int, start int, amount int, wordLen int, buffer []byte) (err error) {
	var address, numElements, maxElements, totElements, sizeRequested int
	offset := 0
	wordSize := 1
	// Some adjustment
	if area == s7areact {
		wordLen = s7wlcounter
	}
	if area == s7areatm {
		wordLen = s7wltimer
	}
	// Calc Word size
	wordSize = dataSizeByte(wordLen)
	if wordSize == 0 {
		return fmt.Errorf(ErrorText(errIsoInvalidDataSize))
	}

	if wordLen == s7wlbit {
		amount = 1 // Only 1 bit can be transferred at time
	} else {
		if wordLen != s7wlcounter && wordLen != s7wltimer {
			amount = amount * wordSize
			wordSize = 1
			wordLen = s7wlbyte
		}
	}

	// PDULength 来自 Setup Communication 协商；没连上/协商失败时是 0。用它算分片上限会得到
	// 非正数，读路径会 numElements<=0 而不消耗 totElements（死循环），写路径会负长度切片 panic。
	maxElements = (mb.pduLength() - 18) / wordSize // 18 = Reply telegram header //lth note here
	if maxElements < 1 {
		return fmt.Errorf("%w (transporter %T)", ErrNoPDULength, mb.transporter)
	}
	totElements = amount
	for totElements > 0 && err == nil {
		numElements = totElements
		if numElements > maxElements {
			numElements = maxElements
		}
		if numElements < 1 {
			// 兜底：任何情况下都不允许"一次请求 0/负个元素"（否则 totElements 不减，死循环）。
			return fmt.Errorf("%w: %d elements per request", ErrNoPDULength, numElements)
		}

		sizeRequested = numElements * wordSize
		// Setup the telegram
		requestData := make([]byte, sizeHeaderRead)
		copy(requestData[0:], s7ReadWriteTelegram[0:])
		request := NewProtocolDataUnit(requestData)
		// Set DB Number
		request.Data[27] = byte(area)
		// Set Area
		if area == s7areadb {
			binary.BigEndian.PutUint16(request.Data[25:], uint16(dbNumber))
			//SetWordAt(request.Data, 25, uint16(DBNumber))
		}

		// Adjusts Start and word length
		if wordLen == s7wlbit || wordLen == s7wlcounter || wordLen == s7wltimer {
			address = start
			request.Data[22] = byte(wordLen)
		} else {
			address = start << 3
		}
		// Num elements
		binary.BigEndian.PutUint16(request.Data[23:], uint16(numElements))
		//SetWordAt(request.Data, 23, uint16(numElements))
		// Address into the PLC (only 3 bytes)
		request.Data[30] = byte(address & 0x0FF)
		address = address >> 8
		request.Data[29] = byte(address & 0x0FF)
		address = address >> 8
		request.Data[28] = byte(address & 0x0FF)
		var response *ProtocolDataUnit
		response, sendError := mb.send(&request)
		err = sendError

		if err == nil {
			size := len(response.Data)
			switch {
			case size < 25:
				err = fmt.Errorf(ErrorText(errIsoInvalidDataSize)+"'%v'", size)
			case response.Data[21] != 0xFF:
				err = fmt.Errorf(ErrorText(CPUError(uint(response.Data[21]))))
			case readItemSize(response.Data) != sizeRequested || size < 25+sizeRequested:
				// 数据项声明的长度与请求不符，或报文根本没有携带这么多字节：
				// 上游在这里 response.Data[25:25+sizeRequested] 越界（response.Data 是 2084 字节
				// 接收缓冲的子切片，所以不 panic，而是把上一地址的数据 + 零填充交给调用方）。
				err = fmt.Errorf("%w: %s: requested %d bytes, item holds %d bytes in a %d byte telegram",
					ErrShortReadResponse, ErrorText(errCliInvalidDataSizeRecvd),
					sizeRequested, readItemSize(response.Data), size)
			case offset+sizeRequested > len(buffer):
				// 调用方缓冲区不足：上游在这里 buffer[offset:offset+sizeRequested] 会越界 panic
				err = fmt.Errorf("%w: have %d bytes, need %d (offset %d)",
					ErrBufferTooSmall, len(buffer), offset+sizeRequested, offset)
			default:
				//copy response to buffer
				copy(buffer[offset:offset+sizeRequested], response.Data[25:25+sizeRequested])
				offset += sizeRequested
			}

		}
		totElements -= numElements
		start += numElements * wordSize
	}
	return
}

// readItemSize returns the number of data bytes a Read Var response item
// announces in bytes 23-24. The length field counts bits for the byte, word
// and integer transport sizes and bytes for bit, octet string and real
// (same convention as AGReadMulti).
func readItemSize(response []byte) int {
	size := int(binary.BigEndian.Uint16(response[23:]))
	if ts := response[22]; ts != tsResOctet && ts != tsResReal && ts != tsResBit {
		size >>= 3
	}
	return size
}

//writeArea write generic area into PLC with following parameters:
//1.area: s7areape/s7areapa/s7areamk/s7areadb/s7areact/s7areatm
//2.dbnumber: specify dbnumber, to use in write DB area, otherwise = 0
//3.start: start of the address
//4.amount: amount of the address
//5.wordlen: bit/byte/word/dword/real/counter/timer
//6.buffer: a byte array input for writing
func (mb *client) writeArea(area int, dbnumber int, start int, amount int, wordlen int, buffer []byte) (err error) {
	var address, numElements, maxElements, totElements, dataSize, isoSize, length int
	offset := 0
	wordSize := 1

	// Some adjustment
	if area == s7areact {
		wordlen = s7wlcounter
	}
	if area == s7areatm {
		wordlen = s7wltimer
	}

	// Calc Word size
	wordSize = dataSizeByte(wordlen)
	if wordSize == 0 {
		return fmt.Errorf(ErrorText(errIsoInvalidDataSize))
	}

	if wordlen == s7wlbit {
		amount = 1 // Only 1 bit can be transferred at time
	} else {
		if wordlen != s7wlcounter && wordlen != s7wltimer {
			amount = amount * wordSize
			wordSize = 1
			wordlen = s7wlbyte
		}
	}
	maxElements = (mb.pduLength() - 35) / wordSize // 35 = Reply telegram header
	if maxElements < 1 {
		// 未协商/协商失败：负数会让 dataSize 为负，随后 buffer[offset:offset+dataSize] 直接 panic。
		return fmt.Errorf("%w (transporter %T)", ErrNoPDULength, mb.transporter)
	}
	totElements = amount
	for totElements > 0 && err == nil {
		numElements = totElements
		if numElements > maxElements {
			numElements = maxElements
		}
		if numElements < 1 {
			return fmt.Errorf("%w: %d elements per request", ErrNoPDULength, numElements)
		}
		dataSize = numElements * wordSize
		isoSize = sizeHeaderWrite + dataSize

		// Setup the telegram
		requestData := make([]byte, sizeHeaderWrite)
		copy(requestData[0:], s7ReadWriteTelegram[0:])

		request := NewProtocolDataUnit(requestData)
		// Whole telegram Size
		binary.BigEndian.PutUint16(request.Data[2:], uint16(isoSize))
		//SetWordAt(request.Data, 2, uint16(isoSize))
		// Data length
		length = dataSize + 4
		binary.BigEndian.PutUint16(request.Data[15:], uint16(length))
		// SetWordAt(request.Data, 15, uint16(length))
		// Function
		request.Data[17] = byte(0x05)
		// Set DB Number
		request.Data[27] = byte(area)
		if area == s7areadb {
			binary.BigEndian.PutUint16(request.Data[25:], uint16(dbnumber))
			//SetWordAt(request.Data, 25, uint16(dbnumber))
		}
		// Adjusts start and word length
		if wordlen == s7wlbit || wordlen == s7wlcounter || wordlen == s7wltimer {
			address = start
			length = dataSize
			request.Data[22] = byte(wordlen)
		} else {
			address = start << 3
			length = dataSize << 3
		}

		// Num elements
		binary.BigEndian.PutUint16(request.Data[23:], uint16(numElements))
		// SetWordAt(request.Data, 23, uint16(numElements))
		// address into the PLC
		request.Data[30] = byte(address & 0x0FF)
		address = address >> 8
		request.Data[29] = byte(address & 0x0FF)
		address = address >> 8
		request.Data[28] = byte(address & 0x0FF)

		// Transport Size
		switch wordlen {
		case s7wlbit:
			request.Data[32] = tsResBit
		case s7wlcounter, s7wltimer:
			// 计数器/定时器按 octet string 传（长度按字节计）；上游把这个移植成了
			// "case counter: 空 case + case timer:"，而 Go 的空 case 不会 fallthrough，
			// 于是计数器写发出的 transport size 是 0（非法）。
			request.Data[32] = tsResOctet
		default:
			request.Data[32] = tsResByte // byte/word/dword etc.
		}
		// length
		// SetWordAt(request.Data, 33, uint16(length))
		binary.BigEndian.PutUint16(request.Data[33:], uint16(length))

		//expand values into array
		if offset+dataSize > len(buffer) {
			// 调用方缓冲区不足：上游在这里 buffer[offset:offset+dataSize] 会越界 panic
			err = fmt.Errorf("%w: have %d bytes, need %d (offset %d)",
				ErrBufferTooSmall, len(buffer), offset+dataSize, offset)
			return
		}
		request.Data = append(request.Data[:35], append(buffer[offset:offset+dataSize], request.Data[35:]...)...)
		response, sendError := mb.send(&request)
		err = sendError
		if err == nil {
			if length = len(response.Data); length == 22 {
				if response.Data[21] != byte(0xFF) {
					err = fmt.Errorf(ErrorText(CPUError(uint(response.Data[21]))))
				}
			} else {
				err = fmt.Errorf(ErrorText(errIsoInvalidPDU))
			}

		}
		offset += dataSize
		totElements -= numElements
		start += numElements * wordSize
	}
	return
}

//DBRead
func (mb *client) Read(variable string, buffer []byte) (value interface{}, err error) {
	variable = strings.ToUpper(variable)              //upper
	variable = strings.Replace(variable, " ", "", -1) //remove spaces

	if variable == "" {
		err = fmt.Errorf("input variable is empty, variable should be S7 syntax")
		return
	}
	//var area, dbNumber, start, amount, wordLen int
	switch valueArea := variable[0:2]; valueArea {
	case "EB": //input byte
	case "EW": //input word
	case "ED": //Input double-word
	case "AB": //Output byte
	case "AW": //Output word
	case "AD": //Output double-word
	case "MB": //Memory byte
	case "MW": //Memory word
	case "MD": //Memory double-word
	case "DB": //Data Block
		dbArray := strings.Split(variable, ".")
		if len(dbArray) < 2 {
			err = fmt.Errorf("Db Area read variable should not be empty")
			return
		}
		dbNo, _ := strconv.ParseInt(string(string(dbArray[0])[2:]), 10, 16)
		dbIndex, _ := strconv.ParseInt(string(string(dbArray[1])[3:]), 10, 16)
		dbType := string(dbArray[1])[0:3]

		switch dbType {
		case "DBB": //byte
			err = mb.AGReadDB(int(dbNo), int(dbIndex), 1, buffer)
			value = buffer[0]
			return
		case "DBW": //word
			err = mb.AGReadDB(int(dbNo), int(dbIndex), 2, buffer)
			value = binary.BigEndian.Uint16(buffer[0:])
			return
		case "DBD": //dword
			err = mb.AGReadDB(int(dbNo), int(dbIndex), 4, buffer)
			value = binary.BigEndian.Uint32(buffer[0:])
			return
		case "DBX": //bit
			mBit, _ := strconv.ParseInt(string(string(dbArray[2])[0:]), 10, 16)
			if mBit > 7 || mBit < 0 {
				err = fmt.Errorf("Db read bit is invalid")
				return
			}
			err = mb.AGReadDB(int(dbNo), int(dbIndex), 1, buffer)
			mask := []byte{0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80}
			value = buffer[0] & mask[mBit]
			return
		default:
			err = fmt.Errorf("error when parsing dbtype")
			return
		}
	default:
		switch otherArea := variable[0:1]; otherArea {
		case "E":
		case "I": //input
		case "A":
		case "0": //output
		case "M": //memory
		case "T": //timer
			startByte, _ := strconv.ParseInt(string(variable[1:]), 10, 16)
			err = mb.AGReadTM(int(startByte), 1, buffer)
			if err != nil {
				return
			}
			helper := Helper{}
			helper.GetValueAt(buffer, 0, value)
			return
		case "Z":
		case "C": //counter
			startByte, _ := strconv.ParseInt(string(variable[1:]), 10, 16)
			err = mb.AGReadCT(int(startByte), 1, buffer)
			if err != nil {
				return
			}
			helper := Helper{}
			helper.GetValueAt(buffer, 0, value)
			return
		default:
			err = fmt.Errorf("error when parsing db area")
			return
		}

	}
	return
}

//send the package of a pdu request and a pdu response, check for response error and verify the package
func (mb *client) send(request *ProtocolDataUnit) (response *ProtocolDataUnit, err error) {
	mb.setPDUReference(request.Data)
	dataResponse, err := mb.transporter.Send(request.Data)
	if err != nil {
		return
	}

	if err = mb.packager.Verify(request.Data, dataResponse); err != nil {
		return
	}
	if dataResponse == nil || len(dataResponse) == 0 {
		// Empty response
		err = fmt.Errorf("s7: response data is empty")
		return
	}
	response = &ProtocolDataUnit{
		Data: dataResponse,
	}
	//check for error if any
	err = responseError(response)
	return response, err
}

// setPDUReference stamps the next PDU reference into an S7 request. The
// telegram templates carry a fixed reference, which makes every reply look
// like the answer to every request; with a running reference the transporter
// and the packager can tell a late reply from the expected one.
func (mb *client) setPDUReference(request []byte) {
	if !isS7PDU(request) {
		return
	}
	ref := uint16(atomic.AddUint32(&mb.pduRef, 1))
	binary.BigEndian.PutUint16(request[s7PDURefOffset:], ref)
}

// pduLength 返回 Setup Communication 协商出的 PDU 长度，未协商/不可用时返回 0。
//
// 读/写路径都用它算"单条报文最多几个元素"：没连上就调用 Read/Write（或协商失败）时
// PDULength 是 0，直接用会算出非正的上限 —— 读路径不消耗元素（死循环），写路径负长度
// 切片（panic）。transporter 不是 *TCPClientHandler（NewClient2 传自定义实现）时同样返回 0，
// 而不是像原来那样对 nil 指针取字段。
func (mb *client) pduLength() int {
	if tt, ok := interface{}(mb.transporter).(*TCPClientHandler); ok && tt != nil {
		return tt.PDULength
	}
	return 0
}

// responseError returns the error class and code from the header of an Ack or
// Ack_Data reply as an S7Error, or nil when the reply reports no error. Only
// these two PDU types carry the error fields, at bytes 17-18 after the TPKT
// and COTP headers.
//
// 上游原先按应答第 1 字节（TPKT reserved，恒为 0）分支、并在第 10-11 字节取错误码，
// 所以永远不返回错误；空 case 也不会 fallthrough。这里同时保留"报文过短不越界"的保证：
// 长度不足 isoHSize+12 时无从取错误信息，返回 nil。
func responseError(response *ProtocolDataUnit) error {
	if response == nil {
		return nil
	}
	data := response.Data
	if len(data) < isoHSize+12 || data[isoHSize] != s7ProtocolID {
		return nil
	}
	switch data[isoHSize+1] {
	case s7PDUTypeAck, s7PDUTypeAckData:
		if class, code := data[17], data[18]; class != 0 || code != 0 {
			return &S7Error{High: class, Low: code}
		}
	}
	return nil
}

//dataSize to number of byte accordingly
func dataSizeByte(wordLength int) int {
	switch wordLength {
	case s7wlbit:
		return 1
	case s7wlbyte:
		return 1
	case s7wlChar:
		return 1
	case s7wlword:
		return 2
	case s7wlint:
		return 2
	case s7wlcounter:
		return 2
	case s7wltimer:
		return 2
	case s7wldword:
		return 4
	case s7wldint:
		return 4
	case s7wlreal:
		return 4
	default:
		return 0
	}

}

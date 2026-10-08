package gos7

// Copyright 2018 Trung Hieu Le. All rights reserved.
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.
import (
	"errors"
	"strconv"
)

// ErrStaleResponse 应答回显的 PDU reference 与请求不一致：读到的不是本次请求的应答。
//
// 典型来源：上一次收发超时后，PLC 迟到的应答还留在 socket 缓冲里，被下一次收发当成
// 自己的应答读走。放任它继续解析会**静默得到上一次请求的数据**（甚至切片越界 panic），
// 所以这里显式报错，调用方应丢弃连接后重连（go-iidc 的 S7 连接器把它当作断线）。
var ErrStaleResponse = errors.New("s7: stale response (PDU reference mismatch)")

// ErrShortReadResponse 应答携带的数据少于请求（对端截断了应答）。
//
// 上游在这里直接 `copy(buf, response.Data[25:25+n])` —— 越界 panic（网关侧表现为
// 整个进程被带崩）。现在返回错误：帧长度是按 TPKT 读完的、流仍然对齐，调用方把相关点
// 标 BAD 即可，不必断连。
var ErrShortReadResponse = errors.New("s7: short read response")

// ErrBufferTooSmall 调用方给的缓冲区装不下请求/写入的数据（调用方误用；上游同样会切片越界 panic）。
var ErrBufferTooSmall = errors.New("s7: buffer too small")

const (
	errTCPSocketCreation    = 1
	errTCPConnectionTimeout = 2
	errTCPConnectionFailed  = 3
	errTCPReceiveTimeout    = 4
	errTCPDataReceive       = -5
	errTCPSendTimeout       = 0x00000006
	errTCPDataSend          = 0x00000007
	errTCPConnectionReset   = 0x00000008
	errTCPNotConnected      = 0x00000009
	errTCPUnreachableHost   = 0x00002751

	errIsoConnect         = 0x00010000 // Connection error
	errIsoInvalidPDU      = 0x00030000 // Bad format
	errIsoInvalidDataSize = 0x00040000 // Bad Datasize passed to send/recv : buffer is invalid

	errCliNegotiatingPDU         = 0x00100000
	errCliInvalidParams          = 0x00200000
	errCliJobPending             = 0x00300000
	errCliTooManyItems           = 0x00400000
	errCliInvalidWordLen         = 0x00500000
	errCliPartialDataWritten     = 0x00600000
	errCliSizeOverPDU            = 0x00700000
	errCliInvalidPlcAnswer       = 0x00800000
	errCliAddressOutOfRange      = 0x00900000
	errCliInvalidTransportSize   = 0x00A00000
	errCliWriteDataSizeMismatch  = 0x00B00000
	errCliItemNotAvailable       = 0x00C00000
	errCliInvalidValue           = 0x00D00000
	errCliCannotStartPLC         = 0x00E00000
	errCliAlreadyRun             = 0x00F00000
	errCliCannotStopPLC          = 0x01000000
	errCliCannotCopyRAMToRom     = 0x01100000
	errCliCannotCompress         = 0x01200000
	errCliAlreadyStop            = 0x01300000
	errCliFunNotAvailable        = 0x01400000
	errCliUploadSequenceFailed   = 0x01500000
	errCliInvalidDataSizeRecvd   = 0x01600000
	errCliInvalidBlockType       = 0x01700000
	errCliInvalidBlockNumber     = 0x01800000
	errCliInvalidBlockSize       = 0x01900000
	errCliNeedPassword           = 0x01D00000
	errCliInvalidPassword        = 0x01E00000
	errCliNoPasswordToSetOrClear = 0x01F00000
	errCliJobTimeout             = 0x02000000
	errCliPartialDataRead        = 0x02100000
	errCliBufferTooSmall         = 0x02200000
	errCliFunctionRefused        = 0x02300000
	errCliDestroying             = 0x02400000
	errCliInvalidParamNumber     = 0x02500000
	errCliCannotChangeParam      = 0x02600000
	errCliFunctionNotImplemented = 0x02700000

	code7Ok                    = 0
	code7AddressOutOfRange     = 5
	code7InvalidTransportSize  = 6
	code7WriteDataSizeMismatch = 7
	code7ResItemNotAvailable   = 10
	code7ResItemNotAvailable1  = 53769
	code7InvalidValue          = 56321
	code7NeedPassword          = 53825
	code7InvalidPassword       = 54786
	code7NoPasswordToClear     = 54788
	code7NoPasswordToSet       = 54789
	code7FunNotAvailable       = 33028
	code7DataOverPDU           = 34048
)

//ErrorText return a string error text from error code integer
func ErrorText(err int) string {
	switch err {
	case 0:
		return "OK"
	case errTCPSocketCreation:
		return "SYS : Error creating the Socket"
	case errTCPConnectionTimeout:
		return "TCP : Connection Timeout"
	case errTCPConnectionFailed:
		return "TCP : Connection Error"
	case errTCPReceiveTimeout:
		return "TCP : Data receive Timeout"
	case errTCPDataReceive:
		return "TCP : Error receiving Data"
	case errTCPSendTimeout:
		return "TCP : Data send Timeout"
	case errTCPDataSend:
		return "TCP : Error sending Data"
	case errTCPConnectionReset:
		return "TCP : Connection reset by the Peer"
	case errTCPNotConnected:
		return "CLI : Client not connected"
	case errTCPUnreachableHost:
		return "TCP : Unreachable host"
	case errIsoConnect:
		return "ISO : Connection Error"
	case errIsoInvalidPDU:
		return "ISO : Invalid PDU received"
	case errIsoInvalidDataSize:
		return "ISO : Invalid Buffer passed to Send/Receive"
	case errCliNegotiatingPDU:
		return "CLI : Error in PDU negotiation"
	case errCliInvalidParams:
		return "CLI : invalid param(s) supplied"
	case errCliJobPending:
		return "CLI : Job pending"
	case errCliTooManyItems:
		return "CLI : too may items (>20) in multi read/write"
	case errCliInvalidWordLen:
		return "CLI : invalid WordLength"
	case errCliPartialDataWritten:
		return "CLI : Partial data written"
	case errCliSizeOverPDU:
		return "CPU : total data exceeds the PDU size"
	case errCliInvalidPlcAnswer:
		return "CLI : invalid CPU answer"
	case errCliAddressOutOfRange:
		return "CPU : Address out of range"
	case errCliInvalidTransportSize:
		return "CPU : Invalid Transport size"
	case errCliWriteDataSizeMismatch:
		return "CPU : Data size mismatch"
	case errCliItemNotAvailable:
		return "CPU : Item not available"
	case errCliInvalidValue:
		return "CPU : Invalid value supplied"
	case errCliCannotStartPLC:
		return "CPU : Cannot start PLC"
	case errCliAlreadyRun:
		return "CPU : PLC already RUN"
	case errCliCannotStopPLC:
		return "CPU : Cannot stop PLC"
	case errCliCannotCopyRAMToRom:
		return "CPU : Cannot copy RAM to ROM"
	case errCliCannotCompress:
		return "CPU : Cannot compress"
	case errCliAlreadyStop:
		return "CPU : PLC already STOP"
	case errCliFunNotAvailable:
		return "CPU : Function not available"
	case errCliUploadSequenceFailed:
		return "CPU : Upload sequence failed"
	case errCliInvalidDataSizeRecvd:
		return "CLI : Invalid data size received"
	case errCliInvalidBlockType:
		return "CLI : Invalid block type"
	case errCliInvalidBlockNumber:
		return "CLI : Invalid block number"
	case errCliInvalidBlockSize:
		return "CLI : Invalid block size"
	case errCliNeedPassword:
		return "CPU : Function not authorized for current protection level"
	case errCliInvalidPassword:
		return "CPU : Invalid password"
	case errCliNoPasswordToSetOrClear:
		return "CPU : No password to set or clear"
	case errCliJobTimeout:
		return "CLI : Job Timeout"
	case errCliFunctionRefused:
		return "CLI : function refused by CPU (Unknown error)"
	case errCliPartialDataRead:
		return "CLI : Partial data read"
	case errCliBufferTooSmall:
		return "CLI : The buffer supplied is too small to accomplish the operation"
	case errCliDestroying:
		return "CLI : Cannot perform (destroying)"
	case errCliInvalidParamNumber:
		return "CLI : Invalid Param Number"
	case errCliCannotChangeParam:
		return "CLI : Cannot change this param now"
	case errCliFunctionNotImplemented:
		return "CLI : Function not implemented"
	default:
		return "CLI : Unknown error (" + strconv.Itoa(err) + ")"
	}
}

//CPUError specific CPU error after response
func CPUError(err uint) int {
	switch err {
	case 0:
		return 0
	case code7AddressOutOfRange:
		return errCliAddressOutOfRange
	case code7InvalidTransportSize:
		return errCliInvalidTransportSize
	case code7WriteDataSizeMismatch:
		return errCliWriteDataSizeMismatch
	case code7ResItemNotAvailable, code7ResItemNotAvailable1:
		return errCliItemNotAvailable
	case code7DataOverPDU:
		return errCliSizeOverPDU
	case code7InvalidValue:
		return errCliInvalidValue
	case code7FunNotAvailable:
		return errCliFunNotAvailable
	case code7NeedPassword:
		return errCliNeedPassword
	case code7InvalidPassword:
		return errCliInvalidPassword
	case code7NoPasswordToSet, code7NoPasswordToClear:
		return errCliNoPasswordToSetOrClear
	default:
		return errCliFunctionRefused
	}
	return 0
}

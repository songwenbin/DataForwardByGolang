package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
)

const SRC = "src"
const DST = "dst"

type DataForward struct {
	ConnId string
	SrcIp  net.Conn
	DstIp  net.Conn
}

type DataForwardTable map[string]DataForward

func (self *DataForwardTable) RegisterConnId(connId string) bool {
	dataForward, exist := (*self)[connId]
	if !exist {
		(*self)[connId] = DataForward{connId, nil, nil}
	}

	if dataForward.SrcIp != nil && dataForward.DstIp != nil {
		return false
	}

	return true
}

func (self *DataForwardTable) UnRegisterConnId(connId string) bool {
	dataForward, exist := (*self)[connId]
	if !exist {
		return false
	}

	if dataForward.SrcIp != nil && dataForward.DstIp != nil {
		delete(*self, connId)
	}

	return true
}

func (self *DataForwardTable) RegisterSrcIp(connId string, robotip net.Conn) bool {
	dataForward, exist := (*self)[connId]
	if !exist {
		return false
	}

	if dataForward.SrcIp == nil {
		(*self)[connId] = DataForward{dataForward.ConnId, robotip, dataForward.DstIp}
		return true
	} else {
		return false
	}
}

func (self *DataForwardTable) UnRegisterSrcIp(connId string) bool {
	dataForward, exist := (*self)[connId]
	if !exist {
		return false
	}
	(*self)[connId] = DataForward{dataForward.ConnId, nil, dataForward.DstIp}

	return true
}

func (self *DataForwardTable) RegisterDstIp(connId string, controlip net.Conn) bool {
	dataForward, exist := (*self)[connId]
	if !exist {
		return false
	}

	if dataForward.DstIp == nil {
		(*self)[connId] = DataForward{dataForward.ConnId, dataForward.SrcIp, controlip}
		return true
	} else {
		return false
	}
}

func (self *DataForwardTable) UnRegisterDstIp(connId string) bool {
	dataForward, exist := (*self)[connId]
	if !exist {
		return false
	}
	(*self)[connId] = DataForward{dataForward.ConnId, dataForward.SrcIp, nil}

	return true
}

func HandleConnection(conn net.Conn, table *DataForwardTable) {

	ret, connId := HandleConnectionProtocol(conn, table)

	dataForward, _ := (*table)[connId]
	if ret == true && dataForward.DstIp != nil && dataForward.SrcIp != nil {
		go ForwardMessage(connId, table)
		go DstMonitor(connId, table)
	}
}

func DstMonitor(dataid string, table *DataForwardTable) {
	dataForward, _ := (*table)[dataid]
	buf := make([]byte, 128)
	for {
		_, err := dataForward.DstIp.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Dst connection is closed.")
				table.UnRegisterDstIp(dataForward.ConnId)
				dataForward.DstIp.Close()
			}
			break
		}
	}
}

func ForwardMessage(dataid string, table *DataForwardTable) {
	dataForward, _ := (*table)[dataid]

	buf := make([]byte, 128)
	for {
		nbytes, err := dataForward.SrcIp.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Src connection is closed.")
				table.UnRegisterSrcIp(dataForward.ConnId)
				dataForward.SrcIp.Close()
			} else {
				fmt.Println("Read Error %s\n", err)
			}
			break
		}

		dataForward.DstIp.Write(buf[:nbytes])
	}
}

func HandleConnectionProtocol(conn net.Conn, table *DataForwardTable) (bool, string) {

	connId, err := ReadConnection(conn, '\n')
	if err == io.EOF {
		conn.Close()
		return false, connId
	}

	ret := HandleConnectionId(connId, conn, table)
	if ret == false {
		conn.Close()
		return false, connId
	}

	connType, err := ReadConnection(conn, '\n')
	if err == io.EOF {
		conn.Close()
		return false, connId
	}

	ret = HandleConnectionType(connType, connId, conn, table)
	if ret == false {
		conn.Close()
		return false, connId
	}

	return true, connId
}

func HandleConnectionId(connId string, conn net.Conn, table *DataForwardTable) bool {
	if table.RegisterConnId(connId) {
		conn.Write([]byte("ok"))
	} else {
		fmt.Println("error")
		return false
	}

	return true
}

func HandleConnectionType(connType string, connId string, conn net.Conn, table *DataForwardTable) bool {
	if connType == SRC {
		ret := table.RegisterSrcIp(connId, conn)
		if ret == false {
			return false
		}
	} else if connType == DST {
		ret := table.RegisterDstIp(connId, conn)
		if ret == false {
			return false
		}
	}

	return true
}

func ReadConnection(conn net.Conn, delimeter byte) (string, error) {
	readBytes := make([]byte, 1)
	var buffer bytes.Buffer
	for {
		_, err := conn.Read(readBytes)
		if err != nil {
			return "", err
		}

		readByte := readBytes[0]
		if readByte == delimeter {
			break
		}
		buffer.WriteByte(readByte)
	}

	return buffer.String(), nil
}

func main() {
	var dataForwardTable DataForwardTable
	dataForwardTable = make(DataForwardTable, 10)

	serverListener, _ := net.Listen("tcp", ":2320")

	for {
		conn, _ := serverListener.Accept()
		go HandleConnection(conn, &dataForwardTable)
	}
}

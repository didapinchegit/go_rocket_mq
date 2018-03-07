package rocketmq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

type InvokeCallback func(responseFuture *ResponseFuture)

type ResponseFuture struct {
	responseCommand *RemotingCommand
	sendRequestOK   bool
	err             error
	opaque          int32
	timeoutMillis   int64
	invokeCallback  InvokeCallback
	beginTimestamp  int64
	done            chan bool
}

type RemotingClient interface {
	connect(addr string) (net.Conn, error)
	invokeAsync(addr string, request *RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error
	invokeSync(addr string, request *RemotingCommand, timeoutMillis int64) (*RemotingCommand, error)
	invokeOneway(addr string, request *RemotingCommand, timeoutMillis int64) error
	ScanResponseTable()
	getNameServerAddressList() []string
}

type DefaultRemotingClient struct {
	connTable         map[string]net.Conn
	connTableLock     sync.RWMutex
	responseTable     map[int32]*ResponseFuture
	responseTableLock sync.RWMutex
	namesrvAddrList   []string
	namesrvAddrChosen string
}

func NewDefaultRemotingClient() RemotingClient {
	return &DefaultRemotingClient{
		connTable:       make(map[string]net.Conn),
		responseTable:   make(map[int32]*ResponseFuture),
		namesrvAddrList: make([]string, 0),
	}
}

func (d *DefaultRemotingClient) ScanResponseTable() {
	d.responseTableLock.Lock()
	for seq, response := range d.responseTable {
		if (response.beginTimestamp + 30) <= time.Now().Unix() {
			delete(d.responseTable, seq)
			if response.invokeCallback != nil {
				response.invokeCallback(nil)
				fmt.Printf("remove time out request %v", response)
			}
		}
	}
	d.responseTableLock.Unlock()

}

func (d *DefaultRemotingClient) getAndCreateConn(addr string) (conn net.Conn, ok bool) {
	// TODO optimize algorithm of getting a nameserver connection
	if strings.ContainsAny(addr, ";") {
		namesrvAddrList := strings.Split(addr, ";")
		namesrvAddrListlen := len(namesrvAddrList)
		namesrvAddr, namesrvAddrList := removeOne(rand.Intn(namesrvAddrListlen), namesrvAddrList)

		var err error
		for i := 0; i < namesrvAddrListlen-1; i++ {
			conn, err = d.connect(namesrvAddr)
			if err != nil {
				fmt.Println("getAndCreateConn error, ", err)
				namesrvAddr, namesrvAddrList = removeOne(rand.Intn(namesrvAddrListlen), namesrvAddrList)
			} else {
				d.namesrvAddrChosen = namesrvAddr
				break
			}
		}
		addr = d.namesrvAddrChosen
	}

	d.connTableLock.RLock()
	conn, ok = d.connTable[addr]
	d.connTableLock.RUnlock()

	return
}

func removeOne(index int, namesrvAddrList []string) (namesrvAddr string, newNamesrvAddrList []string) {
	namesrvAddrListlen := len(namesrvAddrList)
	index = rand.Intn(namesrvAddrListlen)
	namesrvAddr = namesrvAddrList[index]
	namesrvAddrList[index] = namesrvAddrList[namesrvAddrListlen-1]
	newNamesrvAddrList = namesrvAddrList[:namesrvAddrListlen-1]

	return
}

func (d *DefaultRemotingClient) connect(addr string) (conn net.Conn, err error) {
	if addr == "" {
		addr = d.namesrvAddrChosen
	}

	d.connTableLock.RLock()
	conn, ok := d.connTable[addr]
	d.connTableLock.RUnlock()
	if !ok {
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		d.connTableLock.Lock()
		d.connTable[addr] = conn
		d.connTableLock.Unlock()
		fmt.Println("connect to:", addr)
		go d.handlerConn(conn, addr)
	}

	return conn, nil
}

func (d *DefaultRemotingClient) invokeSync(addr string, request *RemotingCommand, timeoutMillis int64) (*RemotingCommand, error) {
	conn, ok := d.getAndCreateConn(addr)
	var err error
	if !ok {
		conn, err = d.connect(addr)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
	}

	response := &ResponseFuture{
		sendRequestOK:  false,
		opaque:         request.Opaque,
		timeoutMillis:  timeoutMillis,
		beginTimestamp: time.Now().Unix(),
		done:           make(chan bool),
	}

	header := request.encodeHeader()
	body := request.Body

	d.responseTableLock.Lock()
	d.responseTable[request.Opaque] = response
	d.responseTableLock.Unlock()

	err = d.sendRequest(header, body, conn, addr)
	if err != nil {
		fmt.Println("invokeSync:err", err)
		return nil, err
	}
	select {
	case <-response.done:
		return response.responseCommand, nil
	case <-time.After(3 * time.Second):
		return nil, errors.New("invoke sync timeout")
	}

}

func (d *DefaultRemotingClient) invokeAsync(addr string, request *RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) (err error) {
	d.connTableLock.RLock()
	conn, ok := d.connTable[addr]
	d.connTableLock.RUnlock()

	if !ok {
		conn, err = d.connect(addr)
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	response := &ResponseFuture{
		sendRequestOK:  false,
		opaque:         request.Opaque,
		timeoutMillis:  timeoutMillis,
		beginTimestamp: time.Now().Unix(),
		invokeCallback: invokeCallback,
	}

	d.responseTableLock.Lock()
	d.responseTable[request.Opaque] = response
	d.responseTableLock.Unlock()

	header := request.encodeHeader()
	body := request.Body
	err = d.sendRequest(header, body, conn, addr)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func (d *DefaultRemotingClient) invokeOneway(addr string, request *RemotingCommand, timeoutMillis int64) (err error) {
	d.connTableLock.RLock()
	conn, ok := d.connTable[addr]
	d.connTableLock.RUnlock()

	if !ok {
		conn, err = d.connect(addr)
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	request.markOnewayRPC()
	header := request.encodeHeader()
	body := request.Body

	return d.sendRequest(header, body, conn, addr)
}

func (d *DefaultRemotingClient) handlerConn(conn net.Conn, addr string) {
	b := make([]byte, 1024)
	var length, headerLength, bodyLength int32
	var buf = bytes.NewBuffer([]byte{})
	var header, body []byte
	var flag = 0
	for {
		n, err := conn.Read(b)
		if err != nil {
			d.releaseConn(addr, conn)
			fmt.Println(err, addr)

			return
		}

		_, err = buf.Write(b[:n])
		if err != nil {
			d.releaseConn(addr, conn)
			return
		}

		for {
			if flag == 0 {
				if buf.Len() >= 4 {
					err = binary.Read(buf, binary.BigEndian, &length)
					if err != nil {
						fmt.Println(err)
						return
					}
					flag = 1
				} else {
					break
				}
			}

			if flag == 1 {
				if buf.Len() >= 4 {
					err = binary.Read(buf, binary.BigEndian, &headerLength)
					if err != nil {
						fmt.Println(err)
						return
					}
					flag = 2
				} else {
					break
				}

			}

			if flag == 2 {
				if (buf.Len() > 0) && (buf.Len() >= int(headerLength)) {
					header = make([]byte, headerLength)
					_, err = buf.Read(header)
					if err != nil {
						fmt.Println(err)
						return
					}
					flag = 3
				} else {
					break
				}
			}

			if flag == 3 {
				bodyLength = length - 4 - headerLength
				if bodyLength == 0 {
					flag = 0
				} else {

					if buf.Len() >= int(bodyLength) {
						body = make([]byte, int(bodyLength))
						_, err = buf.Read(body)
						if err != nil {
							fmt.Println(err)
							return
						}
						flag = 0
					} else {
						break
					}
				}
			}

			if flag == 0 {
				headerCopy := make([]byte, len(header))
				bodyCopy := make([]byte, len(body))
				copy(headerCopy, header)
				copy(bodyCopy, body)
				go func() {
					cmd := decodeRemoteCommand(headerCopy, bodyCopy)
					d.responseTableLock.RLock()
					response, ok := d.responseTable[cmd.Opaque]
					d.responseTableLock.RUnlock()

					d.responseTableLock.Lock()
					delete(d.responseTable, cmd.Opaque)
					d.responseTableLock.Unlock()

					if ok {
						response.responseCommand = cmd
						if response.invokeCallback != nil {
							response.invokeCallback(response)
						}

						if response.done != nil {
							response.done <- true
						}
					} else {
						if cmd.Code == NotifyConsumerIdsChanged {
							return
						}
						jsonCmd, err := json.Marshal(cmd)

						if err != nil {
							fmt.Println(err)
						}
						fmt.Println(string(jsonCmd))
					}
				}()
			}
		}

	}
}

func (d *DefaultRemotingClient) sendRequest(header, body []byte, conn net.Conn, addr string) error {

	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, int32(len(header)+len(body)+4))
	binary.Write(buf, binary.BigEndian, int32(len(header)))
	_, err := conn.Write(buf.Bytes())

	if err != nil {
		d.releaseConn(addr, conn)
		return err
	}

	_, err = conn.Write(header)
	if err != nil {
		d.releaseConn(addr, conn)
		return err
	}

	if body != nil && len(body) > 0 {
		_, err = conn.Write(body)
		if err != nil {
			d.releaseConn(addr, conn)
			return err
		}
	}

	return nil
}

func (d *DefaultRemotingClient) releaseConn(addr string, conn net.Conn) {
	conn.Close()
	d.connTableLock.Lock()
	delete(d.connTable, addr)
	d.connTableLock.Unlock()
}

func (d *DefaultRemotingClient) getNameServerAddressList() []string {
	return d.namesrvAddrList
}

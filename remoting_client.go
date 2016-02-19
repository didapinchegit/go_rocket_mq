package rocketmq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"log"
	"net"
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
}

type DefalutRemotingClient struct {
	mutex              sync.Mutex
	connTables         map[string]net.Conn
	responseTable      map[int32]*ResponseFuture
	namesrvAddrList    []string
	namesrvAddrChoosed string
}

func NewDefaultRemotingClient() RemotingClient {
	return &DefalutRemotingClient{
		connTables:    make(map[string]net.Conn),
		responseTable: make(map[int32]*ResponseFuture),
	}
}

func (self *DefalutRemotingClient) connect(addr string) (conn net.Conn, err error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if addr == "" {
		addr = self.namesrvAddrChoosed
	}

	conn, ok := self.connTables[addr]
	if !ok {
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			log.Print(err)
			return nil, err
		}

		self.connTables[addr] = conn
		log.Print("connect to:", addr)
		go self.handlerConn(conn, addr)
	}

	return conn, nil
}

func (self *DefalutRemotingClient) invokeSync(addr string, request *RemotingCommand, timeoutMillis int64) (*RemotingCommand, error) {

	conn, err := self.connect(addr)

	response := &ResponseFuture{
		sendRequestOK:  false,
		opaque:         request.Opaque,
		timeoutMillis:  timeoutMillis,
		beginTimestamp: time.Now().Unix(),
		done:           make(chan bool),
	}

	header := request.encodeHeader()
	body := request.Body

	self.mutex.Lock()
	self.responseTable[request.Opaque] = response
	self.mutex.Unlock()
	err = self.sendRequest(header, body, conn, addr)
	if err != nil {
		log.Print(err)
		return nil, err
	}
	select {
	case <-response.done:
		return response.responseCommand, nil
	case <-time.After(3 * time.Second):
		return nil, errors.New("invoke timeout")
	}

}

func (self *DefalutRemotingClient) invokeAsync(addr string, request *RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error {
	conn, ok := self.connTables[addr]
	var err error
	if !ok {
		conn, err = self.connect(addr)
		if err != nil {
			log.Print(err)
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

	self.mutex.Lock()
	self.responseTable[request.Opaque] = response
	self.mutex.Unlock()

	header := request.encodeHeader()
	body := request.Body
	err = self.sendRequest(header, body, conn, addr)
	if err != nil {
		log.Print(err)
		return err
	}
	return nil
}

func (self *DefalutRemotingClient) handlerConn(conn net.Conn, addr string) {
	b := make([]byte, 1024)
	var length, headerLength, bodyLength int32
	var buf = bytes.NewBuffer([]byte{})
	var header, body []byte
	var flag int = 0
	for {
		n, err := conn.Read(b)
		if err != nil {
			self.mutex.Lock()
			delete(self.connTables, addr)
			self.mutex.Unlock()
			log.Print(err, addr)
			conn.Close()
			return
		}

		_, err = buf.Write(b[:n])
		if err != nil {
			self.mutex.Lock()
			delete(self.connTables, addr)
			self.mutex.Unlock()
			log.Print(err, addr)
			conn.Close()
			return
		}

		for {
			if flag == 0 {
				if buf.Len() >= 4 {
					err = binary.Read(buf, binary.BigEndian, &length)
					if err != nil {
						log.Print(err)
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
						log.Print(err)
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
						log.Print(err)
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
							log.Print(err)
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
					response, ok := self.responseTable[cmd.Opaque]
					self.mutex.Lock()
					delete(self.responseTable, cmd.Opaque)
					self.mutex.Unlock()

					if ok {
						response.responseCommand = cmd
						if response.invokeCallback != nil {
							response.invokeCallback(response)
						}

						if response.done != nil {
							response.done <- true
						}
					} else {
						if cmd.Code == NOTIFY_CONSUMER_IDS_CHANGED {
							return
						}
						jsonCmd, err := json.Marshal(cmd)

						if err != nil {
							log.Print(err)
						}
						log.Print(string(jsonCmd))
					}
				}()
			}
		}

	}
}

func (self *DefalutRemotingClient) sendRequest(header, body []byte, conn net.Conn, addr string) error {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, int32(len(header)+len(body)+4))
	binary.Write(buf, binary.BigEndian, int32(len(header)))
	_, err := conn.Write(buf.Bytes())

	if err != nil {
		conn.Close()
		self.mutex.Lock()
		delete(self.connTables, addr)
		self.mutex.Unlock()
		self.connect(addr)
		return err
	}

	_, err = conn.Write(header)
	if err != nil {
		conn.Close()
		self.mutex.Lock()
		delete(self.connTables, addr)
		self.mutex.Unlock()
		self.connect(addr)

		return err
	}

	if body != nil && len(body) > 0 {
		_, err = conn.Write(body)
		if err != nil {
			return err
		}
	}

	return nil
}

package rocketmq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"sync"
)

const (
	RPC_TYPE      int = 0
	RPC_ONEWAYint     = 1
)

var opaque int32
var decodeLock sync.Mutex

var (
	remotingVersionKey string = "rocketmq.remoting.version"
	ConfigVersion      int    = -1
	requestId          int32  = 0
)

type RemotingCommand struct {
	//header
	Code      int         `json:"code"`
	Language  string      `json:"language"`
	Version   int         `json:"version"`
	Opaque    int32       `json:"opaque"`
	Flag      int         `json:"flag"`
	remark    string      `json:"remark"`
	ExtFields interface{} `json:"extFields"`
	//body
	Body []byte `json:"body,omitempty"`
}

func (self *RemotingCommand) encodeHeader() []byte {
	length := 4
	headerData := self.buildHeader()
	length += len(headerData)

	if self.Body != nil {
		length += len(self.Body)
	}

	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, length)
	binary.Write(buf, binary.BigEndian, len(self.Body))
	buf.Write(headerData)

	return buf.Bytes()
}

func (self *RemotingCommand) buildHeader() []byte {
	buf, err := json.Marshal(self)
	if err != nil {
		return nil
	}
	return buf
}

func (self *RemotingCommand) encode() []byte {
	length := 4

	headerData := self.buildHeader()
	length += len(headerData)

	if self.Body != nil {
		length += len(self.Body)
	}

	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, length)
	binary.Write(buf, binary.LittleEndian, len(self.Body))
	buf.Write(headerData)

	if self.Body != nil {
		buf.Write(self.Body)
	}

	return buf.Bytes()
}

func decodeRemoteCommand(header, body []byte) *RemotingCommand {
	decodeLock.Lock()
	defer decodeLock.Unlock()

	cmd := &RemotingCommand{}
	cmd.ExtFields = make(map[string]string)
	err := json.Unmarshal(header, cmd)
	if err != nil {
		log.Print(err)
		return nil
	}
	cmd.Body = body
	return cmd
}

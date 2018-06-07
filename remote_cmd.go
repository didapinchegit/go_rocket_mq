package rocketmq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"strconv"
	"sync"
)

const (
	RpcType   = 0
	RpcOneway = 1
)

var opaque int32
var decodeLock sync.Mutex

var (
	remotingVersionKey string = "rocketmq.remoting.version"
	ConfigVersion      int    = -1
	requestId          int32  = 0
)

type RemotingCommand struct {
	// header
	Code      int         `json:"code"`
	Language  string      `json:"language"`
	Version   int         `json:"version"`
	Opaque    int32       `json:"opaque"`
	Flag      int         `json:"flag"`
	remark    string      `json:"remark"`
	ExtFields interface{} `json:"extFields"`
	// body
	Body []byte `json:"body,omitempty"`
}

func (r *RemotingCommand) encodeHeader() []byte {
	length := 4
	headerData := r.buildHeader()
	length += len(headerData)

	if r.Body != nil {
		length += len(r.Body)
	}

	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, length)
	binary.Write(buf, binary.BigEndian, len(r.Body))
	buf.Write(headerData)

	return buf.Bytes()
}

func (r *RemotingCommand) buildHeader() []byte {
	buf, err := json.Marshal(r)
	if err != nil {
		return nil
	}
	return buf
}

func (r *RemotingCommand) encode() []byte {
	length := 4

	headerData := r.buildHeader()
	length += len(headerData)

	if r.Body != nil {
		length += len(r.Body)
	}

	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, length)
	binary.Write(buf, binary.LittleEndian, len(r.Body))
	buf.Write(headerData)

	if r.Body != nil {
		buf.Write(r.Body)
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

func (r *RemotingCommand) decodeCommandCustomHeader() (responseHeader SendMessageResponseHeader) {
	msgId := r.ExtFields.(map[string]interface{})["msgId"].(string)
	queueId, _ := strconv.Atoi(r.ExtFields.(map[string]interface{})["queueId"].(string))
	queueOffset, _ := strconv.Atoi(r.ExtFields.(map[string]interface{})["queueOffset"].(string))
	responseHeader = SendMessageResponseHeader{
		msgId:         msgId,
		queueId:       int32(queueId),
		queueOffset:   int64(queueOffset),
		transactionId: "",
	}
	return
}

func (r *RemotingCommand) markOnewayRPC() {
	bits := 1 << RpcOneway
	r.Flag |= bits
}

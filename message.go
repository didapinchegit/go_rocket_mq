package rocketmq

import (
	"bytes"
	"encoding/binary"
	"github.com/golang/glog"
	"encoding/json"
	"io/ioutil"
	"compress/zlib"
)
const(
	CompressedFlag = (0x1 << 0)
)
type Message struct {
	Topic      string
	Flag       int32
	Properties map[string]string
	Body       []byte
}

type MessageExt struct {
	Message
	QueueId       int32
	StoreSize     int32
	QueueOffset   int64
	SysFlag       int32
	BornTimestamp int64
	//bornHost
	StoreTimestamp int64
	//storeHost
	MsgId                     string
	CommitLogOffset           int64
	BodyCRC                   int32
	ReconsumeTimes            int32
	PreparedTransactionOffset int64
}

func decodeMessage(data []byte) []*MessageExt {
	buf := bytes.NewBuffer(data)
	var storeSize, magicCode, bodyCRC, queueId, flag, sysFlag, reconsumeTimes, bodyLength, bornPort, storePort int32
	var queueOffset, physicOffset, preparedTransactionOffset, bornTimeStamp, storeTimestamp int64
	var topicLen byte
	var topic, body, properties, bornHost, storeHost []byte
	var propertiesLength int16

	var propertiesmap map[string]string

	msgs := make([]*MessageExt, 0, 32)
	for buf.Len() > 0 {
		msg := new(MessageExt)
		binary.Read(buf, binary.BigEndian, &storeSize)
		binary.Read(buf, binary.BigEndian, &magicCode)
		binary.Read(buf, binary.BigEndian, &bodyCRC)
		binary.Read(buf, binary.BigEndian, &queueId)
		binary.Read(buf, binary.BigEndian, &flag)
		binary.Read(buf, binary.BigEndian, &queueOffset)
		binary.Read(buf, binary.BigEndian, &physicOffset)
		binary.Read(buf, binary.BigEndian, &sysFlag)
		binary.Read(buf, binary.BigEndian, &bornTimeStamp)
		bornHost = make([]byte, 4)
		binary.Read(buf, binary.BigEndian, &bornHost)
		binary.Read(buf, binary.BigEndian, &bornPort)
		binary.Read(buf, binary.BigEndian, &storeTimestamp)
		storeHost = make([]byte, 4)
		binary.Read(buf, binary.BigEndian, &storeHost)
		binary.Read(buf, binary.BigEndian, &storePort)
		binary.Read(buf, binary.BigEndian, &reconsumeTimes)
		binary.Read(buf, binary.BigEndian, &preparedTransactionOffset)
		binary.Read(buf, binary.BigEndian, &bodyLength)
		if bodyLength > 0 {
			body = make([]byte, bodyLength)
			binary.Read(buf, binary.BigEndian, body)

			if (sysFlag &  CompressedFlag) == CompressedFlag {
				b := bytes.NewReader(body)
				z, err := zlib.NewReader(b)
				if err != nil {
					glog.Error(err)
					return nil
				}
				defer z.Close()
				body, err = ioutil.ReadAll(z)
				if err != nil {
					glog.Error(err)
					return nil
				}
			}

		}
		binary.Read(buf, binary.BigEndian, &topicLen)
		topic = make([]byte, topicLen)
		binary.Read(buf, binary.BigEndian, &topic)
		binary.Read(buf, binary.BigEndian, &propertiesLength)
		if propertiesLength >0 {
			properties = make([]byte, propertiesLength)
			binary.Read(buf, binary.BigEndian, &properties)
			propertiesmap = make(map[string]string)
			json.Unmarshal(properties,&propertiesmap)
		}

		if magicCode != -626843481 {
			glog.Infof("magic code is error %d",magicCode)
			return nil
		}



		msg.Topic = string(topic)
		msg.QueueId = queueId
		msg.SysFlag = sysFlag
		msg.QueueOffset = queueOffset
		msg.BodyCRC = bodyCRC
		msg.StoreSize = storeSize
		msg.BornTimestamp = bornTimeStamp
		msg.ReconsumeTimes = reconsumeTimes
		msg.Flag = flag
		//msg.commitLogOffset=physicOffset
		msg.StoreTimestamp = storeTimestamp
		msg.PreparedTransactionOffset = preparedTransactionOffset
		msg.Body = body
		msg.Properties = propertiesmap

		msgs = append(msgs, msg)
	}

	return msgs
}

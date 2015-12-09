package rocketmq

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
)

const (
	MEMORY_FIRST_THEN_STORE = 0
	READ_FROM_MEMORY        = 1
	READ_FROM_STORE         = 2
)

type OffsetStore interface {
	//load() error
	updateOffset(mq *MessageQueue, offset int64, increaseOnly bool)
	readOffset(mq *MessageQueue, flag int) int64
	//persistAll(mqs []MessageQueue)
	//persist(mq MessageQueue)
	//removeOffset(mq MessageQueue)
	//cloneOffsetTable(topic string) map[MessageQueue]int64
}
type RemoteOffsetStore struct {
	groupName   string
	mqClient    *MqClient
	offsetTable map[MessageQueue]int64
	mutex       sync.Mutex
}

func (self *RemoteOffsetStore) readOffset(mq *MessageQueue, readType int) int64 {

	switch readType {
	case MEMORY_FIRST_THEN_STORE:
	case READ_FROM_MEMORY:
		offset, ok := self.offsetTable[*mq]
		if ok {
			return offset
		} else if readType == READ_FROM_MEMORY {
			return -1
		}
	case READ_FROM_STORE:
		offset, err := self.fetchConsumeOffsetFromBroker(mq)

		if err != nil {
			log.Print(err)
			return -1
		}
		self.updateOffset(mq, offset, false)
		return offset
	}

	return -1

}

func (self *RemoteOffsetStore) fetchConsumeOffsetFromBroker(mq *MessageQueue) (int64, error) {
	brokerAddr, _, found := self.mqClient.findBrokerAddressInSubscribe(mq.brokerName, 0, false)

	if !found {
		self.mqClient.updateTopicRouteInfoFromNameServerByTopic(mq.topic)
		brokerAddr, _, found = self.mqClient.findBrokerAddressInSubscribe(mq.brokerName, 0, false)
	}

	if found {
		requestHeader := &QueryConsumerOffsetRequestHeader{}
		requestHeader.Topic = mq.topic
		requestHeader.QueueId = mq.queueId
		requestHeader.ConsumerGroup = self.groupName
		return self.mqClient.queryConsumerOffset(brokerAddr, requestHeader, 3000)
	}

	return 0, errors.New("fetch consumer offset error")
}

func (self *RemoteOffsetStore) persist(mq *MessageQueue) {
	offset, ok := self.offsetTable[*mq]
	if ok {
		err := self.updateConsumeOffsetToBroker(mq, offset)
		if err != nil {
			log.Print(err)
		}
	}
}

type UpdateConsumerOffsetRequestHeader struct {
	consumerGroup string
	topic         string
	queueId       int32
	commitOffset  int64
}

func (self *RemoteOffsetStore) updateConsumeOffsetToBroker(mq *MessageQueue, offset int64) error {
	addr, found, _ := self.mqClient.findBrokerAddressInAdmin(mq.brokerName)
	if !found {
		self.mqClient.updateTopicRouteInfoFromNameServerByTopic(mq.topic)
		addr, found, _ = self.mqClient.findBrokerAddressInAdmin(mq.brokerName)
	}

	if found {
		requestHeader := &UpdateConsumerOffsetRequestHeader{
			consumerGroup: self.groupName,
			topic:         mq.topic,
			queueId:       mq.queueId,
			commitOffset:  offset,
		}

		self.mqClient.updateConsumerOffsetOneway(addr, requestHeader, 5*1000)
		return nil
	}
	return errors.New("not found broker")
}

func (self *RemoteOffsetStore) updateOffset(mq *MessageQueue, offset int64, increaseOnly bool) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if mq != nil {
		offsetOld, ok := self.offsetTable[*mq]
		if !ok {
			self.offsetTable[*mq] = offset
		} else {
			if increaseOnly {
				atomic.AddInt64(&offsetOld, offset)
				self.offsetTable[*mq] = offsetOld
			} else {
				self.offsetTable[*mq] = offset

			}
		}

	}

}

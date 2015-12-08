package rocketmq

import (
	"errors"
	"log"
)

type OffsetStore interface {
	//load() error
	//updateOffset(mq MessageQueue, offset int64, increaseOnly bool)
	readOffset(mq *MessageQueue, flag string) int64
	//persistAll(mqs []MessageQueue)
	//persist(mq MessageQueue)
	//removeOffset(mq MessageQueue)
	//cloneOffsetTable(topic string) map[MessageQueue]int64
}
type RemoteOffsetStore struct {
	groupName string
	mqClient  *MqClient
}

func (self *RemoteOffsetStore) readOffset(mq *MessageQueue, readType string) int64 {
	if readType == "READ_FROM_STORE" {
		offset, err := self.fetchConsumeOffsetFromBroker(mq)
		if err != nil {
			log.Print(err)
			return -1
		}
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

func (self *RemoteOffsetStore) persist(mqs []MessageQueue) {
}

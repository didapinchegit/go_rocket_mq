package rocketmq

import (
	"math/rand"
)

type TopicPublishInfo struct {
	orderTopic          bool
	haveTopicRouterInfo bool
	messageQueueList    []*MessageQueue
	topicRouteData      *TopicRouteData
}

func NewTopicPublishInfo() *TopicPublishInfo {
	return &TopicPublishInfo{}
}

func (t *TopicPublishInfo) ok() (ok bool) {
	if len(t.messageQueueList) != 0 {
		ok = true
	}
	return
}

func (t *TopicPublishInfo) selectOneMessageQueue(lastBrokerName string) (messageQueue *MessageQueue) {
	// TODO add sendLatencyFaultEnable trigger and handle it
	// TODO optimize algorithm of getting message from queue
	mqCnt := len(t.messageQueueList)
	messageQueue = t.messageQueueList[rand.Intn(mqCnt)]
	if lastBrokerName != "" {
		for i := 0; i < mqCnt; i++ {
			if lastBrokerName == t.messageQueueList[i].brokerName {
				messageQueue = t.messageQueueList[i]
				return
			}
		}
	}
	return
}

package rocketmq

type MessageQueue struct {
	topic      string
	brokerName string
	queueId    int32
}

func (self *MessageQueue) clone() *MessageQueue {
	no := new(MessageQueue)
	no.topic = self.topic
	no.queueId = self.queueId
	no.brokerName = self.brokerName
	return no
}

type MessageQueues []*MessageQueue

func (self MessageQueues) Less(i, j int) bool {
	imq := self[i]
	jmq := self[j]

	if imq.topic < jmq.topic {
		return true
	} else if imq.topic < jmq.topic {
		return false
	}

	if imq.brokerName < jmq.brokerName {
		return true
	} else if imq.brokerName < jmq.brokerName {
		return false
	}

	if imq.queueId < jmq.queueId {
		return true
	} else {
		return false
	}
}

func (self MessageQueues) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self MessageQueues) Len() int {
	return len(self)
}

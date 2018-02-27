package rocketmq

type MessageQueue struct {
	topic      string
	brokerName string
	queueId    int32
}

func NewMessageQueue(topic string, brokerName string, queueId int32) *MessageQueue {
	return &MessageQueue{
		topic:      topic,
		brokerName: brokerName,
		queueId:    queueId,
	}
}

func (m *MessageQueue) clone() *MessageQueue {
	no := new(MessageQueue)
	no.topic = m.topic
	no.queueId = m.queueId
	no.brokerName = m.brokerName
	return no
}

func (m MessageQueue) getBrokerName() string {
	return m.brokerName
}

type MessageQueues []*MessageQueue

func (m MessageQueues) Less(i, j int) bool {
	imq := m[i]
	jmq := m[j]

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
	}
	return false
}

func (m MessageQueues) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (m MessageQueues) Len() int {
	return len(m)
}

package rocketmq

type Admin interface {
	createTopic(key string, newTopic string, queueNum int)
	createTopic1(key string, newTopic string, queueNum int, topicSysFlag int)
	searchOffset(mq MessageQueue, timestamp int64) error
	maxOffset(mq MessageQueue) error
	minOffset(mq MessageQueue) error
	earliestMsgStoreTime(mq MessageQueue) error
	queryMessage(topic string, key, string, maxNum int, begin int64, end int64) error
}

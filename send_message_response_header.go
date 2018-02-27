package rocketmq

type SendMessageResponseHeader struct {
	msgId         string
	queueId       int32
	queueOffset   int64
	transactionId string
}

package rocketmq

const (
	SendStatusOK = iota
	SendStatusFlushDiskTimeout
	SendStatusFlushSlaveTimeout
	SendStatusSlaveNotAvailable
)

type SendResult struct {
	sendStatus    int
	msgId         string
	messageQueue  *MessageQueue
	queueOffset   int64
	transactionId string
	offsetMsgId   string
	regionId      string
}

func NewSendResult(sendStatus int, msgId string, offsetMsgId string, messageQueue *MessageQueue, queueOffset int64) *SendResult {
	return &SendResult{
		sendStatus:   sendStatus,
		msgId:        msgId,
		offsetMsgId:  offsetMsgId,
		messageQueue: messageQueue,
		queueOffset:  queueOffset,
	}
}

func (s *SendResult) SendResult(SendStatus int, msgId string, messageQueue MessageQueue, queueOffset uint64,
	transactionId string, offsetMsgId string, regionId string) (ok bool) {
	return
}

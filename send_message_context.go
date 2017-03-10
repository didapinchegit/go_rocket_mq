package rocketmq

const (
	NormalMsg = iota
	TransMsgHalf
	TransMsgCommit
	DelayMsg
)

type SendMessageContext struct {
	producerGroup     string
	Message           Message
	mq                MessageQueue
	brokerAddr        string
	bornHost          string
	communicationMode string
	sendResult        *SendResult
	props             map[string]string
	producer          Producer
	msgType           string
}

func newSendMessageContext() *SendMessageContext {
	return &SendMessageContext{}
}

// TODO add decodeMessage
func (s *SendMessageContext) decodeMessage(data []byte) (messageExt []*MessageExt) {
	return messageExt
}

package rocketmq

type SendRequest struct {
	producerGroup string
	messageQueue  *MessageQueue
	nextOffset    int64
}

type SendMessageRequestHeader struct {
	ProducerGroup         string `json:"producerGroup"`
	Topic                 string `json:"topic"`
	DefaultTopic          string `json:"defaultTopic"`
	DefaultTopicQueueNums int    `json:"defaultTopicQueueNums"`
	QueueId               int32  `json:"queueId"`
	SysFlag               int    `json:"sysFlag"`
	BornTimestamp         int64  `json:"bornTimestamp"`
	Flag                  int32  `json:"flag"`
	Properties            string `json:"properties"`
	ReconsumeTimes        int    `json:"reconsumeTimes"`
	UnitMode              bool   `json:"unitMode"`
	MaxReconsumeTimes     int    `json:"maxReconsumeTimes"`
}

type SendMessageService struct {
	pushRequestQueue chan *SendRequest
	producer         *DefaultProducer
}

func NewSendMessageService() *SendMessageService {
	return &SendMessageService{
		pushRequestQueue: make(chan *SendRequest, 1024),
	}
}

func (s *SendMessageService) start() {
	//for {
	//	pushRequest := <-self.pushRequestQueue
	//	self.producer.sendMessage(pushRequest)
	//}
}

package rocketmq

import (
	"testing"
	"time"
)

// dev-goProducerConsumerTest
var consumerGroup = "dev-goProducerConsumerTest"

// goProducerConsumerTest
var consumerTopic = "goProducerConsumerTest"
var timeSleep = 60 * time.Second
var consumerConf = &Config{
	//Nameserver:   "192.168.7.103:9876",
	Namesrv:      "192.168.6.69:9876",
	ClientIp:     "192.168.23.137",
	InstanceName: "DEFAULT_tt",
}

func TestConsume(t *testing.T) {
	consumer, err := NewDefaultConsumer(consumerGroup, consumerConf)
	if err != nil {
		t.Fatalf("NewDefaultConsumer err, %s", err)
	}
	consumer.Subscribe(consumerTopic, "*")
	consumer.RegisterMessageListener(
		func(msgs []*MessageExt) error {
			for i, msg := range msgs {
				t.Log("msg", i, msg.Topic, msg.Flag, msg.Properties, string(msg.Body))
			}
			t.Log("Consume success!")
			return nil
		})
	consumer.Start()

	time.Sleep(timeSleep)
}

package rocketmq

import (
	"testing"
	"time"
)

var consumerGroup = "dev-goProducerConsumerTest"
var consumerTopic = "goProducerConsumerTest"
var sleep = 60 * time.Second
var consumerConf = &Config{
	Namesrv:      "192.168.7.101:9876;192.168.7.102:9876;192.168.7.103:9876",
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

	time.Sleep(sleep)
}

package rocketmq

import (
	"strconv"
	"testing"
)

var producerGroup = "dev-goProducerConsumerTest"
var topic = "goProducerConsumerTest"
var conf = &Config{
	Namesrv:      "192.168.7.101:9876;192.168.7.102:9876;192.168.7.103:9876",
	ClientIp:     "192.168.23.137",
	InstanceName: "DEFAULT_tt",
}

func TestSend(t *testing.T) {
	producer, err := NewDefaultProducer(producerGroup, conf)
	producer.Start()
	if err != nil {
		t.Fatalf("NewDefaultProducer err, %s", err)
	}
	for i := 0; i < 3; i++ {
		msg := NewMessage(topic, []byte("Hello RocketMQ "+strconv.Itoa(i)))
		if sendResult, err := producer.Send(msg); err != nil {
			t.Fatalf("Sync sending fail!, %s", err.Error())
		} else {
			t.Log("sendResult", sendResult)
			t.Logf("Sync sending success, %d", i)
			//t.Logf("sendResult.sendStatus", sendResult.sendStatus)
			//t.Logf("sendResult.msgId", sendResult.msgId)
			//t.Logf("sendResult.messageQueue", sendResult.messageQueue)
			//t.Logf("sendResult.queueOffset", sendResult.queueOffset)
			//t.Logf("sendResult.transactionId", sendResult.transactionId)
			//t.Logf("sendResult.offsetMsgId", sendResult.offsetMsgId)
			//t.Logf("sendResult.regionId", sendResult.regionId)
		}
	}

	t.Log("Sync sending success!")
}

func TestSendOneway(t *testing.T) {
	producer, err := NewDefaultProducer(producerGroup, conf)
	producer.Start()
	if err != nil {
		t.Fatalf("NewDefaultProducer err, %s", err)
	}
	for i := 0; i < 3; i++ {
		msg := NewMessage(topic, []byte("Hello RocketMQ "+strconv.Itoa(i)))
		if err := producer.SendOneway(msg); err != nil {
			t.Fatalf("Oneway sending fail! %s", err.Error())
		} else {
			t.Logf("Oneway sending success, %d", i)
		}
	}

	t.Log("Oneway sending success!")
}

func TestSendAsync(t *testing.T) {
	producer, err := NewDefaultProducer(producerGroup, conf)
	producer.Start()
	if err != nil {
		t.Fatalf("NewDefaultProducer err, %s", err)
	}
	for i := 0; i < 3; i++ {
		msg := NewMessage(topic, []byte("Hello RocketMQ "+strconv.Itoa(i)))
		sendCallback := func() error {
			t.Logf("I am callback")
			return nil
		}
		if err := producer.SendAsync(msg, sendCallback); err != nil {
			t.Fatalf("Async sending fail! %s", err.Error())
		} else {
			t.Logf("Async sending success, %d", i)
		}
	}

	t.Log("Async sending success!")
}

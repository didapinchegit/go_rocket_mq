package rocketmq

import (
	"strconv"
	"testing"
)

// dev-goProducerConsumerTest
var group = "dev-goProducerConsumerTest"

// goProducerConsumerTest
var topic = "goProducerConsumerTest"
var conf = &Config{
	//Nameserver:   "192.168.7.101:9876;192.168.7.102:9876;192.168.7.103:9876",
	Namesrv:      "192.168.6.69:9876",
	ClientIp:     "192.168.23.137",
	InstanceName: "DEFAULT_tt",
}

func TestSend(t *testing.T) {
	producer, err := NewDefaultProducer(group, conf)
	producer.Start()
	if err != nil {
		t.Fatalf("NewDefaultProducer err, %s", err)
	}
	for i := 0; i < 3; i++ {
		msg := NewMessage(topic, []byte("Hello RocketMQ "+strconv.Itoa(i)))
		if sendResult, err := producer.Send(msg); err != nil {
			t.Error("Sync send fail!") // 如果不是如预期的那么就报错
			t.Fatalf("err->%s", err)
		} else {
			t.Logf("sendResult", sendResult)
			t.Logf("Sync send success, %d", i)
			//t.Logf("sendResult.sendStatus", sendResult.sendStatus)
			//t.Logf("sendResult.msgId", sendResult.msgId)
			//t.Logf("sendResult.messageQueue", sendResult.messageQueue)
			//t.Logf("sendResult.queueOffset", sendResult.queueOffset)
			//t.Logf("sendResult.transactionId", sendResult.transactionId)
			//t.Logf("sendResult.offsetMsgId", sendResult.offsetMsgId)
			//t.Logf("sendResult.regionId", sendResult.regionId)
		}
	}

	t.Log("Sync send success!")
}

func TestSendOneway(t *testing.T) {
	producer, err := NewDefaultProducer(group, conf)
	producer.Start()
	if err != nil {
		t.Fatalf("NewDefaultProducer err, %s", err)
	}
	for i := 0; i < 3; i++ {
		msg := NewMessage(topic, []byte("Hello RocketMQ "+strconv.Itoa(i)))
		if err := producer.SendOneway(msg); err != nil {
			t.Error("Oneway send fail!") // 如果不是如预期的那么就报错
			t.Fatalf("err->%s", err)
		} else {
			t.Logf("Oneway send success, %d", i)
		}
	}

	t.Log("Oneway send success!")
}

func TestSendAsync(t *testing.T) {
	producer, err := NewDefaultProducer(group, conf)
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
			t.Error("Async send fail!") // 如果不是如预期的那么就报错
			t.Fatalf("err->%s", err)
		} else {
			t.Logf("Async send success, %d", i)
		}
	}

	t.Log("Async send success!")
}

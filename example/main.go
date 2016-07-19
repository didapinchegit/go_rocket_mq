package main

import (
	rocketmq "didapinche.com/go_rocket_mq"
	"github.com/golang/glog"
	"time"
	"flag"
)

func main() {
	flag.Parse()
	conf := &rocketmq.Config{
		Nameserver:   "192.168.1.234:9876",
		InstanceName: "DEFAULT",
	}
	consumer, err := rocketmq.NewDefaultConsumer("C_TEST", conf)
	if err != nil {
		panic(err)
	}
	consumer.Subscribe("t_city", "*")
	consumer.RegisterMessageListener(func(msgs []*rocketmq.MessageExt) error {
		for i, msg := range msgs {
			glog.Info(i, string(msg.Body))
		}
		return nil
	})
	consumer.Start()
	time.Sleep(1000 * time.Second)
}

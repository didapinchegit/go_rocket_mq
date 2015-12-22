package main

import (
	rocketmq "didapinche.com/go_rocket_mq"
	"log"
	"runtime"
	"time"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	conf := &rocketmq.Config{
		Nameserver:   "192.168.1.234:9876",
		ClientIp:     "192.168.1.23",
		InstanceName: "DEFAULT",
	}
	consumer, err := rocketmq.NewDefaultConsumer("C_TEST", conf)
	if err != nil {
		log.Panic(err)
	}
	consumer.Subscribe("test2", "*")
	consumer.Subscribe("test3", "*")
	consumer.RegisterMessageListener(func(msgs []*rocketmq.MessageExt) error {
		for i, msg := range msgs {
			log.Print(i, string(msg.Body))
		}
		return nil
	})
	consumer.Start()
	time.Sleep(1000 * time.Second)
}

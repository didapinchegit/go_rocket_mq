# Introduction
A RocketMQ client for golang.

# Import package
import "github.com/sevenNt/rocketmq"

# Getting started
### Getting message with consumer
```
group := "dev-VodHotClacSrcData"
topic := "canal_vod_collect__video_collected_count_live"
var timeSleep = 30 * time.Second
conf := &rocketmq.Config{
    Nameserver:   "192.168.7.101:9876;192.168.7.102:9876;192.168.7.103:9876",
    ClientIp:     "192.168.1.23",
    InstanceName: "DEFAULT",
}

consumer, err := rocketmq.NewDefaultConsumer(consumerGroup, consumerConf)
if err != nil {
    return err
}
consumer.Subscribe(consumerTopic, "*")
consumer.RegisterMessageListener(
    func(msgs []*MessageExt) error {
        for i, msg := range msgs {
            fmt.Println("msg", i, msg.Topic, msg.Flag, msg.Properties, string(msg.Body))
        }
        fmt.Println("Consume success!")
        return nil
    })
consumer.Start()

time.Sleep(timeSleep)
```

### Sending message with producer
- Synchronous sending
```
group := "dev-VodHotClacSrcData"
topic := "canal_vod_collect__video_collected_count_live"
conf := &rocketmq.Config{
    Nameserver:   "192.168.7.101:9876;192.168.7.102:9876;192.168.7.103:9876",
    ClientIp:     "192.168.1.23",
    InstanceName: "DEFAULT",
}

producer, err := rocketmq.NewDefaultProducer(group, conf)
producer.Start()
if err != nil {
    return errors.New("NewDefaultProducer err")
}
msg := NewMessage(topic, []byte("Hello RocketMQ!")
if sendResult, err := producer.Send(msg); err != nil {
    return errors.New("Sync send fail!") // 如果不是如预期的那么就报错
} else {
    fmt.Printlnf("sendResult", sendResult)
    fmt.Printlnf("Sync send success!")
}
```

- Asynchronous sending
```
group := "dev-VodHotClacSrcData"
topic := "canal_vod_collect__video_collected_count_live"
conf := &rocketmq.Config{
    Nameserver:   "192.168.7.101:9876;192.168.7.102:9876;192.168.7.103:9876",
    ClientIp:     "192.168.1.23",
    InstanceName: "DEFAULT",
}
producer, err := rocketmq.NewDefaultProducer(group, conf)
producer.Start()
if err != nil {
    return err
}
msg := NewMessage(topic, []byte("Hello RocketMQ!")
sendCallback := func() error {
    fmt.Printlnf("I am callback")
    return nil
}
if err := producer.SendAsync(msg, sendCallback); err != nil {
    return err
} else {
    fmt.Printlnf("Async send success!")
}
```

- Oneway sending
```
group := "dev-VodHotClacSrcData"
topic := "canal_vod_collect__video_collected_count_live"
conf := &rocketmq.Config{
    Nameserver:   "192.168.7.101:9876;192.168.7.102:9876;192.168.7.103:9876",
    ClientIp:     "192.168.1.23",
    InstanceName: "DEFAULT",
}
producer, err := rocketmq.NewDefaultProducer(group, conf)
producer.Start()
if err != nil {
    return err
}
msg := NewMessage(topic, []byte("Hello RocketMQ!")
if err := producer.SendOneway(msg); err != nil {
    return err
} else {
    fmt.Printlnf("Oneway send success!")
}
```
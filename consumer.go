package rocketmq

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	BrokerSuspendMaxTimeMillis       = 1000 * 15
	FlagCommitOffset           int32 = 0x1 << 0
	FlagSuspend                int32 = 0x1 << 1
	FlagSubscription           int32 = 0x1 << 2
	FlagClassFilter            int32 = 0x1 << 3
)

type MessageListener func(msgs []*MessageExt) error

var DefaultIp = GetLocalIp4()

type Config struct {
	Namesrv      string
	ClientIp     string
	InstanceName string
}

type Consumer interface {
	Start() error
	Shutdown()
	RegisterMessageListener(listener MessageListener)
	Subscribe(topic string, subExpression string)
	UnSubscribe(topic string)
	SendMessageBack(msg MessageExt, delayLevel int) error
	SendMessageBack1(msg MessageExt, delayLevel int, brokerName string) error
	fetchSubscribeMessageQueues(topic string) error
}

type DefaultConsumer struct {
	conf             *Config
	consumerGroup    string
	consumeFromWhere string
	consumerType     string
	messageModel     string
	unitMode         bool
	subscription     map[string]string
	messageListener  MessageListener
	offsetStore      OffsetStore
	brokers          map[string]net.Conn
	rebalance        *Rebalance
	remotingClient   RemotingClient
	mqClient         *MqClient
}

func NewDefaultConsumer(consumerGroup string, conf *Config) (Consumer, error) {
	if conf == nil {
		conf = &Config{
			Namesrv:      os.Getenv("ROCKETMQ_NAMESVR"),
			InstanceName: "DEFAULT",
		}
	}

	if conf.ClientIp == "" {
		conf.ClientIp = DefaultIp
	}

	remotingClient := NewDefaultRemotingClient()
	mqClient := NewMqClient()

	rebalance := NewRebalance()
	rebalance.groupName = consumerGroup
	rebalance.mqClient = mqClient

	offsetStore := new(RemoteOffsetStore)
	offsetStore.mqClient = mqClient
	offsetStore.groupName = consumerGroup
	offsetStore.offsetTable = make(map[MessageQueue]int64)

	pullMessageService := NewPullMessageService()

	consumer := &DefaultConsumer{
		conf:             conf,
		consumerGroup:    consumerGroup,
		consumeFromWhere: "CONSUME_FROM_LAST_OFFSET",
		subscription:     make(map[string]string),
		offsetStore:      offsetStore,
		brokers:          make(map[string]net.Conn),
		rebalance:        rebalance,
		remotingClient:   remotingClient,
		mqClient:         mqClient,
	}

	mqClient.consumerTable[consumerGroup] = consumer
	mqClient.remotingClient = remotingClient
	mqClient.conf = conf
	mqClient.clientId = conf.ClientIp + "@" + strconv.Itoa(os.Getpid())
	mqClient.pullMessageService = pullMessageService

	rebalance.consumer = consumer
	pullMessageService.service = consumer

	return consumer, nil
}

func (c *DefaultConsumer) Start() error {
	c.mqClient.start()
	return nil
}

func (c *DefaultConsumer) Shutdown() {
}

func (c *DefaultConsumer) RegisterMessageListener(messageListener MessageListener) {
	c.messageListener = messageListener
}

func (c *DefaultConsumer) Subscribe(topic string, subExpression string) {
	c.subscription[topic] = subExpression

	subData := &SubscriptionData{
		Topic:     topic,
		SubString: subExpression,
	}
	c.rebalance.subscriptionInner[topic] = subData
}

func (c *DefaultConsumer) UnSubscribe(topic string) {
	delete(c.subscription, topic)
}

func (c *DefaultConsumer) SendMessageBack(msg MessageExt, delayLevel int) error {
	return nil
}

func (c *DefaultConsumer) SendMessageBack1(msg MessageExt, delayLevel int, brokerName string) error {
	return nil
}

func (c *DefaultConsumer) fetchSubscribeMessageQueues(topic string) error {
	return nil
}

func (c *DefaultConsumer) pullMessage(pullRequest *PullRequest) {

	commitOffsetEnable := false
	commitOffsetValue := int64(0)

	commitOffsetValue = c.offsetStore.readOffset(pullRequest.messageQueue, ReadFromMemory)
	if commitOffsetValue > 0 {
		commitOffsetEnable = true
	}

	var sysFlag = int32(0)
	if commitOffsetEnable {
		sysFlag |= FlagCommitOffset
	}

	sysFlag |= FlagSuspend

	subscriptionData, ok := c.rebalance.subscriptionInner[pullRequest.messageQueue.topic]
	var subVersion int64
	var subString string
	if ok {
		subVersion = subscriptionData.SubVersion
		subString = subscriptionData.SubString

		sysFlag |= FlagSubscription
	}

	requestHeader := new(PullMessageRequestHeader)
	requestHeader.ConsumerGroup = pullRequest.consumerGroup
	requestHeader.Topic = pullRequest.messageQueue.topic
	requestHeader.QueueId = pullRequest.messageQueue.queueId
	requestHeader.QueueOffset = pullRequest.nextOffset

	requestHeader.SysFlag = sysFlag
	requestHeader.CommitOffset = commitOffsetValue
	requestHeader.SuspendTimeoutMillis = BrokerSuspendMaxTimeMillis

	if ok {
		requestHeader.SubVersion = subVersion
		requestHeader.Subscription = subString
	}

	pullCallback := func(responseFuture *ResponseFuture) {
		var nextBeginOffset = pullRequest.nextOffset

		if responseFuture != nil {
			responseCommand := responseFuture.responseCommand
			if responseCommand.Code == Success && len(responseCommand.Body) > 0 {
				var err error
				pullResult, ok := responseCommand.ExtFields.(map[string]interface{})
				if ok {
					if nextBeginOffsetInter, ok := pullResult["nextBeginOffset"]; ok {
						if nextBeginOffsetStr, ok := nextBeginOffsetInter.(string); ok {
							nextBeginOffset, err = strconv.ParseInt(nextBeginOffsetStr, 10, 64)
							if err != nil {
								fmt.Println(err)
								return
							}
						}
					}
				}

				msgs := decodeMessage(responseFuture.responseCommand.Body)
				err = c.messageListener(msgs)
				if err != nil {
					fmt.Println(err)
					//TODO retry
				} else {
					c.offsetStore.updateOffset(pullRequest.messageQueue, nextBeginOffset, false)
				}
			} else if responseCommand.Code == PullNotFound {
			} else if responseCommand.Code == PullRetryImmediately || responseCommand.Code == PullOffsetMoved {
				fmt.Printf("pull message error,code=%d,request=%v", responseCommand.Code, requestHeader)
				var err error
				pullResult, ok := responseCommand.ExtFields.(map[string]interface{})
				if ok {
					if nextBeginOffsetInter, ok := pullResult["nextBeginOffset"]; ok {
						if nextBeginOffsetStr, ok := nextBeginOffsetInter.(string); ok {
							nextBeginOffset, err = strconv.ParseInt(nextBeginOffsetStr, 10, 64)
							if err != nil {
								fmt.Println(err)
							}
						}
					}
				}
				//time.Sleep(1 * time.Second)
			} else {
				fmt.Println(fmt.Sprintf("pull message error,code=%d,body=%s", responseCommand.Code, string(responseCommand.Body)))
				fmt.Println(pullRequest.messageQueue)
				time.Sleep(1 * time.Second)
			}
		} else {
			fmt.Println("responseFuture is nil")
		}

		nextPullRequest := &PullRequest{
			consumerGroup: pullRequest.consumerGroup,
			nextOffset:    nextBeginOffset,
			messageQueue:  pullRequest.messageQueue,
		}

		c.mqClient.pullMessageService.pullRequestQueue <- nextPullRequest
	}

	brokerAddr, _, found := c.mqClient.findBrokerAddressInSubscribe(pullRequest.messageQueue.brokerName, 0, false)

	if found {
		currOpaque := atomic.AddInt32(&opaque, 1)
		remotingCommand := new(RemotingCommand)
		remotingCommand.Code = PullMsg
		remotingCommand.Opaque = currOpaque
		remotingCommand.Flag = 0
		remotingCommand.Language = "JAVA"
		remotingCommand.Version = 79

		remotingCommand.ExtFields = requestHeader

		c.remotingClient.invokeAsync(brokerAddr, remotingCommand, 1000, pullCallback)
	}
}

func (c *DefaultConsumer) updateTopicSubscribeInfo(topic string, info []*MessageQueue) {
	if c.rebalance.subscriptionInner != nil {
		c.rebalance.subscriptionInnerLock.RLock()
		_, ok := c.rebalance.subscriptionInner[topic]
		c.rebalance.subscriptionInnerLock.RUnlock()
		if ok {
			c.rebalance.subscriptionInnerLock.Lock()
			c.rebalance.topicSubscribeInfoTable[topic] = info
			c.rebalance.subscriptionInnerLock.Unlock()
		}
	}
}

func (c *DefaultConsumer) subscriptions() []*SubscriptionData {
	subscriptions := make([]*SubscriptionData, 0)
	for _, subscription := range c.rebalance.subscriptionInner {
		subscriptions = append(subscriptions, subscription)
	}
	return subscriptions
}

func (c *DefaultConsumer) doRebalance() {
	c.rebalance.doRebalance()
}

func (c *DefaultConsumer) isSubscribeTopicNeedUpdate(topic string) bool {
	subTable := c.rebalance.subscriptionInner
	if _, ok := subTable[topic]; ok {
		if _, ok := c.rebalance.topicSubscribeInfoTable[topic]; !ok {
			return true
		}
	}
	return false
}

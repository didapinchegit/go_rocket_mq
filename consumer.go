package rocketmq

import "log"
import "net"
import "os"
import "strconv"
import "sync/atomic"

const (
	BrokerSuspendMaxTimeMillis = 1000 * 15
)

type MessageListener func(msgs []*MessageExt)

type Config struct {
	Nameserver   string
	ClientIp     string
	InstanceName string
}

type Consumer interface {
	//Admin
	Start() error
	Shutdown()
	RegisterMessageListener(listener MessageListener)
	Subscribe(topic string, subExpression string)
	UnSubcribe(topic string)
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

	subscription    map[string]string
	messageListener MessageListener
	offsetStore     OffsetStore
	brokers         map[string]net.Conn

	rebalance      *Rebalance
	remotingClient RemotingClient
	mqClient       *MqClient
}

func NewDefaultConsumer(name string, conf *Config) (Consumer, error) {
	if conf == nil {
		conf = &Config{
			Nameserver:   os.Getenv("ROCKETMQ_NAMESVR"),
			InstanceName: "DEFAULT",
		}
	}

	remotingClient := NewDefaultRemotingClient()
	mqClient := NewMqClient()

	rebalance := NewRebalance()
	rebalance.groupName = name
	rebalance.mqClient = mqClient

	offsetStore := new(RemoteOffsetStore)
	offsetStore.mqClient = mqClient
	offsetStore.groupName = name

	pullMessageService := NewPullMessageService()

	consumer := &DefaultConsumer{
		conf:             conf,
		consumerGroup:    name,
		consumeFromWhere: "CONSUME_FROM_LAST_OFFSET",
		subscription:     make(map[string]string),
		offsetStore:      offsetStore,
		brokers:          make(map[string]net.Conn),
		rebalance:        rebalance,
		remotingClient:   remotingClient,
		mqClient:         mqClient,
	}

	mqClient.consumerTable[name] = consumer
	mqClient.remotingClient = remotingClient
	mqClient.conf = conf
	mqClient.clientId = conf.ClientIp + "@" + strconv.Itoa(os.Getpid())
	mqClient.pullMessageService = pullMessageService

	rebalance.consumer = consumer
	pullMessageService.consumer = consumer

	return consumer, nil
}

func (self *DefaultConsumer) Start() error {
	self.mqClient.start()
	return nil
}

func (self *DefaultConsumer) Shutdown() {
}

func (self *DefaultConsumer) RegisterMessageListener(messageListener MessageListener) {
	self.messageListener = messageListener
}

func (self *DefaultConsumer) Subscribe(topic string, subExpression string) {
	self.subscription[topic] = subExpression

	subData := &SubscriptionData{
		Topic:     topic,
		SubString: subExpression,
	}
	self.rebalance.subscriptionInner[topic] = subData
}

func (self *DefaultConsumer) UnSubcribe(topic string) {
	delete(self.subscription, topic)
}

func (self *DefaultConsumer) SendMessageBack(msg MessageExt, delayLevel int) error {
	return nil
}

func (self *DefaultConsumer) SendMessageBack1(msg MessageExt, delayLevel int, brokerName string) error {
	return nil
}

func (self *DefaultConsumer) fetchSubscribeMessageQueues(topic string) error {
	return nil
}

func (self *DefaultConsumer) pullMessage(pullRequest *PullRequest) {

	requestHeader := new(PullMessageRequestHeader)
	requestHeader.ConsumerGroup = pullRequest.consumerGroup
	requestHeader.Topic = pullRequest.messageQueue.topic
	requestHeader.QueueId = pullRequest.messageQueue.queueId
	requestHeader.QueueOffset = pullRequest.nextOffset

	requestHeader.SysFlag = 2
	requestHeader.CommitOffset = 0
	requestHeader.SuspendTimeoutMillis = BrokerSuspendMaxTimeMillis
	requestHeader.Subscription = "*"

	subscriptionData, ok := self.rebalance.subscriptionInner[pullRequest.messageQueue.topic]

	if ok {
		requestHeader.SubVersion = subscriptionData.SubVersion
	}

	currOpaque := atomic.AddInt32(&opaque, 1)
	remotingCommand := new(RemotingCommand)
	remotingCommand.Code = PULL_MESSAGE
	remotingCommand.Opaque = currOpaque
	remotingCommand.Flag = 0
	remotingCommand.Language = "JAVA"
	remotingCommand.Version = 79

	remotingCommand.ExtFields = requestHeader

	brokerAddr, _, found := self.mqClient.findBrokerAddressInSubscribe(pullRequest.messageQueue.brokerName, 0, false)

	pullCallback := func(responseFuture *ResponseFuture) {
		if responseFuture.responseCommand.Code == 0 && len(responseFuture.responseCommand.Body) > 0 {
			var nextBeginOffset int64
			var err error
			pullResult, ok := responseFuture.responseCommand.ExtFields.(map[string]interface{})
			if ok {
				if nextBeginOffsetInter, ok := pullResult["nextBeginOffset"]; ok {
					if nextBeginOffsetStr, ok := nextBeginOffsetInter.(string); ok {
						nextBeginOffset, err = strconv.ParseInt(nextBeginOffsetStr, 10, 64)
						if err != nil {
							log.Print(err)
							return
						}

					}

				}

			}

			nextPullRequest := &PullRequest{
				consumerGroup: pullRequest.consumerGroup,
				nextOffset:    nextBeginOffset,
				messageQueue:  pullRequest.messageQueue,
			}

			self.mqClient.pullMessageService.pullRequestQueue <- nextPullRequest

			msgs := decodeMessage(responseFuture.responseCommand.Body)
			self.messageListener(msgs)
		}
	}

	if found {
		self.remotingClient.invokeAsync(brokerAddr, remotingCommand, 1000, pullCallback)
	}
}

func (self *DefaultConsumer) updateTopicSubscribeInfo(topic string, info []*MessageQueue) {
	if self.rebalance.subscriptionInner != nil {
		_, ok := self.rebalance.subscriptionInner[topic]
		if ok {
			self.rebalance.topicSubscribeInfoTable[topic] = info
		}
	}
}

func (self *DefaultConsumer) subscriptions() []*SubscriptionData {
	subscriptions := make([]*SubscriptionData, 0)
	for _, subscription := range self.rebalance.subscriptionInner {
		subscriptions = append(subscriptions, subscription)
	}
	return subscriptions
}

func (self *DefaultConsumer) doRebalance() {
	self.rebalance.doRebalance()
}

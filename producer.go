package rocketmq

import (
	_ "encoding/binary"
	_ "encoding/json"
	"errors"
	"fmt"
	_ "fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	_ "sync/atomic"
	"time"
)

type Producer interface {
	Start() error
	Shutdown()
	Send(msg *Message) (*SendResult, error)
	SendAsync(msg *Message, sendCallback SendCallback) error
	SendOneway(msg *Message) error
}

// communicationMode
const (
	Sync = iota
	Async
	Oneway
)

// ServiceState
const (
	CreateJust = iota
	Running
	ShutdownAlready
	StartFailed
)

type SendCallback func() error

type DefaultProducer struct {
	conf          *Config
	producerGroup string
	producerType  string

	rebalance             *Rebalance
	remotingClient        RemotingClient
	mqClient              *MqClient
	topicPublishInfoTable map[string]*TopicPublishInfo
	instanceName          string

	sendMsgTimeout                   int64
	serviceState                     int
	createTopicKey                   string
	defaultTopicQueueNums            int
	compressMsgBodyOverHowmuch       int
	retryTimesWhenSendFailed         int
	retryTimesWhenSendAsyncFailed    int
	retryAnotherBrokerWhenNotStoreOK bool
	maxMessageSize                   int

	vipChannelEnabled bool
}

func NewDefaultProducer(producerGroup string, conf *Config) (Producer, error) {
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
	pullMessageService := NewPullMessageService()
	producer := &DefaultProducer{
		conf:          conf,
		producerGroup: producerGroup,

		remotingClient:        remotingClient,
		mqClient:              mqClient,
		topicPublishInfoTable: make(map[string]*TopicPublishInfo),

		sendMsgTimeout:                   3000,
		instanceName:                     "DEFAULT",
		serviceState:                     CreateJust,
		createTopicKey:                   DefaultTopic,
		defaultTopicQueueNums:            4,
		compressMsgBodyOverHowmuch:       1024 * 4,
		retryTimesWhenSendFailed:         2,
		retryTimesWhenSendAsyncFailed:    2,
		retryAnotherBrokerWhenNotStoreOK: false,
		maxMessageSize:                   1024 * 1024 * 4, // 4M
	}

	mqClient.remotingClient = remotingClient
	mqClient.conf = conf
	mqClient.clientId = conf.ClientIp + "@" + strconv.Itoa(os.Getpid())
	mqClient.pullMessageService = pullMessageService
	mqClient.defaultProducer = producer

	pullMessageService.service = producer

	return producer, nil
}

func (d *DefaultProducer) start(startFactory bool) (err error) {
	switch d.serviceState {
	case CreateJust:
		d.serviceState = StartFailed
		d.checkConfig()
		if d.producerGroup == ClientInnerProducerGroup {
			if d.instanceName == "DEFAULT" {
				d.instanceName = strconv.Itoa(os.Getpid())
			}
		}

		if registerOK := d.mqClient.registerProducer(d.producerGroup, d); !registerOK {
			d.serviceState = CreateJust
			err = errors.New("The producer group[" + d.producerGroup + "] has been created before, specify another name please.")
			return
		}

		topicPublishInfo := NewTopicPublishInfo()
		d.topicPublishInfoTable[d.createTopicKey] = topicPublishInfo
		if startFactory {
			d.mqClient.start()
		}
		d.serviceState = Running

	case Running, ShutdownAlready, StartFailed:
		err = errors.New("The producer service state not OK, maybe started once," + strconv.Itoa(d.serviceState))
	}
	return
}

func (d *DefaultProducer) Start() error {
	return d.start(true)
}

func (d *DefaultProducer) checkConfig() (err error) {
	if d.producerGroup == DefaultProducerGroup {
		err = errors.New("producerGroup can not equal " + DefaultProducerGroup + ", please specify another one.")
	}
	return
}

func (d *DefaultProducer) makeSureStateOK() (err error) {
	if d.serviceState != Running {
		err = errors.New("The producer service state not OK," + strconv.Itoa(d.serviceState))
	}
	return
}

func (d *DefaultProducer) Shutdown() {
}

func (d *DefaultProducer) Send(msg *Message) (*SendResult, error) {
	return d.send(msg, Sync, nil, d.sendMsgTimeout)
}

func (d *DefaultProducer) SendAsync(msg *Message, sendCallback SendCallback) (err error) {
	_, err = d.send(msg, Async, sendCallback, d.sendMsgTimeout)
	return
}

func (d *DefaultProducer) SendOneway(msg *Message) error {
	d.send(msg, Oneway, nil, d.sendMsgTimeout)
	return nil
}

func (d *DefaultProducer) send(msg *Message, communicationMode int, sendCallback SendCallback, timeout int64) (sendResult *SendResult, err error) {
	if err = d.makeSureStateOK(); err != nil {
		return
	}
	if err = msg.checkMessage(d); err != nil {
		return
	}
	topicPublishInfo := d.tryToFindTopicPublishInfo(msg.Topic)

	// TODO handle topicPublishInfo
	if topicPublishInfo.ok() {
		timesTotal := 1
		if communicationMode == Sync {
			timesTotal = 1 + d.getRetryTimesWhenSendFailed()
		}

		var mq *MessageQueue
		brokersSent := make([]string, 0)

		for times := 0; times < timesTotal; times++ {
			lastBrokerName := ""
			if mq == nil {
				lastBrokerName = ""
			} else {
				lastBrokerName = mq.getBrokerName()
			}
			tmpmq := topicPublishInfo.selectOneMessageQueue(lastBrokerName)
			if tmpmq != nil {
				mq = tmpmq
				brokersSent = append(brokersSent, mq.getBrokerName())
				beginTimestampPrev := time.Now().Unix()
				sendResult, err = d.sendKernel(msg, mq, communicationMode, sendCallback, topicPublishInfo, timeout)
				endTimestamp := time.Now().Unix()
				d.updateFaultItem(mq.getBrokerName(), endTimestamp-beginTimestampPrev, false)

				switch communicationMode {
				case Async:
					return
				case Oneway:
					return
				case Sync:
					if sendResult.sendStatus != SendStatusOK {
						if d.retryAnotherBrokerWhenNotStoreOK {
							continue
						}
					}
					return sendResult, nil
				default:
				}
			}
		}
	}

	if d.remotingClient.getNameServerAddressList() == nil || len(d.remotingClient.getNameServerAddressList()) == 0 {
		err = errors.New("No name server address, please set it")
	}
	err = errors.New("No route info of this topic," + msg.Topic)
	return
}

func (d *DefaultProducer) getRetryTimesWhenSendFailed() int {
	return d.retryTimesWhenSendFailed
}

func (d *DefaultProducer) tryToFindTopicPublishInfo(topic string) (topicPublishInfo *TopicPublishInfo) {
	ok := false
	if topicPublishInfo, ok = d.topicPublishInfoTable[topic]; !ok || !topicPublishInfo.ok() {
		d.topicPublishInfoTable[topic] = NewTopicPublishInfo()
		d.mqClient.updateTopicRouteInfoFromNameServerKernel(topic, false, DefaultProducer{})
		topicPublishInfo = d.topicPublishInfoTable[topic]
	}

	// TODO handle topicPublishInfo
	if !topicPublishInfo.haveTopicRouterInfo && !topicPublishInfo.ok() {
		d.mqClient.updateTopicRouteInfoFromNameServerKernel(topic, true, *d)
		topicPublishInfo = d.topicPublishInfoTable[topic]
	}

	return
}

func (d *DefaultProducer) sendKernel(msg *Message, mq *MessageQueue, communicationMode int, sendCallback SendCallback,
	topicPublishInfo *TopicPublishInfo, timeout int64) (sendResult *SendResult, err error) {
	brokerAddr := d.mqClient.findBrokerAddressInPublish(mq.getBrokerName())
	if brokerAddr == "" {
		d.tryToFindTopicPublishInfo(msg.Topic)
		brokerAddr = d.mqClient.findBrokerAddressInPublish(mq.getBrokerName())
	}
	context := newSendMessageContext()
	if brokerAddr != "" {
		brokerAddr = BrokerVIPChannel(d.vipChannelEnabled, brokerAddr)
		prevBody := msg.Body

		MessageClientIDSetter.setUniqID(msg)

		sysFlag := 0
		if d.tryToCompressMessage(msg) {
			sysFlag |= CompressedFlag
		}

		tranMsg := msg.Properties[MessageConst.PropertyTransactionPrepared]
		if b, err := strconv.ParseBool(tranMsg); tranMsg != "" && err == nil && b {
			sysFlag |= TransactionPreparedType
		}

		if d.hasCheckForbiddenHook() {
			fmt.Fprintf(os.Stderr, brokerAddr)
		}
		if d.hasSendMessageHook() {
		}

		requestHeader := new(SendMessageRequestHeader)
		requestHeader.ProducerGroup = d.producerGroup
		requestHeader.Topic = msg.Topic
		requestHeader.DefaultTopic = d.createTopicKey
		requestHeader.DefaultTopicQueueNums = d.defaultTopicQueueNums
		requestHeader.QueueId = mq.queueId
		requestHeader.SysFlag = sysFlag
		requestHeader.Properties = messageProperties2String(msg.Properties)
		requestHeader.ReconsumeTimes = 0

		if strings.HasPrefix(requestHeader.Topic, RetryGroupTopicPrefix) {
			if MessageConst.PropertyReconsumeTime != "" {
				requestHeader.ReconsumeTimes, _ = strconv.Atoi(MessageConst.PropertyReconsumeTime)
				delete(msg.Properties, MessageConst.PropertyReconsumeTime)
			}
			if MessageConst.PropertyMaxReconsumeTimes != "" {
				requestHeader.MaxReconsumeTimes, _ = strconv.Atoi(MessageConst.PropertyMaxReconsumeTimes)
				delete(msg.Properties, MessageConst.PropertyMaxReconsumeTimes)
			}
		}

		switch communicationMode {
		case Async:
			sendResult, err = d.sendMessage(
				brokerAddr,
				mq.brokerName,
				msg,
				requestHeader,
				timeout,
				communicationMode,
				sendCallback,
				topicPublishInfo,
				d.mqClient,
				d.retryTimesWhenSendAsyncFailed,
				context)
		case Oneway, Sync:
			sendResult, err = d.sendMessage(
				brokerAddr,
				mq.brokerName,
				msg,
				requestHeader,
				timeout,
				communicationMode,
				sendCallback,
				topicPublishInfo,
				d.mqClient,
				d.retryTimesWhenSendFailed,
				context)
		}
		if d.hasSendMessageHook() {
			if msg.Properties[MessageConst.PropertyTransactionPrepared] == "true" {
				context.msgType = strconv.Itoa(TransMsgHalf)
			}
			if msg.Properties["__STARTDELIVERTIME"] != "" || msg.Properties[MessageConst.PropertyDelayTimeLevel] != "" {
				context.msgType = strconv.Itoa(DelayMsg)
			}
		}

		msg.Body = prevBody
	}
	return
}

func (d *DefaultProducer) tryToCompressMessage(msg *Message) bool {
	return false
	// TODO add compression
	//body := msg.Body
	//if body != nil && len(body) != 0 {
	//}
}

func (d *DefaultProducer) hasCheckForbiddenHook() bool {
	return false
}

func (d *DefaultProducer) hasSendMessageHook() bool {
	return false
}

func (d *DefaultProducer) pullMessage(pullRequest *PullRequest) {
	return
}

func (d *DefaultProducer) sendMessage(addr string, brokerName string, msg *Message, requestHeader *SendMessageRequestHeader,
	timeoutMillis int64, communicationMode int, sendCallback SendCallback, topicPublishInfo *TopicPublishInfo, mqClient *MqClient,
	retryTimesWhenSendFailed int, context *SendMessageContext) (sendResult *SendResult, err error) {
	remotingCommand := new(RemotingCommand)
	remotingCommand.Code = SendMsg
	currOpaque := atomic.AddInt32(&opaque, 1)
	remotingCommand.Opaque = currOpaque
	remotingCommand.Flag = 0
	remotingCommand.Language = "JAVA"
	remotingCommand.Version = 79
	remotingCommand.ExtFields = requestHeader
	remotingCommand.Body = msg.Body

	switch communicationMode {
	case Async:
		times := atomic.AddInt32(&opaque, 1)
		d.sendMessageAsync(addr, brokerName, msg, timeoutMillis, remotingCommand, sendCallback, topicPublishInfo, mqClient,
			retryTimesWhenSendFailed, times, context, d)
	case Oneway:
		err = d.remotingClient.invokeOneway(addr, remotingCommand, timeoutMillis)
	case Sync:
		sendResult, err = d.sendMessageSync(addr, brokerName, msg, timeoutMillis, remotingCommand)
	}

	return
}

func (d *DefaultProducer) sendMessageSync(addr string, brokerName string, msg *Message, timeoutMillis int64, remotingCommand *RemotingCommand) (sendResult *SendResult, err error) {
	var response *RemotingCommand
	fmt.Fprintln(os.Stderr, "msg:", msg.Topic, msg.Flag, string(msg.Body), msg.Properties)
	if response, err = d.remotingClient.invokeSync(addr, remotingCommand, timeoutMillis); err != nil {
		fmt.Fprintln(os.Stderr, "sendMessageSync err", err)
	}
	return d.processSendResponse(brokerName, msg, response)
}

func (d *DefaultProducer) sendMessageAsync(addr string, brokerName string, msg *Message, timeoutMillis int64,
	remotingCommand *RemotingCommand, sendCallback SendCallback, topicPublishInfo *TopicPublishInfo, mqClient *MqClient,
	retryTimesWhenSendFailed int, times int32, context *SendMessageContext, producer *DefaultProducer) (err error) {
	invokeCallback := func(responseFuture *ResponseFuture) {
		var sendResult *SendResult
		responseCommand := responseFuture.responseCommand

		if sendCallback == nil && responseCommand != nil {
			sendResult, err = d.processSendResponse(brokerName, msg, responseCommand)
			if err != nil && context != nil && sendResult != nil {
				context.sendResult = sendResult
				// TODO add send hook
			}
			d.updateFaultItem(brokerName, time.Now().Unix()-responseFuture.beginTimestamp, false)
			return
		}

		sendCallback()
		if responseCommand != nil {
			if sendResult, err = d.processSendResponse(brokerName, msg, responseCommand); sendResult == nil || err != nil {
				fmt.Fprintln(os.Stderr, "sendResult can't be null, error ", err)
				producer.updateFaultItem(brokerName, time.Now().Unix()-responseFuture.beginTimestamp, true)
				return
			} else {
				if context != nil {
					context.sendResult = sendResult
					// TODO add send hook
					producer.updateFaultItem(brokerName, time.Now().Unix()-responseFuture.beginTimestamp, false)
				}
			}
		}
		producer.updateFaultItem(brokerName, time.Now().Unix()-responseFuture.beginTimestamp, true)
	}
	err = d.remotingClient.invokeAsync(addr, remotingCommand, 1000, invokeCallback)
	return
}

func (d *DefaultProducer) processSendResponse(brokerName string, msg *Message, response *RemotingCommand) (sendResult *SendResult, err error) {
	var sendStatus int

	if response == nil {
		err = errors.New("response in processSendResponse is nil!")
		return
	}
	switch response.Code {
	// TODO add log
	case FlushDiskTimeout:
		sendStatus = SendStatusFlushDiskTimeout
	case FlushSlaveTimeout:
		sendStatus = SendStatusFlushSlaveTimeout
	case SlaveNotAvailable:
		sendStatus = SendStatusSlaveNotAvailable
	case Success:
		sendStatus = SendStatusOK
		responseHeader := response.decodeCommandCustomHeader()
		messageQueue := NewMessageQueue(msg.Topic, brokerName, responseHeader.queueId)

		sendResult = NewSendResult(sendStatus, MessageClientIDSetter.getUniqID(msg), responseHeader.msgId, messageQueue, responseHeader.queueOffset)
		sendResult.transactionId = responseHeader.transactionId

		pullResult, ok := response.ExtFields.(map[string]interface{})
		if ok {
			if regionId, ok := pullResult[MessageConst.PropertyMsgRegion]; ok {
				if regionId == nil || regionId == "" {
					regionId = "DefaultRegion"
				}
				sendResult.regionId = regionId.(string)
			}
		}
		return
	}
	err = errors.New("processSendResponse error")

	return
}

func (d *DefaultProducer) updateFaultItem(brokerName string, currentLatency int64, isolation bool) {
}

func (d *DefaultProducer) getPublishTopicList() (topicList []string) {
	topicList = make([]string, 0)
	for topic := range d.topicPublishInfoTable {
		topicList = append(topicList, topic)
	}

	return
}

func (d *DefaultProducer) isPublishTopicNeedUpdate(topic string) bool {
	prev := d.topicPublishInfoTable[topic]
	return !prev.ok()
}

func (d *DefaultProducer) updateTopicPublishInfo(topic string, topicPublishInfo *TopicPublishInfo) {
	if topic != "" {
		d.topicPublishInfoTable[topic] = topicPublishInfo
	}
}

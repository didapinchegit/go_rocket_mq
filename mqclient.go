package rocketmq

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type GetRouteInfoRequestHeader struct {
	topic string
}

func (self *GetRouteInfoRequestHeader) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString("{\"topic\":\"")
	buf.WriteString(self.topic)
	buf.WriteString("\"}")
	return buf.Bytes(), nil
}

type QueueData struct {
	BrokerName     string
	ReadQueueNums  int32
	WriteQueueNums int32
	Perm           int32
	TopicSynFlag   int32
}

type BrokerData struct {
	BrokerName  string
	BrokerAddrs map[string]string
}

type TopicRouteData struct {
	OrderTopicConf string
	QueueDatas     []*QueueData
	BrokerDatas    []*BrokerData
}

type MqClient struct {
	clientId           string
	conf               *Config
	brokerAddrTable    map[string]map[string]string //map[brokerName]map[bokerId]addrs
	consumerTable      map[string]*DefaultConsumer
	topicRouteTable    map[string]*TopicRouteData
	remotingClient     RemotingClient
	pullMessageService *PullMessageService
	mutex              sync.Mutex
}

func NewMqClient() *MqClient {
	return &MqClient{
		brokerAddrTable: make(map[string]map[string]string),
		consumerTable:   make(map[string]*DefaultConsumer),
		topicRouteTable: make(map[string]*TopicRouteData),
	}
}
func (self *MqClient) findBrokerAddressInSubscribe(brokerName string, brokerId int64, onlyThisBroker bool) (brokerAddr string, slave bool, found bool) {
	slave = false
	found = false

	brokerMap, ok := self.brokerAddrTable[brokerName]
	if ok {
		brokerAddr, ok = brokerMap[strconv.FormatInt(brokerId, 10)]
		slave = (brokerId != 0)
		found = ok

		if !found && !onlyThisBroker {
			var id string
			for id, brokerAddr = range brokerMap {
				slave = (id != "0")
				found = true
				break
			}
		}
	}

	return
}

func (self *MqClient) findBrokerAddressInAdmin(brokerName string) (addr string, found, slave bool) {
	found = false
	slave = false
	brokers, ok := self.brokerAddrTable[brokerName]
	if ok {
		for brokerId, addr := range brokers {

			if addr != "" {
				found = true
				if brokerId == "0" {
					slave = false
				} else {
					slave = true
				}
				break
			}
		}
	}

	return
}

func (self *MqClient) findBrokerAddrByTopic(topic string) (addr string, ok bool) {
	topicRouteData, ok := self.topicRouteTable[topic]
	if !ok {
		return "", ok
	}

	brokers := topicRouteData.BrokerDatas
	if brokers != nil && len(brokers) > 0 {
		brokerData := brokers[0]
		if ok {
			addr, ok = brokerData.BrokerAddrs["0"]

			if ok {
				return
			}
			for _, addr = range brokerData.BrokerAddrs {
				return addr, ok
			}
		}
	}
	return
}
func (self *MqClient) findConsumerIdList(topic string, groupName string) ([]string, error) {
	brokerAddr, ok := self.findBrokerAddrByTopic(topic)
	if !ok {
		self.updateTopicRouteInfoFromNameServerByTopic(topic)
		brokerAddr, ok = self.findBrokerAddrByTopic(topic)
	}

	if ok {
		return self.getConsumerIdListByGroup(brokerAddr, groupName, 3000)
	}

	return nil, errors.New("can't find broker")

}

type GetConsumerListByGroupRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
}

type GetConsumerListByGroupResponseBody struct {
	ConsumerIdList []string
}

func (self *MqClient) getConsumerIdListByGroup(addr string, consumerGroup string, timeoutMillis int64) ([]string, error) {
	requestHeader := new(GetConsumerListByGroupRequestHeader)
	requestHeader.ConsumerGroup = consumerGroup

	currOpaque := atomic.AddInt32(&opaque, 1)
	request := &RemotingCommand{
		Code:      GET_CONSUMER_LIST_BY_GROUP,
		Language:  "JAVA",
		Version:   79,
		Opaque:    currOpaque,
		Flag:      0,
		ExtFields: requestHeader,
	}

	response, err := self.remotingClient.invokeSync(addr, request, timeoutMillis)
	if err != nil {
		log.Print(err)
		return nil, err
	}

	if response.Code == SUCCESS {
		getConsumerListByGroupResponseBody := new(GetConsumerListByGroupResponseBody)
		bodyjson := strings.Replace(string(response.Body), "0:", "\"0\":", -1)
		bodyjson = strings.Replace(bodyjson, "1:", "\"1\":", -1)
		err := json.Unmarshal([]byte(bodyjson), getConsumerListByGroupResponseBody)
		if err != nil {
			log.Print(err)
			return nil, err
		}
		return getConsumerListByGroupResponseBody.ConsumerIdList, nil
	}

	log.Print(response)
	return nil, errors.New("getConsumerIdListByGroup error")
}

func (self *MqClient) getTopicRouteInfoFromNameServer(topic string, timeoutMillis int64) (*TopicRouteData, error) {
	requestHeader := &GetRouteInfoRequestHeader{
		topic: topic,
	}

	remotingCommand := new(RemotingCommand)
	remotingCommand.Code = GET_ROUTEINTO_BY_TOPIC
	currOpaque := atomic.AddInt32(&opaque, 1)
	remotingCommand.Opaque = currOpaque
	remotingCommand.Flag = 0
	remotingCommand.Language = "JAVA"
	remotingCommand.Version = 79

	remotingCommand.ExtFields = requestHeader
	response, err := self.remotingClient.invokeSync(self.conf.Nameserver, remotingCommand, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response.Code == SUCCESS {
		topicRouteData := new(TopicRouteData)
		bodyjson := strings.Replace(string(response.Body), "0:", "\"0\":", -1)
		bodyjson = strings.Replace(bodyjson, "1:", "\"1\":", -1)
		err = json.Unmarshal([]byte(bodyjson), topicRouteData)
		if err != nil {
			log.Print(err)
			return nil, err
		}
		return topicRouteData, nil
	} else {
		return nil, errors.New("getTopicRouteInfoFromNameServer error:" + response.remark)
	}

}

func (self *MqClient) updateTopicRouteInfoFromNameServer() {
	for _, consumer := range self.consumerTable {
		subscriptions := consumer.subscriptions()
		for _, subData := range subscriptions {
			self.updateTopicRouteInfoFromNameServerByTopic(subData.Topic)
		}
	}
}

func (self *MqClient) updateTopicRouteInfoFromNameServerByTopic(topic string) error {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	topicRouteData, err := self.getTopicRouteInfoFromNameServer(topic, 3000*1000)
	if err != nil {
		log.Print(err)
		return err
	}

	for _, bd := range topicRouteData.BrokerDatas {
		self.brokerAddrTable[bd.BrokerName] = bd.BrokerAddrs
	}

	mqList := make([]*MessageQueue, 0)
	for _, queueData := range topicRouteData.QueueDatas {
		var i int32
		for i = 0; i < queueData.ReadQueueNums; i++ {
			mq := &MessageQueue{
				topic:      topic,
				brokerName: queueData.BrokerName,
				queueId:    i,
			}

			mqList = append(mqList, mq)
		}
	}

	for _, consumer := range self.consumerTable {
		consumer.updateTopicSubscribeInfo(topic, mqList)
	}

	self.topicRouteTable[topic] = topicRouteData

	return nil
}

type ConsumerData struct {
	GroupName           string
	ConsumerType        string
	MessageModel        string
	ConsumeFromWhere    string
	SubscriptionDataSet []*SubscriptionData
	UnitMode            bool
}

type HeartbeatData struct {
	ClientId        string
	ConsumerDataSet []*ConsumerData
}

func (self *MqClient) prepareHeartbeatData() *HeartbeatData {
	heartbeatData := new(HeartbeatData)
	heartbeatData.ClientId = self.clientId
	heartbeatData.ConsumerDataSet = make([]*ConsumerData, 0)
	for group, consumer := range self.consumerTable {
		consumerData := new(ConsumerData)
		consumerData.GroupName = group
		consumerData.ConsumerType = consumer.consumerType
		consumerData.ConsumeFromWhere = consumer.consumeFromWhere
		consumerData.MessageModel = consumer.messageModel
		consumerData.SubscriptionDataSet = consumer.subscriptions()
		consumerData.UnitMode = consumer.unitMode

		heartbeatData.ConsumerDataSet = append(heartbeatData.ConsumerDataSet, consumerData)
	}
	return heartbeatData
}

func (self *MqClient) sendHeartbeatToAllBrokerWithLock() error {
	heartbeatData := self.prepareHeartbeatData()
	if len(heartbeatData.ConsumerDataSet) == 0 {
		return errors.New("send heartbeat error")
	}

	for _, brokerTable := range self.brokerAddrTable {
		for brokerId, addr := range brokerTable {
			if addr == "" || brokerId != "0" {
				continue
			}
			currOpaque := atomic.AddInt32(&opaque, 1)
			remotingCommand := &RemotingCommand{
				Code:     HEART_BEAT,
				Language: "JAVA",
				Version:  79,
				Opaque:   currOpaque,
				Flag:     0,
			}

			data, err := json.Marshal(*heartbeatData)
			if err != nil {
				log.Print(err)
				return err
			}
			remotingCommand.Body = data
			response, err := self.remotingClient.invokeSync(addr, remotingCommand, 3000)
			if err != nil {
				log.Print(err)
				return err
			} else {
				if response.Code != 0 {
					return errors.New("send heartbeat error")
				} else {
					return nil
				}
			}
		}
	}
	return nil
}

func (self *MqClient) startScheduledTask() {
	go func() {
		updateTopicRouteTimer := time.NewTimer(5 * time.Second)
		for {
			<-updateTopicRouteTimer.C
			self.updateTopicRouteInfoFromNameServer()
			updateTopicRouteTimer.Reset(5 * time.Second)
		}
	}()

	go func() {
		heartbeatTimer := time.NewTimer(10 * time.Second)
		for {
			<-heartbeatTimer.C
			self.sendHeartbeatToAllBrokerWithLock()
			heartbeatTimer.Reset(10 * time.Second)
		}
	}()

	go func() {
		rebalanceTimer := time.NewTimer(15 * time.Second)
		for {
			<-rebalanceTimer.C
			self.doRebalance()
			rebalanceTimer.Reset(10 * time.Second)
		}
	}()
}

func (self *MqClient) doRebalance() {
	for _, consumer := range self.consumerTable {
		consumer.doRebalance()
	}
}

func (self *MqClient) start() {
	self.startScheduledTask()
	go self.pullMessageService.start()
}

type QueryConsumerOffsetRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueId       int32  `json:"queueId"`
}

func (self *MqClient) queryConsumerOffset(addr string, requestHeader *QueryConsumerOffsetRequestHeader, timeoutMillis int64) (int64, error) {
	currOpaque := atomic.AddInt32(&opaque, 1)
	remotingCommand := &RemotingCommand{
		Code:     QUERY_CONSUMER_OFFSET,
		Language: "JAVA",
		Version:  79,
		Opaque:   currOpaque,
		Flag:     0,
	}

	remotingCommand.ExtFields = requestHeader
	reponse, err := self.remotingClient.invokeSync(addr, remotingCommand, timeoutMillis)

	if err != nil {
		log.Print(err)
		return 0, err
	}

	if extFields, ok := (reponse.ExtFields).(map[string]interface{}); ok {
		if offsetInter, ok := extFields["offset"]; ok {
			if offsetStr, ok := offsetInter.(string); ok {
				offset, err := strconv.ParseInt(offsetStr, 10, 64)
				if err != nil {
					log.Print(err)
					return 0, err
				}
				return offset, nil

			}
		}
	}

	log.Print(reponse)
	return 0, errors.New("query offset error")
}

func (self *MqClient) updateConsumerOffsetOneway(addr string, header *UpdateConsumerOffsetRequestHeader, timeoutMillis int64) {

	currOpaque := atomic.AddInt32(&opaque, 1)
	remotingCommand := &RemotingCommand{
		Code:      QUERY_CONSUMER_OFFSET,
		Language:  "JAVA",
		Version:   79,
		Opaque:    currOpaque,
		Flag:      0,
		ExtFields: header,
	}

	self.remotingClient.invokeSync(addr, remotingCommand, timeoutMillis)
}

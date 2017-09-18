package rocketmq

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type GetRouteInfoRequestHeader struct {
	topic string
}

func (g *GetRouteInfoRequestHeader) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString("{\"topic\":\"")
	buf.WriteString(g.topic)
	buf.WriteString("\"}")
	return buf.Bytes(), nil
}

type QueueData struct {
	BrokerName     string
	ReadQueueNums  int32
	WriteQueueNums int32
	Perm           int
	TopicSynFlag   int32
}

type BrokerData struct {
	BrokerName      string
	BrokerAddrs     map[string]string
	BrokerAddrsLock sync.RWMutex
}

type TopicRouteData struct {
	OrderTopicConf string
	QueueDatas     []*QueueData
	BrokerDatas    []*BrokerData
}

type MqClient struct {
	clientId            string
	conf                *Config
	brokerAddrTable     map[string]map[string]string //map[brokerName]map[bokerId]addrs
	brokerAddrTableLock sync.RWMutex
	consumerTable       map[string]*DefaultConsumer
	consumerTableLock   sync.RWMutex
	producerTable       map[string]*DefaultProducer
	producerTableLock   sync.RWMutex
	topicRouteTable     map[string]*TopicRouteData
	topicRouteTableLock sync.RWMutex
	remotingClient      RemotingClient
	pullMessageService  *PullMessageService
	defaultProducer     *DefaultProducer
	serviceState        int
}

func NewMqClient() *MqClient {
	return &MqClient{
		brokerAddrTable: make(map[string]map[string]string),
		consumerTable:   make(map[string]*DefaultConsumer),
		producerTable:   make(map[string]*DefaultProducer),
		topicRouteTable: make(map[string]*TopicRouteData),
	}
}

type slice interface {
	Len() int
}

func sliceCompare(a, b interface{}) bool {
	switch a.(type) {
	case []*QueueData, []*BrokerData:
		if a == nil && b == nil {
			return true
		}
		if a == nil || b == nil {
			return false
		}
		if len(a.([]interface{})) != len(b.([]interface{})) {
			return false
		}
		for i := range a.([]interface{}) {
			if a.([]interface{})[i] != b.([]interface{})[i] {
				return false
			}
		}
		return true
	}
	return false
}

func (m *MqClient) findBrokerAddressInSubscribe(brokerName string, brokerId int64, onlyThisBroker bool) (brokerAddr string, slave bool, found bool) {
	slave = false
	found = false
	m.brokerAddrTableLock.RLock()
	brokerMap, ok := m.brokerAddrTable[brokerName]
	m.brokerAddrTableLock.RUnlock()
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

func (m *MqClient) findBrokerAddressInAdmin(brokerName string) (addr string, found, slave bool) {
	found = false
	slave = false
	m.brokerAddrTableLock.RLock()
	brokers, ok := m.brokerAddrTable[brokerName]
	m.brokerAddrTableLock.RUnlock()
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

func (m *MqClient) findBrokerAddrByTopic(topic string) (addr string, ok bool) {
	m.topicRouteTableLock.RLock()
	topicRouteData, ok := m.topicRouteTable[topic]
	m.topicRouteTableLock.RUnlock()
	if !ok {
		return "", ok
	}

	brokers := topicRouteData.BrokerDatas
	if brokers != nil && len(brokers) > 0 {
		brokerData := brokers[0]
		if ok {
			brokerData.BrokerAddrsLock.RLock()
			addr, ok = brokerData.BrokerAddrs["0"]
			brokerData.BrokerAddrsLock.RUnlock()

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

func (m *MqClient) findConsumerIdList(topic string, groupName string) ([]string, error) {
	brokerAddr, ok := m.findBrokerAddrByTopic(topic)
	if !ok {
		_, err := m.updateTopicRouteInfoFromNameServerKernel(topic, false, DefaultProducer{})
		fmt.Fprintln(os.Stderr, err)
		brokerAddr, ok = m.findBrokerAddrByTopic(topic)
	}

	if ok {
		return m.getConsumerIdListByGroup(brokerAddr, groupName, 3000)
	}

	return nil, errors.New("can't find broker")

}

type GetConsumerListByGroupRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
}

type GetConsumerListByGroupResponseBody struct {
	ConsumerIdList []string
}

func (m *MqClient) getConsumerIdListByGroup(addr string, consumerGroup string, timeoutMillis int64) ([]string, error) {
	requestHeader := new(GetConsumerListByGroupRequestHeader)
	requestHeader.ConsumerGroup = consumerGroup

	currOpaque := atomic.AddInt32(&opaque, 1)
	request := &RemotingCommand{
		Code:      GetConsumerListByGroup,
		Language:  "JAVA",
		Version:   79,
		Opaque:    currOpaque,
		Flag:      0,
		ExtFields: requestHeader,
	}

	response, err := m.remotingClient.invokeSync(addr, request, timeoutMillis)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return nil, err
	}

	if response.Code == Success {
		getConsumerListByGroupResponseBody := new(GetConsumerListByGroupResponseBody)
		bodyjson := strings.Replace(string(response.Body), "0:", "\"0\":", -1)
		bodyjson = strings.Replace(bodyjson, "1:", "\"1\":", -1)
		err := json.Unmarshal([]byte(bodyjson), getConsumerListByGroupResponseBody)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return nil, err
		}
		return getConsumerListByGroupResponseBody.ConsumerIdList, nil
	}

	return nil, errors.New("getConsumerIdListByGroup error")
}

func (m *MqClient) getTopicRouteInfoFromNameServer(topic string, timeoutMillis int64) (*TopicRouteData, error) {
	requestHeader := &GetRouteInfoRequestHeader{
		topic: topic,
	}

	remotingCommand := new(RemotingCommand)
	remotingCommand.Code = GetRouteinfoByTopic
	currOpaque := atomic.AddInt32(&opaque, 1)
	remotingCommand.Opaque = currOpaque
	remotingCommand.Flag = 0
	remotingCommand.Language = "JAVA"
	remotingCommand.Version = 79

	remotingCommand.ExtFields = requestHeader

	response, err := m.remotingClient.invokeSync(m.conf.Namesrv, remotingCommand, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response.Code == Success {
		topicRouteData := new(TopicRouteData)
		bodyjson := strings.Replace(string(response.Body), ",0:", ",\"0\":", -1)
		bodyjson = strings.Replace(bodyjson, ",1:", ",\"1\":", -1)
		bodyjson = strings.Replace(bodyjson, "{0:", "{\"0\":", -1)
		bodyjson = strings.Replace(bodyjson, "{1:", "{\"1\":", -1)
		err = json.Unmarshal([]byte(bodyjson), topicRouteData)
		if err != nil {
			fmt.Fprintln(os.Stderr, "json.Unmarshal", err)
			return nil, err
		}
		return topicRouteData, nil
	} else {
		return nil, errors.New(fmt.Sprintf("get topicRouteInfo from nameServer error[code:%d,topic:%s]", response.Code, topic))
	}

}

func (m *MqClient) updateTopicRouteInfoFromNameServer() {
	topicList := make([]string, 0)
	for _, consumer := range m.consumerTable {
		subscriptions := consumer.subscriptions()
		for _, subData := range subscriptions {
			topicList = append(topicList, subData.Topic)
		}
	}

	for _, producer := range m.producerTable {
		topicList = append(topicList, producer.getPublishTopicList()...)
	}

	for _, topic := range topicList {
		m.updateTopicRouteInfoFromNameServerKernel(topic, false, DefaultProducer{})
	}
}

func (m *MqClient) topicRouteDataIsChange(oldData *TopicRouteData, nowData *TopicRouteData) bool {
	if !topicRouteDataIsNil(oldData) || !topicRouteDataIsNil(nowData) {
		return false
	}
	if !sliceCompare(oldData.QueueDatas, nowData.QueueDatas) {
		return false
	}
	if !sliceCompare(oldData.BrokerDatas, nowData.BrokerDatas) {
		return false
	}
	return true
}

func topicRouteDataIsNil(topicRouteData *TopicRouteData) (isNil bool) {
	if len(topicRouteData.QueueDatas) == 0 && len(topicRouteData.BrokerDatas) == 0 {
		isNil = true
	}
	return
}

func (m *MqClient) isNeedUpdateTopicRouteInfo(topic string) (result bool) {
	for _, producer := range m.producerTable {
		if !result && producer.producerGroup != "" {
			result = producer.isPublishTopicNeedUpdate(topic)
		}
	}

	for _, consumer := range m.consumerTable {
		if !result && consumer.consumerGroup != "" {
			result = consumer.isSubscribeTopicNeedUpdate(topic)
		}
	}
	return result
}

func (m *MqClient) updateTopicRouteInfoFromNameServerKernel(topic string, isDefault bool, producer DefaultProducer) (ok bool, err error) {
	var topicRouteData *TopicRouteData
	if isDefault && producer.producerGroup != "" {
		topicRouteData, err = m.getTopicRouteInfoFromNameServer(producer.createTopicKey, 3000*1000)
		if err != nil {
			fmt.Println(err)
			return true, err
		}
		for _, data := range topicRouteData.QueueDatas {
			queueNums := int32(math.Min(float64(producer.defaultTopicQueueNums), float64(data.ReadQueueNums)))
			data.ReadQueueNums = queueNums
			data.WriteQueueNums = queueNums
		}
	} else {
		topicRouteData, err = m.getTopicRouteInfoFromNameServer(topic, 3000*1000)
	}

	if topicRouteData != nil && !topicRouteDataIsNil(topicRouteData) {
		old := m.topicRouteTable[topic]
		changed := sliceCompare(old, topicRouteData)

		if !changed {
			changed = m.isNeedUpdateTopicRouteInfo(topic)
		} else {
			fmt.Fprintln(os.Stderr, "the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData)
		}

		if changed {
			for _, bd := range topicRouteData.BrokerDatas {
				m.brokerAddrTableLock.Lock()
				m.brokerAddrTable[bd.BrokerName] = bd.BrokerAddrs
				m.brokerAddrTableLock.Unlock()
			}

			// Update Pub info
			publishInfo := topicRouteData2TopicPublishInfo(topic, topicRouteData)
			publishInfo.haveTopicRouterInfo = true
			for _, producer := range m.producerTable {
				producer.updateTopicPublishInfo(topic, publishInfo)
			}

			// Update sub info
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
			for _, consumer := range m.consumerTable {
				consumer.updateTopicSubscribeInfo(topic, mqList)
			}
			m.topicRouteTableLock.Lock()
			m.topicRouteTable[topic] = topicRouteData
			m.topicRouteTableLock.Unlock()

			return true, err
		}
	}

	return false, errors.New("updateTopicRouteInfoFromNameServer wrong")
}

func topicRouteData2TopicPublishInfo(topic string, route *TopicRouteData) (publishInfo *TopicPublishInfo) {
	publishInfo = NewTopicPublishInfo()
	publishInfo.topicRouteData = route
	if route.OrderTopicConf != "" {
		brokers := strings.Split(route.OrderTopicConf, ";")
		for _, borker := range brokers {
			item := strings.Split(borker, ":")
			nums, _ := strconv.Atoi(item[1])
			for i := int32(0); i < int32(nums); i++ {
				mq := NewMessageQueue(topic, item[0], i)
				publishInfo.messageQueueList = append(publishInfo.messageQueueList, mq)
			}
		}
		publishInfo.orderTopic = true
	} else {
		qds := route.QueueDatas
		for _, qd := range qds {
			if PermName.isWritable(qd.Perm) {
				var brokerData *BrokerData
				for _, bd := range route.BrokerDatas {
					if bd.BrokerName == qd.BrokerName {
						brokerData = bd
						break
					}
				}

				if brokerData.BrokerName == "" {
					continue
				}

				if _, ok := brokerData.BrokerAddrs[strconv.Itoa(MasterId)]; !ok {
					continue
				}

				for i := int32(0); i < qd.WriteQueueNums; i++ {
					mq := NewMessageQueue(topic, qd.BrokerName, i)
					publishInfo.messageQueueList = append(publishInfo.messageQueueList, mq)
				}
			}
		}
		publishInfo.orderTopic = false
	}

	return
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

func (m *MqClient) prepareHeartbeatData() *HeartbeatData {
	heartbeatData := new(HeartbeatData)
	heartbeatData.ClientId = m.clientId
	heartbeatData.ConsumerDataSet = make([]*ConsumerData, 0)
	for group, consumer := range m.consumerTable {
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

func (m *MqClient) sendHeartbeatToAllBrokerWithLock() error {
	heartbeatData := m.prepareHeartbeatData()
	if len(heartbeatData.ConsumerDataSet) == 0 {
		return errors.New("send heartbeat error")
	}

	m.brokerAddrTableLock.RLock()
	for _, brokerTable := range m.brokerAddrTable {
		for brokerId, addr := range brokerTable {
			if addr == "" || brokerId != "0" {
				continue
			}
			currOpaque := atomic.AddInt32(&opaque, 1)
			remotingCommand := &RemotingCommand{
				Code:     HeartBeat,
				Language: "JAVA",
				Version:  79,
				Opaque:   currOpaque,
				Flag:     0,
			}

			data, err := json.Marshal(*heartbeatData)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return err
			}
			remotingCommand.Body = data
			fmt.Fprintln(os.Stderr, "send heartbeat to broker[", addr+"]")
			response, err := m.remotingClient.invokeSync(addr, remotingCommand, 3000)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			} else {
				if response == nil || response.Code != Success {
					fmt.Fprintln(os.Stderr, "send heartbeat response  error")
				}
			}
		}
	}
	m.brokerAddrTableLock.RUnlock()
	return nil
}

func (m *MqClient) startScheduledTask() {
	go func() {
		updateTopicRouteTimer := time.NewTimer(5 * time.Second)
		for {
			<-updateTopicRouteTimer.C
			m.updateTopicRouteInfoFromNameServer()
			updateTopicRouteTimer.Reset(5 * time.Second)
		}
	}()

	go func() {
		heartbeatTimer := time.NewTimer(10 * time.Second)
		for {
			<-heartbeatTimer.C
			m.sendHeartbeatToAllBrokerWithLock()
			heartbeatTimer.Reset(5 * time.Second)
		}
	}()

	go func() {
		rebalanceTimer := time.NewTimer(15 * time.Second)
		for {
			<-rebalanceTimer.C
			m.doRebalance()
			rebalanceTimer.Reset(30 * time.Second)
		}
	}()

	go func() {
		timeoutTimer := time.NewTimer(3 * time.Second)
		for {
			<-timeoutTimer.C
			m.remotingClient.ScanResponseTable()
			timeoutTimer.Reset(time.Second)
		}
	}()
}

func (m *MqClient) doRebalance() {
	for _, consumer := range m.consumerTable {
		consumer.doRebalance()
	}
}

func (m *MqClient) start() {
	switch m.serviceState {
	case CreateJust:
		m.serviceState = StartFailed
		m.startScheduledTask()
		go m.pullMessageService.start()
		if m.defaultProducer != nil {
			m.defaultProducer.start(false)
		}
		fmt.Fprintln(os.Stderr, "the client factory [{}] start OK", m.clientId)
		m.serviceState = Running
	case Running, ShutdownAlready, StartFailed:
		fmt.Fprintln(os.Stderr, "The Factory object["+m.clientId+"] has been created before, and failed.")
	}

}

type QueryConsumerOffsetRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueId       int32  `json:"queueId"`
}

func (m *MqClient) queryConsumerOffset(addr string, requestHeader *QueryConsumerOffsetRequestHeader, timeoutMillis int64) (int64, error) {
	currOpaque := atomic.AddInt32(&opaque, 1)
	remotingCommand := &RemotingCommand{
		Code:     QueryConsumerOffset,
		Language: "JAVA",
		Version:  79,
		Opaque:   currOpaque,
		Flag:     0,
	}

	remotingCommand.ExtFields = requestHeader
	reponse, err := m.remotingClient.invokeSync(addr, remotingCommand, timeoutMillis)

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 0, err
	}

	if reponse.Code == QueryNotFound {
		return 0, nil
	}

	if extFields, ok := (reponse.ExtFields).(map[string]interface{}); ok {
		if offsetInter, ok := extFields["offset"]; ok {
			if offsetStr, ok := offsetInter.(string); ok {
				offset, err := strconv.ParseInt(offsetStr, 10, 64)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					return 0, err
				}
				return offset, nil

			}
		}
	}
	fmt.Fprintln(os.Stderr, requestHeader, reponse)
	return 0, errors.New("query offset error")
}

func (m *MqClient) updateConsumerOffsetOneway(addr string, header *UpdateConsumerOffsetRequestHeader, timeoutMillis int64) {

	currOpaque := atomic.AddInt32(&opaque, 1)
	remotingCommand := &RemotingCommand{
		Code:      QueryConsumerOffset,
		Language:  "JAVA",
		Version:   79,
		Opaque:    currOpaque,
		Flag:      0,
		ExtFields: header,
	}

	m.remotingClient.invokeSync(addr, remotingCommand, timeoutMillis)
}

func (m *MqClient) findBrokerAddressInPublish(brokerName string) string {
	tmpMap := m.brokerAddrTable[brokerName]
	if tmpMap != nil && len(tmpMap) != 0 {
		brokerName = tmpMap[strconv.Itoa(MasterId)]
	}
	return brokerName
}

func (m *MqClient) registerProducer(group string, producer *DefaultProducer) bool {
	if group == "" {
		return false
	}
	if _, err := m.producerTable[group]; err {
		fmt.Fprintln(os.Stderr, "the producer group[{}] exist already.", group)
		return false
	} else {
		m.producerTable[group] = producer
		return true
	}
}

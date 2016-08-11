package rocketmq

import (
	"errors"
	"github.com/golang/glog"
	"sort"
	"sync"
)

type SubscriptionData struct {
	Topic           string
	SubString       string
	ClassFilterMode bool
	TagsSet         []string
	CodeSet         []string
	SubVersion      int64
}
type Rebalance struct {
	groupName                    string
	messageModel                 string
	topicSubscribeInfoTable      map[string][]*MessageQueue
	topicSubscribeInfoTableLock sync.RWMutex
	subscriptionInner            map[string]*SubscriptionData
	subscriptionInnerLock sync.RWMutex
	mqClient                     *MqClient
	allocateMessageQueueStrategy AllocateMessageQueueStrategy
	consumer                     *DefaultConsumer
	processQueueTable            map[MessageQueue]int32
	processQueueTableLock sync.RWMutex
	mutex sync.Mutex
}

func NewRebalance() *Rebalance {
	return &Rebalance{
		topicSubscribeInfoTable:      make(map[string][]*MessageQueue),
		subscriptionInner:            make(map[string]*SubscriptionData),
		allocateMessageQueueStrategy: new(AllocateMessageQueueAveragely),
		messageModel:                 "CLUSTERING",
		processQueueTable:            make(map[MessageQueue]int32),
	}
}

func (self *Rebalance) doRebalance() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	for topic, _ := range self.subscriptionInner {
		self.rebalanceByTopic(topic)
	}
}

type ConsumerIdSorter []string

func (self ConsumerIdSorter) Len() int      { return len(self) }
func (self ConsumerIdSorter) Swap(i, j int) { self[i], self[j] = self[j], self[i] }
func (self ConsumerIdSorter) Less(i, j int) bool {
	if self[i] < self[j] {
		return true
	}
	return false
}

type AllocateMessageQueueStrategy interface {
	allocate(consumerGroup string, currentCID string, mqAll []*MessageQueue, cidAll []string) ([]*MessageQueue, error)
}
type AllocateMessageQueueAveragely struct{}

func (self *AllocateMessageQueueAveragely) allocate(consumerGroup string, currentCID string, mqAll []*MessageQueue, cidAll []string) ([]*MessageQueue, error) {
	if currentCID == "" {
		return nil, errors.New("currentCID is empty")
	}

	if mqAll == nil || len(mqAll) == 0 {
		return nil, errors.New("mqAll is nil or mqAll empty")
	}

	if cidAll == nil || len(cidAll) == 0 {
		return nil, errors.New("cidAll is nil or cidAll empty")
	}

	result := make([]*MessageQueue, 0)
	for i, cid := range cidAll {
		if cid == currentCID {
			mqLen := len(mqAll)
			cidLen := len(cidAll)
			mod := mqLen % cidLen
			var averageSize int
			if mqLen < cidLen {
				averageSize = 1
			} else {
				if mod > 0 && i < mod {
					averageSize = mqLen/cidLen + 1
				} else {
					averageSize = mqLen / cidLen
				}
			}

			var startIndex int
			if mod > 0 && i < mod {
				startIndex = i * averageSize
			} else {
				startIndex = i*averageSize + mod
			}

			var min int
			if averageSize > mqLen-startIndex {
				min = mqLen - startIndex
			} else {
				min = averageSize
			}

			for j := 0; j < min; j++ {
				result = append(result, mqAll[(startIndex+j)%mqLen])
			}
			return result, nil

		}
	}

	return nil, errors.New("cant't find currentCID")
}

func (self *Rebalance) rebalanceByTopic(topic string) error {
	cidAll, err := self.mqClient.findConsumerIdList(topic, self.groupName)
	if err != nil {
		glog.Error(err)
		return err
	}

	self.topicSubscribeInfoTableLock.RLock()
	mqs, ok := self.topicSubscribeInfoTable[topic]
	self.topicSubscribeInfoTableLock.RUnlock()
	if ok && len(mqs) > 0 && len(cidAll) > 0 {
		var messageQueues MessageQueues = mqs
		var consumerIdSorter ConsumerIdSorter = cidAll

		sort.Sort(messageQueues)
		sort.Sort(consumerIdSorter)
	}

	allocateResult, err := self.allocateMessageQueueStrategy.allocate(self.groupName, self.mqClient.clientId, mqs, cidAll)

	if err != nil {
		glog.Error(err)
		return err
	}

	glog.V(1).Infof("rebalance topic[%s]", topic)
	self.updateProcessQueueTableInRebalance(topic, allocateResult)
	return nil
}

func (self *Rebalance) updateProcessQueueTableInRebalance(topic string, mqSet []*MessageQueue) {
	for _, mq := range mqSet {
		self.processQueueTableLock.RLock()
		_, ok := self.processQueueTable[*mq]
		self.processQueueTableLock.RUnlock()
		if !ok {
			pullRequest := new(PullRequest)
			pullRequest.consumerGroup = self.groupName
			pullRequest.messageQueue = mq
			pullRequest.nextOffset = self.computePullFromWhere(mq)
			self.mqClient.pullMessageService.pullRequestQueue <- pullRequest
			self.processQueueTableLock.Lock()
			self.processQueueTable[*mq] = 1
			self.processQueueTableLock.Unlock()
		}
	}

}

func (self *Rebalance) computePullFromWhere(mq *MessageQueue) int64 {
	var result int64 = -1
	lastOffset := self.consumer.offsetStore.readOffset(mq, READ_FROM_STORE)

	if lastOffset >= 0 {
		result = lastOffset
	} else {
		result = 0
	}
	return result
}

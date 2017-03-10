package rocketmq

type PullRequest struct {
	consumerGroup string
	messageQueue  *MessageQueue
	nextOffset    int64
}

type PullMessageRequestHeader struct {
	ConsumerGroup        string `json:"consumerGroup"`
	Topic                string `json:"topic"`
	QueueId              int32  `json:"queueId"`
	QueueOffset          int64  `json:"queueOffset"`
	MaxMsgNums           int32  `json:"maxMsgNums"`
	SysFlag              int32  `json:"sysFlag"`
	CommitOffset         int64  `json:"commitOffset"`
	SuspendTimeoutMillis int64  `json:"suspendTimeoutMillis"`
	Subscription         string `json:"subscription"`
	SubVersion           int64  `json:"subVersion"`
}

type Service interface {
	pullMessage(pullRequest *PullRequest)
}

type PullMessageService struct {
	pullRequestQueue chan *PullRequest
	service          Service
}

func NewPullMessageService() *PullMessageService {
	return &PullMessageService{
		pullRequestQueue: make(chan *PullRequest, 1024),
	}
}

func (p *PullMessageService) start() {
	for {
		pullRequest := <-p.pullRequestQueue
		p.service.pullMessage(pullRequest)
	}
}

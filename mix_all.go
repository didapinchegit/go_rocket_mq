package rocketmq

import (
	"strconv"
	"strings"
)

const (
	RocketmqHomeEnv          = "ROCKETMQ_HOME"
	RocketmqHomeProperty     = "rocketmq.home.dir"
	NamesrvAddrEnv           = "NAMESRV_ADDR"
	NamesrvAddrProperty      = "rocketmq.namesrv.addr"
	MessageCompressLevel     = "rocketmq.message.compressLevel"
	WsDomainName             = "192.168.7.101"
	WsDomainSubgroup         = ""
	WsAddr                   = "http://" + WsDomainName + ":8080/rocketmq/" + WsDomainSubgroup
	DefaultTopic             = "TBW102"
	BenchmarkTopic           = "BenchmarkTest"
	DefaultProducerGroup     = "DEFAULT_PRODUCER"
	DefaultConsumerGroup     = "DEFAULT_CONSUMER"
	ToolsConsumerGroup       = "TOOLS_CONSUMER"
	FiltersrvConsumerGroup   = "FILTERSRV_CONSUMER"
	MonitrorConsumerGroup    = "__MONITOR_CONSUMER"
	ClientInnerProducerGroup = "CLIENT_INNER_PRODUCER"
	SelfTestProducerGroup    = "SELF_TEST_P_GROUP"
	SelfTestConsumerGroup    = "SELF_TEST_C_GROUP"
	SelfTestTopic            = "SELF_TEST_TOPIC"
	OffsetMovedEvent         = "OFFSET_MOVED_EVENT"
	OnsHttpProxyGroup        = "CID_ONS-HTTP-PROXY"
	CidOnsapiPermissionGroup = "CID_ONSAPI_PERMISSION"
	CidOnsapiOwnerGroup      = "CID_ONSAPI_OWNER"
	CidOnsapiPullGroup       = "CID_ONSAPI_PULL"
	CidRmqSysPerfix          = "CID_RMQ_SYS_"

	Localhost      = "127.0.0.1"
	DefaultCharset = "UTF-8"
	MasterId       = 0

	RetryGroupTopicPrefix = "%RETRY%"
	DlqGroupTopicPerfix   = "%DLQ%"
	SysTopicPerfix        = "rmq_sys_"
	UniqMsgQueryFlag      = "_UNIQUE_KEY_QUERY"
)

type MixAll struct{}

func BrokerVIPChannel(isChange bool, brokerAddr string) (borkerAddrNew string) {
	borkerAddrNew = brokerAddr
	if isChange {
		ipAndPort := strings.Split(brokerAddr, ":")
		if port, err := strconv.Atoi(ipAndPort[1]); err == nil {
			borkerAddrNew = ipAndPort[0] + ":" + strconv.Itoa(port-2)
		}
	}
	return
}

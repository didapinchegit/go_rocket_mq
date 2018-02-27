package rocketmq

type messageConst struct {
	PropertyKeys                      string
	PropertyTags                      string
	PropertyWaitStoreMsgOk            string
	PropertyDelayTimeLevel            string
	PropertyRetryTopic                string
	PropertyRealTopic                 string
	PropertyRealQueueId               string
	PropertyTransactionPrepared       string
	PropertyProducerGroup             string
	PropertyMinOffset                 string
	PropertyMaxOffset                 string
	PropertyBuyerId                   string
	PropertyOriginMessageId           string
	PropertyTransferFlag              string
	PropertyCorrectionFlag            string
	PropertyMq2Flag                   string
	PropertyReconsumeTime             string
	PropertyMsgRegion                 string
	PropertyUniqClientMessageIdKeyidx string
	PropertyMaxReconsumeTimes         string
	PropertyConsumeStartTimeStamp     string

	KeySeparator string
	systemKeySet []string
}

var MessageConst = &messageConst{
	PropertyKeys:                      "KEYS",
	PropertyTags:                      "TAGS",
	PropertyWaitStoreMsgOk:            "WAIT",
	PropertyDelayTimeLevel:            "DELAY",
	PropertyRetryTopic:                "RETRY_TOPIC",
	PropertyRealTopic:                 "REAL_TOPIC",
	PropertyRealQueueId:               "REAL_QID",
	PropertyTransactionPrepared:       "TRAN_MSG",
	PropertyProducerGroup:             "PGROUP",
	PropertyMinOffset:                 "MIN_OFFSET",
	PropertyMaxOffset:                 "MAX_OFFSET",
	PropertyBuyerId:                   "BUYER_ID",
	PropertyOriginMessageId:           "ORIGIN_MESSAGE_ID",
	PropertyTransferFlag:              "TRANSFER_FLAG",
	PropertyCorrectionFlag:            "CORRECTION_FLAG",
	PropertyMq2Flag:                   "MQ2_FLAG",
	PropertyReconsumeTime:             "RECONSUME_TIME",
	PropertyMsgRegion:                 "MSG_REGION",
	PropertyUniqClientMessageIdKeyidx: "UNIQ_KEY",
	PropertyMaxReconsumeTimes:         "MAX_RECONSUME_TIMES",
	PropertyConsumeStartTimeStamp:     "CONSUME_START_TIME",

	KeySeparator: "",
}

func init() {
	var systemKeySet = make([]string, 0)
	systemKeySet = append(systemKeySet, MessageConst.PropertyKeys)
	systemKeySet = append(systemKeySet, MessageConst.PropertyTags)
	systemKeySet = append(systemKeySet, MessageConst.PropertyWaitStoreMsgOk)
	systemKeySet = append(systemKeySet, MessageConst.PropertyDelayTimeLevel)
	systemKeySet = append(systemKeySet, MessageConst.PropertyRetryTopic)
	systemKeySet = append(systemKeySet, MessageConst.PropertyRealTopic)
	systemKeySet = append(systemKeySet, MessageConst.PropertyRealQueueId)
	systemKeySet = append(systemKeySet, MessageConst.PropertyTransactionPrepared)
	systemKeySet = append(systemKeySet, MessageConst.PropertyProducerGroup)
	systemKeySet = append(systemKeySet, MessageConst.PropertyMinOffset)
	systemKeySet = append(systemKeySet, MessageConst.PropertyMaxOffset)
	systemKeySet = append(systemKeySet, MessageConst.PropertyBuyerId)
	systemKeySet = append(systemKeySet, MessageConst.PropertyOriginMessageId)
	systemKeySet = append(systemKeySet, MessageConst.PropertyTransferFlag)
	systemKeySet = append(systemKeySet, MessageConst.PropertyCorrectionFlag)
	systemKeySet = append(systemKeySet, MessageConst.PropertyMq2Flag)
	systemKeySet = append(systemKeySet, MessageConst.PropertyReconsumeTime)
	systemKeySet = append(systemKeySet, MessageConst.PropertyMsgRegion)
	systemKeySet = append(systemKeySet, MessageConst.PropertyUniqClientMessageIdKeyidx)
	systemKeySet = append(systemKeySet, MessageConst.PropertyMaxReconsumeTimes)
	systemKeySet = append(systemKeySet, MessageConst.PropertyConsumeStartTimeStamp)

	MessageConst.systemKeySet = systemKeySet
}

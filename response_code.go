package rocketmq

const (
	// 成功
	Success = 0
	// 发生了未捕获异常
	SysError = 1
	// 由于线程池拥堵，系统繁忙
	SysBusy = 2
	// 请求代码不支持
	RequestCodeNotSupported = 3
	//事务失败，添加db失败
	TransactionFailed = 4
	// Broker 刷盘超时
	FlushDiskTimeout = 10
	// Broker 同步双写，Slave不可用
	SlaveNotAvailable = 11
	// Broker 同步双写，等待Slave应答超时
	FlushSlaveTimeout = 12
	// Broker 消息非法
	MsgIllegal = 13
	// Broker, Namesrv 服务不可用，可能是正在关闭或者权限问题
	ServiceNotAvailable = 14
	// Broker, Namesrv 版本号不支持
	VersionNotSupported = 15
	// Broker, Namesrv 无权限执行此操作，可能是发、收、或者其他操作
	NoPermission = 16
	// Broker, Topic不存在
	TopicNotExist = 17
	// Broker, Topic已经存在，创建Topic
	TopicExistAlready = 18
	// Broker 拉消息未找到（请求的Offset等于最大Offset，最大Offset无对应消息）
	PullNotFound = 19
	// Broker 可能被过滤，或者误通知等
	PullRetryImmediately = 20
	// Broker 拉消息请求的Offset不合法，太小或太大
	PullOffsetMoved = 21
	// Broker 查询消息未找到
	QueryNotFound = 22
	// Broker 订阅关系解析失败
	SubscriptionParseFailed = 23
	// Broker 订阅关系不存在
	SubscriptionNotExist = 24
	// Broker 订阅关系不是最新的
	SubscriptionNotLatest = 25
	// Broker 订阅组不存在
	SubscriptionGroupNotExist = 26
	// Producer 事务应该被提交
	TransactionShouldCommit = 200
	// Producer 事务应该被回滚
	TransactionShouldRollback = 201
	// Producer 事务状态未知
	TransactionStateUnknow = 202
	// Producer ProducerGroup错误
	TransactionStateGroupWrong = 203
	// 单元化消息，需要设置 buyerId
	NoBuyerId = 204
	// 单元化消息，非本单元消息
	NotInCurrentUint = 205
	// Consumer不在线
	ConsumerNotOnline = 206
	// Consumer消费消息超时
	ConsumeMsgTimeout = 207
)

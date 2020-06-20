package zscheduler

// Scheduler is a standard interface for zeus cammand
type Scheduler interface {
	Schedule(registerName string, serviceName string) error
	Listen(string) error
	GetIdles() []interface{}
	GetIdle() interface{}
}

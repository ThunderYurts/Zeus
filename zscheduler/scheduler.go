package zscheduler

// Scheduler is a standard interface for zeus cammand
type Scheduler interface {
	Schedule(string) error
	Listen(string) error
	GetIdles() []interface{}
	GetIdle() interface{}
}

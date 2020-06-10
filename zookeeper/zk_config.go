package zookeeper

//ZKServiceHost is a single key value pair for local ServiceHost
type ZKServiceHost struct {
	Service   string
	SyncHost  string
	Primary   string
	Secondary []string
	SlotBegin uint32
	SlotEnd   uint32
}

// ZKRegister is a register response to yurt temporary node
type ZKRegister struct {
	ServiceName string
}

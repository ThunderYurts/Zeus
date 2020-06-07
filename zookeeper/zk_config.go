package zookeeper

//ZKServiceHost is a single key value pair for local ServiceHost
type ZKServiceHost struct {
	Key   string
	Value []string
}

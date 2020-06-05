package zroute

import "sync"

// ServiceHost is a standard container that storages service addrs synced from zookeeper
type ServiceHost struct {
	lock  sync.RWMutex
	hosts map[string][]string // key is service name, value is addrs
}

func NewServiceHost() ServiceHost {
	return ServiceHost{
		lock:  sync.RWMutex{},
		hosts: make(map[string][]string),
	}
}

// Algo is supported for diff algo about load balance
type Algo interface {
	Source(Key string, hosts *ServiceHost) (string, error)
}

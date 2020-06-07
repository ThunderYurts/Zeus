package zroute

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/ThunderYurts/Zeus/zookeeper"
)

// ServiceHost is a standard container that storages service addrs synced from zookeeper
type ServiceHost struct {
	Lock  *sync.RWMutex
	Hosts map[string][]string // key is service name, value is addrs
}

// NewServiceHost is a help function new ServiceHost
func NewServiceHost() ServiceHost {
	return ServiceHost{
		Lock:  &sync.RWMutex{},
		Hosts: make(map[string][]string),
	}
}

// Sync will sync ServiceHost from channel
func (sh *ServiceHost) Sync(channel <-chan []byte) {
	var newHosts zookeeper.ZKServiceHost
	for {
		select {
		case data, ok := <-channel:
			{
				if !ok {
					return
				}
				newHosts = zookeeper.ZKServiceHost{}
				dec := gob.NewDecoder(bytes.NewBuffer(data))
				if err := dec.Decode(&newHosts); err != nil {
					fmt.Printf("err in algo decode :%v", err)
				} else {
					fmt.Printf("sync new hosts %s:%v\n", newHosts.Key, newHosts)
					// sync into the sh
					sh.Lock.Lock()
					sh.Hosts[newHosts.Key] = newHosts.Value
					sh.Lock.Unlock()
				}
			}
		}
	}
}

// Algo is supported for diff algo about load balance
type Algo interface {
	Source(Key string, hosts *ServiceHost) (string, error)
}

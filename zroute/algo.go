package zroute

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/ThunderYurts/Zeus/zconst"
	"github.com/ThunderYurts/Zeus/zookeeper"
	"github.com/samuel/go-zookeeper/zk"
)

// Segment is present slot segment
type Segment struct {
	Begin       uint32
	End         uint32
	Block       bool
	ServiceName string
}

// ServiceHost is a standard container that storages service addrs synced from zookeeper
type ServiceHost struct {
	ctx          context.Context
	wg           *sync.WaitGroup
	Lock         *sync.RWMutex
	PrimaryHosts map[string]string
	Hosts        map[string][]string // key is service name, value is addrs
	watchers     map[string]*zookeeper.ConfigWatcher
	channel      chan []byte
	segChannel   chan Segment
	conn         *zk.Conn
}

// NewServiceHost is a help function new ServiceHost
func NewServiceHost(ctx context.Context, wg *sync.WaitGroup, conn *zk.Conn, segChannel chan Segment) ServiceHost {
	return ServiceHost{
		ctx:          ctx,
		wg:           wg,
		Lock:         &sync.RWMutex{},
		Hosts:        make(map[string][]string),
		PrimaryHosts: make(map[string]string),
		channel:      make(chan []byte, 100),
		segChannel:   segChannel,
		watchers:     make(map[string]*zookeeper.ConfigWatcher),
		conn:         conn,
	}
}

// ServiceSync watch service and add configwatcher
func (sh *ServiceHost) ServiceSync() {
	children, _, channel, err := sh.conn.ChildrenW(zconst.ServiceRoot)
	if err != nil {
		panic(err)
	}
	fmt.Printf("in servicesync %v\n", children)
	for _, srv := range children {
		if _, exist := sh.watchers[srv]; exist {
			continue
		}
		watcher := zookeeper.NewConfigWatcher(sh.ctx, sh.wg, sh.channel, sh.conn, zconst.ServiceRoot+"/"+srv)
		watcher.Start()
		sh.watchers[srv] = &watcher
		// Split slots
	}
	go func() {
		for {
			select {
			case e, ok := <-channel:
				{
					if !ok {
						return
					}
					fmt.Printf("in algo service sync %v\n", e)
					if e.Type == zk.EventNodeChildrenChanged {
						children, _, channel, err = sh.conn.ChildrenW(zconst.ServiceRoot)
						if err != nil {
							fmt.Printf("%v", err)
							panic(err)
						}
						for _, srv := range children {
							if _, exist := sh.watchers[srv]; exist {
								continue
							}
							watcher := zookeeper.NewConfigWatcher(sh.ctx, sh.wg, sh.channel, sh.conn, zconst.ServiceRoot+"/"+srv)
							watcher.Start()
							sh.watchers[srv] = &watcher
						}
					}
				}
			case <-sh.ctx.Done():
				{
					return
				}
			}
		}
	}()
}

// ItemSync will sync the specific service config
func (sh *ServiceHost) ItemSync() {
	go func() {
		var newHosts zookeeper.ZKServiceHost
		for {
			select {
			case data, ok := <-sh.channel:
				{
					if !ok {
						return
					}
					newHosts = zookeeper.ZKServiceHost{}
					dec := gob.NewDecoder(bytes.NewBuffer(data))
					if err := dec.Decode(&newHosts); err != nil {
						fmt.Printf("err in algo decode :%v", err)
					} else {
						// sync into the sh
						// TODO handle slots
						sh.Lock.Lock()
						if newHosts.SyncHost == "" {
							// Notice that the channel has no buffer
							sh.segChannel <- Segment{Begin: newHosts.SlotBegin, End: newHosts.SlotEnd, Block: false, ServiceName: newHosts.Service}
						}
						fmt.Printf("secondary: %v\n", newHosts.Secondary)
						sh.Hosts[newHosts.Service] = append(newHosts.Secondary, newHosts.Primary)
						sh.PrimaryHosts[newHosts.Service] = newHosts.Primary
						fmt.Printf("sync new hosts :%v\n", newHosts)
						fmt.Printf("update hosts %v\n", sh.Hosts)
						sh.Lock.Unlock()
					}
				}
			}
		}
	}()
}

// Sync will sync ServiceHost from channel
func (sh *ServiceHost) Sync() {
	// sync all service from zk
	sh.ServiceSync()
	sh.ItemSync()
}

// Algo is supported for diff algo about load balance
type Algo interface {
	Source(service string, action string, hosts *ServiceHost) (string, error)
}

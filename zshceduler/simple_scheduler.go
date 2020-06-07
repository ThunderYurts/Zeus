package zscheduler

import (
	"context"
	"fmt"
	"sync"

	"github.com/ThunderYurts/Zeus/zroute"
	"github.com/samuel/go-zookeeper/zk"
)

// SimpleScheduler is a simple algo for schedule idle yurt
type SimpleScheduler struct {
	ctx         context.Context
	wg          *sync.WaitGroup
	conn        *zk.Conn
	minRequest  int
	ServiceHost *zroute.ServiceHost
}

// NewSimpleScheduler is a help function for new SimpleScheduler
func NewSimpleScheduler(ctx context.Context, wg *sync.WaitGroup, conn *zk.Conn, minRequest int, ServiceHost *zroute.ServiceHost) (SimpleScheduler, error) {
	return SimpleScheduler{
		ctx:         ctx,
		wg:          wg,
		conn:        conn,
		minRequest:  minRequest,
		ServiceHost: ServiceHost,
	}, nil
}

// Schedule will give a
func (ss *SimpleScheduler) Schedule(registerNode string) error {
	fmt.Printf("schedule register node \n")
	ss.ServiceHost.Lock.RLock()
	if len(ss.ServiceHost.Hosts) == 0 {
		fmt.Println("hosts zero")
		return nil
	}
	statistic := make(map[string]int)
	for key, value := range ss.ServiceHost.Hosts {
		statistic[key] = len(value)
	}
	ss.ServiceHost.Lock.RUnlock()
	fmt.Printf("statistic value %v\n", statistic)
	for key, value := range statistic {
		if value < ss.minRequest {
			// scheduler to a service for secondary (maybe)
			_, stat, err := ss.conn.Get(registerNode)
			if err != nil {
				fmt.Printf("in scheduler loop1 error : %v", err)
				return err
			}
			stat, err = ss.conn.Set(registerNode, []byte(key), stat.Version)
			if err != nil {
				fmt.Printf("in scheduler loop2 error : %v", err)
				return err
			}
			fmt.Printf("node %s is dispatched to %s\n", registerNode, key)
			return nil
		}
	}
	fmt.Printf("node %s is still idle and waiting for dispatch\n", registerNode)
	return nil
}

// Listen will start a routine to ChildrenW
func (ss *SimpleScheduler) Listen(path string) error {
	idles, _, childChan, err := ss.conn.ChildrenW(path)
	fmt.Printf("listen %s start\n", path)
	fmt.Printf("idles %v\n", idles)
	if err != nil {
		fmt.Printf("in scheduler listen1 error : %v", err)
		return err
	}
	for _, idle := range idles {
		ss.Schedule(path + "/" + idle)
	}
	ss.wg.Add(1)
	defer ss.wg.Done()
	for {
		select {
		case <-ss.ctx.Done():
			{
				return err
			}
		case e := <-childChan:
			{
				fmt.Printf("simple_scheduler event: %v\n", e)
				if e.Type != zk.EventNodeDeleted {
					idles, _, childChan, err = ss.conn.ChildrenW(path)
					if err != nil {
						fmt.Printf("in scheduler listen2 error : %v", err)
					}
					for _, idle := range idles {
						ss.Schedule(path + "/" + idle)
					}
				}
			}
		}
	}

}

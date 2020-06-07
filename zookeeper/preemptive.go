package zookeeper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// Preemptive is a common
type Preemptive struct {
	ctx            context.Context
	wg             *sync.WaitGroup
	conn           *zk.Conn
	preemptiveName string
}

// NewPreemptive is a help function to new Preemptive
func NewPreemptive(ctx context.Context, wg *sync.WaitGroup, conn *zk.Conn, PreemptiveName string) Preemptive {
	return Preemptive{
		ctx:            ctx,
		preemptiveName: PreemptiveName,
		wg:             wg,
		conn:           conn,
	}

}

// Preemptive will try to preemptive a temporary node,
// if we do not get the node, we will retry in a forever loop
// if the primary is down, we will quickly hold the temporary node
// and then we will become the primary
func (p *Preemptive) Preemptive(data []byte) error {
	nodeName := string(data)
	for {
		acl := zk.WorldACL(zk.PermAll)
		_, err := p.conn.Create(p.preemptiveName, data, zk.FlagEphemeral, acl)
		if err == nil {
			fmt.Printf("%s will hold master\n", nodeName)
			return nil
		}
		time.Sleep(1 * time.Second)
		fmt.Println(err.Error())
		// select {
		// case e := <-ch:
		// 	{
		// 		fmt.Printf("%s receive event: %v\n", nodeName, e)
		// 		if e.Type == zk.EventNodeDeleted {
		// 			acl := zk.WorldACL(zk.PermAll)
		// 			_, err := p.conn.Create(p.preemptiveName, data, zk.FlagEphemeral, acl)
		// 			if err == nil {
		// 				fmt.Println("a node will hold master")
		// 				return nil
		// 			}
		// 			if err == zk.ErrNodeExists {
		// 				continue
		// 			}
		// 		}
		// 	}
		// }
	}

}

// GetSessionID will return conn.SeSessionId
func (p *Preemptive) GetSessionID() int64 {
	return p.conn.SessionID()
}

// Close will disconnect (just a debug function)
func (p *Preemptive) Close() {
	p.conn.Close()
}

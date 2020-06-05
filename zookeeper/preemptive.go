package zookeeper

import (
	"sync"

	"github.com/samuel/go-zookeeper/zk"
)

// Preemptive is a common
type Preemptive struct {
	preemptiveName string
	wg             *sync.WaitGroup
	conn           *zk.Conn
}

// NewPreemptive is a help function to new Preemptive
func NewPreemptive(PreemptiveName string, wg *sync.WaitGroup, conn *zk.Conn) Preemptive {
	return Preemptive{
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
	for {
		acl := zk.WorldACL(zk.PermAll)
		_, err := p.conn.Create(p.preemptiveName, data, zk.FlagEphemeral, acl)
		if err == nil {
			return nil
		}
	}
}

// GetSessionID will return conn.SeSessionId
func (p *Preemptive) GetSessionID() int64 {
	return p.conn.SessionID()
}

// Close will disconnect
func (p *Preemptive) Close() {
	p.conn.Close()
}

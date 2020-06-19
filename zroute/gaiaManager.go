package zroute

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/ThunderYurts/Zeus/gserver"
	"github.com/ThunderYurts/Zeus/zconst"
	"github.com/ThunderYurts/Zeus/zookeeper"
	"github.com/samuel/go-zookeeper/zk"
	"google.golang.org/grpc"
	"math/rand"
	"reflect"
	"sync"
	"time"
)

var NOT_ENOUGH_GAIA = errors.New("not enough gaia")

type GaiaManager struct {
	mu       sync.Mutex
	ctx      context.Context
	conn     *zk.Conn
	Monitors map[string]zookeeper.ZKNode
}

// NewGaiaManager is a help function
func NewGaiaManager(ctx context.Context, conn *zk.Conn) GaiaManager {
	exist, _, err := conn.Exists(zconst.GaiaRoot)
	if err != nil {
		panic(err)
	}
	if !exist {
		_, err = conn.Create(zconst.GaiaRoot, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			panic(err)
		}
	}
	return GaiaManager{
		mu:       sync.Mutex{},
		ctx:      ctx,
		conn:     conn,
		Monitors: make(map[string]zookeeper.ZKNode),
	}
}

func (co *GaiaManager) Collect() {
	go func() {
		for {
			select {
			case <-co.ctx.Done():
				{
					return
				}
			default:
				{
					// avoid add ChildrenW
					mon := make(map[string]zookeeper.ZKNode)
					children, _, err := co.conn.Children(zconst.GaiaRoot)
					if err != nil {
						panic(err)
					}
					for _, child := range children {
						data, _, err := co.conn.Get(zconst.GaiaRoot + "/" + child)
						if err != nil {
							fmt.Printf("err in GaiaManager %v\n", err)
							mon[child] = zookeeper.ZKNode{CPU: -1, Memory: -1}
							continue
						}
						node := zookeeper.ZKNode{}
						if len(data) > 0 {
							dec := gob.NewDecoder(bytes.NewBuffer(data))

							err = dec.Decode(&node)
						}
						mon[child] = node
					}
					co.mu.Lock()
					co.Monitors = mon
					co.mu.Unlock()
					time.Sleep(2 * time.Second)
				}
			}
		}
	}()
}

func (co *GaiaManager) Create(addr string, serviceName string) error {
	if addr == "" {
		// random a gaia policy
		keys := reflect.ValueOf(co.Monitors).MapKeys()
		if len(keys) == 0 {
			return NOT_ENOUGH_GAIA
		}
		key := keys[rand.Intn(len(keys))].Interface()
		node := co.Monitors[key.(string)]
		addr = node.CreateAddr
	}
	createConn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	cli := gserver.NewBreedClient(createConn)
	reply, err := cli.Create(co.ctx, &gserver.CreateRequest{ServiceName: serviceName})
	if err != nil {
		return err
	}
	if reply.Code != gserver.CreateCode_CREATE_SUCCESS {
		return errors.New("create reply code error")
	}
	return nil

}

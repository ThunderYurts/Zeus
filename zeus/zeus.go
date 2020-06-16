package zeus

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ThunderYurts/Zeus/zconst"
	"github.com/ThunderYurts/Zeus/zookeeper"
	"github.com/ThunderYurts/Zeus/zroute"
	"github.com/ThunderYurts/Zeus/zsource"
	"github.com/samuel/go-zookeeper/zk"
)

// Config of Zeus contains Source Server config
type Config struct {
	SourceConfig *zsource.ServerConfig
	Name         string
	slotBegin    uint32
	slotEnd      uint32
}

// Zeus is control pannel of ThunderYurt
type Zeus struct {
	ctx           context.Context
	fianalizeFunc context.CancelFunc
	sourceServer  zsource.Server
	wg            *sync.WaitGroup
	config        *Config
	conn          *zk.Conn
	preemptive    *zookeeper.Preemptive
}

// NewZeus is a help function
func NewZeus(ctx context.Context, fianalizeFunc context.CancelFunc, name string, slotBegin uint32, slotEnd uint32) Zeus {
	// TODO choose algo
	algo := zroute.NewRoundRobin()

	sourceConfig := zsource.NewServerConfig()
	config := Config{
		SourceConfig: &sourceConfig,
		Name:         name,
		slotBegin:    slotBegin,
		slotEnd:      slotEnd,
	}
	wg := &sync.WaitGroup{}
	return Zeus{
		ctx:           ctx,
		fianalizeFunc: fianalizeFunc,
		sourceServer:  zsource.NewServer(ctx, &algo, &sourceConfig, nil),
		wg:            wg,
		config:        &config,
		conn:          nil,
		preemptive:    nil,
	}
}
func existOrCreate(conn *zk.Conn, path string, data []byte) error {
	exist, _, err := conn.Exists(path)
	if err != nil {
		return err
	}
	if !exist {
		_, err = conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}
	return nil
}

func (z *Zeus) initZk(zkAddr []string) error {
	conn, _, err := zk.Connect(zkAddr, 1*time.Second)
	z.conn = conn
	if err != nil {
		return err
	}

	err = existOrCreate(z.conn, zconst.ZeusRoot, []byte{})
	if err != nil {
		return err
	}
	segment := strconv.Itoa(int(z.config.slotBegin)) + "-" + strconv.Itoa(int(z.config.slotEnd))
	preemptiveName := zconst.ZeusRoot + "/" + segment
	preemptive := zookeeper.NewPreemptive(z.ctx, z.wg, z.conn, preemptiveName)
	z.preemptive = &preemptive
	// Zeus will block here forever
	err = z.preemptive.Preemptive([]byte(z.config.Name))
	fmt.Printf("%s is master now !", z.config.Name)
	if err != nil {
		fmt.Printf("in zeus error :%s", err.Error())
		return err
	}

	err = existOrCreate(z.conn, zconst.YurtRoot, []byte{})
	if err != nil {
		return err
	}

	err = existOrCreate(z.conn, zconst.ServiceRoot, []byte{})
	if err != nil {
		return err
	}

	// TODO exist is a very strange state
	// Init segement ServiceHost
	// initServiceHost := zookeeper.ZKServiceHost{Service: segment, Primary: "", Secondary: []string{}}

	// buf := new(bytes.Buffer)
	// enc := gob.NewEncoder(buf)
	// err = enc.Encode(initServiceHost)
	// if err != nil {
	// 	return err
	// }
	//TODO sync first or init default (move to source)

	return nil

}

// Start Zues
func (z *Zeus) Start(sourcePort string, zkAddr []string) error {
	// TODO init connection with zk and if not exist create some folder
	err := z.initZk(zkAddr)
	if err != nil {
		return err
	}
	err = z.sourceServer.Start(sourcePort, z.conn, z.config.slotBegin, z.config.slotEnd, z.wg)
	if err != nil {
		return err
	}
	return nil

}

// Stop Zues
func (z *Zeus) Stop() {
	z.fianalizeFunc()
	z.wg.Wait()
	z.conn.Close()
}

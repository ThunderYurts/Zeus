package zscheduler

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"
	"testing"
	"time"

	"github.com/ThunderYurts/Zeus/zconst"
	"github.com/ThunderYurts/Zeus/zookeeper"
	"github.com/ThunderYurts/Zeus/zroute"
	"github.com/ThunderYurts/Zeus/zslot"
	"github.com/samuel/go-zookeeper/zk"
	. "github.com/smartystreets/goconvey/convey"
)

func deleteNode(path string, conn *zk.Conn) {
	exist, stat, _ := conn.Exists(path)
	if exist {
		// find child first
		children, _, _ := conn.Children(path)
		for _, child := range children {
			deleteNode(path+"/"+child, conn)
		}
		_ = conn.Delete(path, stat.Version)
	}
}

func testScheduler(t *testing.T) {
	Convey("test simple scheduler", t, func() {
		zkAddr := []string{"localhost:2181"}
		conn, _, err := zk.Connect(zkAddr, 1*time.Second)
		var s Scheduler
		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		segChannel := make(chan zroute.Segment)
		ServiceHost := zroute.NewServiceHost(ctx, wg, conn, segChannel)
		ServiceHost.Lock.Lock()
		ServiceHost.Hosts["service1"] = make([]string, 1)
		ServiceHost.Hosts["service1"][0] = "127.0.0.1:9999"
		ServiceHost.Lock.Unlock()
		_, err = conn.Create("/yurt/idle0", []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		So(err, ShouldBeNil)
		sc := zslot.NewSlotCluster(conn)
		ss, err := NewSimpleScheduler(ctx, wg, conn, 2, &ServiceHost, &sc, segChannel)
		s = &ss
		So(err, ShouldBeNil)
		go func() {
			s.Listen(zconst.YurtRoot)
		}()
		So(err, ShouldBeNil)
		_, err = conn.Create("/yurt/idle1", []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		So(err, ShouldBeNil)
		time.Sleep(1 * time.Second)
		data0, _, err := conn.Get("/yurt/idle0")
		dec := gob.NewDecoder(bytes.NewBuffer(data0))
		reg0 := zookeeper.ZKRegister{}
		err = dec.Decode(&reg0)
		So(err, ShouldBeNil)
		So(reg0.ServiceName, ShouldEqual, "service1")
		data1, _, err := conn.Get("/yurt/idle1")
		dec = gob.NewDecoder(bytes.NewBuffer(data1))
		reg1 := zookeeper.ZKRegister{}
		err = dec.Decode(&reg1)
		So(err, ShouldBeNil)
		So(reg1.ServiceName, ShouldEqual, "service1")
		cancel()
		wg.Wait()
	})
}

func testSchedulerAndWatcher(t *testing.T) {
	Convey("test scheduler and config watcher", t, func() {
		testAddr := "localhost:8888"
		zkAddr := []string{"localhost:2181"}
		writer, _, err := zk.Connect(zkAddr, 3*time.Second)
		So(err, ShouldBeNil)
		_, err = writer.Create(zconst.ServiceRoot, []byte{}, 0, zk.WorldACL(zk.PermAll))
		So(err, ShouldBeNil)
		newHosts := zookeeper.ZKServiceHost{}
		newHosts.Secondary = []string{}
		newHosts.Primary = ""
		newHosts.Service = "mockservice"
		newHosts.SlotBegin = 0
		newHosts.SlotEnd = 11384
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)
		err = enc.Encode(newHosts)
		So(err, ShouldBeNil)
		_, err = writer.Create(zconst.ServiceRoot+"/mockservice", buf.Bytes(), 0, zk.WorldACL(zk.PermAll))
		So(err, ShouldBeNil)
		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		conn, _, err := zk.Connect(zkAddr, 3*time.Second)
		So(err, ShouldBeNil)
		segChannel := make(chan zroute.Segment)
		sh := zroute.NewServiceHost(ctx, wg, conn, segChannel)
		sh.Sync()
		var s Scheduler
		sc := zslot.NewSlotCluster(conn)
		ss, err := NewSimpleScheduler(ctx, wg, conn, 2, &sh, &sc, segChannel)
		s = &ss
		go func() {
			s.Listen(zconst.YurtRoot)
		}()

		time.Sleep(2 * time.Second)

		_, err = writer.Create("/yurt/idle00", []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		So(err, ShouldBeNil)

		time.Sleep(time.Second)

		data0, _, err := writer.Get("/yurt/idle00")
		dec := gob.NewDecoder(bytes.NewBuffer(data0))
		reg0 := zookeeper.ZKRegister{}
		err = dec.Decode(&reg0)
		So(err, ShouldBeNil)
		So(reg0.ServiceName, ShouldEqual, "mockservice")

		time.Sleep(time.Second)

		initSh := zookeeper.ZKServiceHost{}
		initSh.Service = "mockservice"
		initSh.SyncHost = ""
		initSh.Primary = testAddr
		initSh.Secondary = []string{}
		initSh.SlotBegin = 0
		initSh.SlotEnd = 11384
		buf = new(bytes.Buffer)
		enc = gob.NewEncoder(buf)
		err = enc.Encode(initSh)
		So(err, ShouldBeNil)

		_, stat, err := writer.Get(zconst.ServiceRoot + "/mockservice")
		So(err, ShouldBeNil)

		stat, err = writer.Set(zconst.ServiceRoot+"/mockservice", buf.Bytes(), stat.Version)
		So(err, ShouldBeNil)

		time.Sleep(1 * time.Second)
		_, err = writer.Create("/yurt/idle01", []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		So(err, ShouldBeNil)

		time.Sleep(3 * time.Second)

		data1, _, err := writer.Get("/yurt/idle01")
		So(err, ShouldBeNil)
		dec = gob.NewDecoder(bytes.NewBuffer(data1))
		reg1 := zookeeper.ZKRegister{}
		err = dec.Decode(&reg1)
		So(err, ShouldBeNil)
		So(reg1.ServiceName, ShouldEqual, "mockservice")

		err = conn.Delete(zconst.ServiceRoot+"/mockservice", stat.Version)
		So(err, ShouldBeNil)

		cancel()
		wg.Wait()
		writer.Close()
		conn.Close()
	})
}

func testCreateNewService(t *testing.T) {
	setup()
	Convey("test create new service", t, func() {
		zkAddr := []string{"localhost:2181"}
		writer, _, err := zk.Connect(zkAddr, 3*time.Second)
		So(err, ShouldBeNil)
		_, err = writer.Create(zconst.ServiceRoot, []byte{}, 0, zk.WorldACL(zk.PermAll))
		So(err, ShouldBeNil)
		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		conn, _, err := zk.Connect(zkAddr, 3*time.Second)
		So(err, ShouldBeNil)
		segChannel := make(chan zroute.Segment)
		sh := zroute.NewServiceHost(ctx, wg, conn, segChannel)
		sh.Sync()
		var s Scheduler
		sc := zslot.NewSlotCluster(conn)
		ss, err := NewSimpleScheduler(ctx, wg, conn, 1, &sh, &sc, segChannel)
		s = &ss
		go func() {
			s.Listen(zconst.YurtRoot)
		}()

		time.Sleep(2 * time.Second)

		_, err = writer.Create("/yurt/idle00", []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		So(err, ShouldBeNil)

		time.Sleep(2 * time.Second)

		So(ss.ServiceHost.Hosts, ShouldHaveLength, 1)

		onlyServices := []string{}
		for key := range ss.ServiceHost.Hosts {
			onlyServices = append(onlyServices, key)
		}

		data0, _, err := writer.Get("/yurt/idle00")
		dec := gob.NewDecoder(bytes.NewBuffer(data0))
		reg0 := zookeeper.ZKRegister{}
		err = dec.Decode(&reg0)
		So(err, ShouldBeNil)
		So(err, ShouldBeNil)
		So(reg0.ServiceName, ShouldEqual, onlyServices[0])

		cancel()
		wg.Wait()
		writer.Close()
		conn.Close()
	})
	teardown()
}

func setup() {
	zkAddr := []string{"localhost:2181"}
	conn, _, _ := zk.Connect(zkAddr, 1*time.Second)
	conn.Create(zconst.YurtRoot, []byte{}, 0, zk.WorldACL(zk.PermAll))
	conn.Close()
}

func teardown() {
	zkAddr := []string{"localhost:2181"}
	conn, _, _ := zk.Connect(zkAddr, 1*time.Second)
	_, stat, _ := conn.Get(zconst.YurtRoot)
	_ = conn.Delete(zconst.YurtRoot, stat.Version)
	_, stat, _ = conn.Get(zconst.ServiceRoot)
	deleteNode(zconst.ServiceRoot, conn)
	conn.Close()
}

func TestEverything(t *testing.T) {
	// Maintaining this map is error-prone and cumbersome (note the subtle bug):
	fs := map[string]func(*testing.T){
		// "testScheduler": testScheduler,
		"testSchedulerAndWatcher": testSchedulerAndWatcher,
	}
	// You may be able to use the `importer` package to enumerate tests instead,
	// but that starts getting complicated.
	for name, f := range fs {
		setup()
		t.Run(name, f)
		teardown()
	}
}

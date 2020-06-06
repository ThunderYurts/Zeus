package scheduler

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"
	"testing"
	"time"

	"github.com/ThunderYurts/Zeus/zookeeper"
	"github.com/ThunderYurts/Zeus/zroute"
	"github.com/samuel/go-zookeeper/zk"
	. "github.com/smartystreets/goconvey/convey"
)

func testScheduler(t *testing.T) {
	Convey("test simple scheduler", t, func() {
		zkAddr := []string{"localhost:2181"}
		conn, _, err := zk.Connect(zkAddr, 5*time.Second)
		var s Scheduler
		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		ServiceHost := zroute.NewServiceHost()
		ServiceHost.Lock.Lock()
		ServiceHost.Hosts["service1"] = make([]string, 1)
		ServiceHost.Hosts["service1"][0] = "127.0.0.1:9999"
		ServiceHost.Lock.Unlock()
		_, err = conn.Create("/yurt/idle0", []byte("idle0"), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		So(err, ShouldBeNil)
		ss, err := NewSimpleScheduler(ctx, wg, zkAddr, 2, &ServiceHost)
		s = &ss
		So(err, ShouldBeNil)
		go func() {
			s.Listen("/yurt")
		}()
		So(err, ShouldBeNil)
		_, err = conn.Create("/yurt/idle1", []byte("idle1"), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		So(err, ShouldBeNil)
		time.Sleep(1 * time.Second)
		data0, _, err := conn.Get("/yurt/idle0")
		So(err, ShouldBeNil)
		So(string(data0), ShouldEqual, "service1")
		data1, _, err := conn.Get("/yurt/idle1")
		So(err, ShouldBeNil)
		So(string(data1), ShouldEqual, "service1")
		cancel()
		wg.Wait()
	})
}

func testSchedulerAndWatcher(t *testing.T) {
	Convey("test scheduler and config watcher", t, func() {
		zkAddr := []string{"localhost:2181"}
		writer, _, err := zk.Connect(zkAddr, 3*time.Second)
		So(err, ShouldBeNil)
		_, err = writer.Create("/config", []byte("config"), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		channel := make(chan []byte, 100)
		conn, _, err := zk.Connect(zkAddr, 3*time.Second)
		So(err, ShouldBeNil)
		cw := zookeeper.NewConfigWatcher(ctx, wg, channel, conn, "/config")
		cw.Start()
		sh := zroute.NewServiceHost()
		go func() {
			sh.Sync(channel)
		}()
		var s Scheduler
		ss, err := NewSimpleScheduler(ctx, wg, zkAddr, 1, &sh)
		s = &ss
		go func() {
			s.Listen("/yurt")
		}()

		initSh := make(map[string][]string)
		initSh["service1"] = []string{}
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)
		err = enc.Encode(initSh)
		So(err, ShouldBeNil)

		_, stat, err := writer.Get("/config")
		So(err, ShouldBeNil)
		_, err = writer.Set("/config", buf.Bytes(), stat.Version)
		So(err, ShouldBeNil)

		time.Sleep(2 * time.Second)

		_, err = writer.Create("/yurt/idle00", []byte("idle00"), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		So(err, ShouldBeNil)

		time.Sleep(time.Second)

		data0, _, err := writer.Get("/yurt/idle00")
		So(err, ShouldBeNil)
		So(string(data0), ShouldEqual, "service1")

		time.Sleep(time.Second)

		initSh = make(map[string][]string)
		initSh["service1"] = []string{"localhost:9999"}
		buf = new(bytes.Buffer)
		enc = gob.NewEncoder(buf)
		err = enc.Encode(initSh)
		So(err, ShouldBeNil)

		_, stat, err = writer.Get("/config")
		So(err, ShouldBeNil)
		_, err = writer.Set("/config", buf.Bytes(), stat.Version)
		So(err, ShouldBeNil)

		time.Sleep(1 * time.Second)
		_, err = writer.Create("/yurt/idle01", []byte("idle01"), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		So(err, ShouldBeNil)

		data1, _, err := writer.Get("/yurt/idle01")
		So(err, ShouldBeNil)
		So(string(data1), ShouldEqual, "idle01")

		cancel()
		wg.Wait()
		writer.Close()
	})
}

func setup() {
	zkAddr := []string{"localhost:2181"}
	conn, _, _ := zk.Connect(zkAddr, 5*time.Second)
	conn.Create("/yurt", []byte{}, 0, zk.WorldACL(zk.PermAll))
	conn.Close()
}

func teardown() {
	zkAddr := []string{"localhost:2181"}
	conn, _, _ := zk.Connect(zkAddr, 5*time.Second)
	_, stat, _ := conn.Get("/yurt")
	_ = conn.Delete("/yurt", stat.Version)
	conn.Close()
}

func TestEverything(t *testing.T) {
	// Maintaining this map is error-prone and cumbersome (note the subtle bug):
	fs := map[string]func(*testing.T){
		"testScheduler":           testScheduler,
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

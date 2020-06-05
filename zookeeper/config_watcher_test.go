package zookeeper

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	. "github.com/smartystreets/goconvey/convey"
)

func TestConfigWatcher(t *testing.T) {
	Convey("test config watcher", t, func() {
		fmt.Println("test config watcher")
		writer, _, err := zk.Connect([]string{"localhost:2181"}, 3*time.Second)
		So(err, ShouldBeNil)
		_, err = writer.Create("/config", []byte("config"), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		So(err, ShouldBeNil)
		_, stat, err := writer.Get("/config")
		So(err, ShouldBeNil)
		ctx, canncel := context.WithCancel(context.Background())
		channel := make(chan []byte, 100)
		conn, _, err := zk.Connect([]string{"localhost:2181"}, 3*time.Second)
		So(err, ShouldBeNil)
		wg := &sync.WaitGroup{}
		watcher := NewConfigWatcher(ctx, wg, channel, conn, "/config")
		watcher.Start()
		stat, err = writer.Set("/config", []byte("gifnoc"), stat.Version)
		So(err, ShouldBeNil)
		stat, err = writer.Set("/config", []byte("dll"), stat.Version)
		So(err, ShouldBeNil)
		canncel()
		wg.Wait()
		receive := []string{}
		for {
			if value, ok := <-channel; ok {
				receive = append(receive, string(value))
			} else {
				break
			}
		}
		So(receive, ShouldHaveLength, 3)
		So(receive[0], ShouldEqual, "config")
		So(receive[1], ShouldEqual, "gifnoc")
		So(receive[2], ShouldEqual, "dll")
	})
}

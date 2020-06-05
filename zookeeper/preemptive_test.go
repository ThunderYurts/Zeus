package zookeeper

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	. "github.com/smartystreets/goconvey/convey"
)

func testPreemptive(t *testing.T) {
	Convey("test preemptive", t, func() {
		fmt.Println("test preemptive")
		wg := &sync.WaitGroup{}
		connmaster, _, err := zk.Connect([]string{"localhost:2181"}, 5*time.Second)
		master := NewPreemptive("/master", wg, connmaster)
		So(err, ShouldBeNil)
		err = master.Preemptive([]byte("master"))
		So(err, ShouldBeNil)
		conn, _, err := zk.Connect([]string{"localhost:2181"}, 5*time.Second)
		So(err, ShouldBeNil)
		data, stat, err := conn.Get("/master")
		So(err, ShouldBeNil)
		So(stat.EphemeralOwner, ShouldEqual, master.GetSessionID())
		So(string(data), ShouldEqual, "master")
		time.Sleep(2 * time.Second)
		go func() {
			time.Sleep(5 * time.Second)
			master.Close()
		}()
		connbackup, _, err := zk.Connect([]string{"localhost:2181"}, 5*time.Second)
		So(err, ShouldBeNil)
		backup := NewPreemptive("/master", wg, connbackup)
		backup.Preemptive([]byte("backup"))
		data, stat, err = conn.Get("/master")
		So(err, ShouldBeNil)
		So(stat.EphemeralOwner, ShouldEqual, backup.GetSessionID())
		So(string(data), ShouldEqual, "backup")
		backup.Close()
	})
}

func TestEverything(t *testing.T) {
	// Maintaining this map is error-prone and cumbersome (note the subtle bug):
	fs := map[string]func(*testing.T){
		"testPreemptive": testPreemptive,
	}
	// You may be able to use the `importer` package to enumerate tests instead,
	// but that starts getting complicated.
	for name, f := range fs {
		t.Run(name, f)
	}
}

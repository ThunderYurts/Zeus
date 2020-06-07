package test

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"testing"
	"time"

	"github.com/ThunderYurts/Zeus/zconst"
	"github.com/ThunderYurts/Zeus/zeus"
	"github.com/ThunderYurts/Zeus/zookeeper"
	"github.com/ThunderYurts/Zeus/zsource"
	"github.com/samuel/go-zookeeper/zk"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
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

func TestZeusStart(t *testing.T) {
	// Convey("test zeus start", t, func() {
	// 	zkAddr := []string{"localhost:2181"}
	// 	conn, _, err := zk.Connect(zkAddr, 1*time.Second)
	// 	So(err, ShouldBeNil)
	// 	// clean up start
	// 	cleanNode := []string{zconst.YurtRoot, zconst.SlotsRoot, zconst.ServiceRoot}
	// 	for _, node := range cleanNode {
	// 		deleteNode(node, conn)
	// 	}
	// 	// clean up end
	// 	testAddr := "localhost:8888"
	// 	testAddrExt := "localhost:7777"
	// 	testPort := ":40000"
	// 	ctx, cancel := context.WithCancel(context.Background())
	// 	zeus := zeus.NewZeus(ctx, cancel, "zeus", 0, 11384)
	// 	err = zeus.Start(testPort, []string{"localhost:2181"})
	// 	So(err, ShouldBeNil)
	// 	exist, _, err := conn.Exists(zconst.YurtRoot)
	// 	So(err, ShouldBeNil)
	// 	So(exist, ShouldBeTrue)

	// 	exist, _, err = conn.Exists(zconst.ServiceRoot)
	// 	So(err, ShouldBeNil)
	// 	So(exist, ShouldBeTrue)

	// 	exist, _, err = conn.Exists(zconst.SlotsRoot)
	// 	So(err, ShouldBeNil)
	// 	So(exist, ShouldBeTrue)

	// 	// fake yurt register
	// 	_, err = conn.Create(zconst.YurtRoot+"/idle0", []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	// 	So(err, ShouldBeNil)
	// 	time.Sleep(2 * time.Second)
	// 	data, _, err := conn.Get(zconst.YurtRoot + "/idle0")
	// 	So(len(data), ShouldNotBeZeroValue)
	// 	key := string(data)
	// 	So(key, ShouldEqual, "0-11384")
	// 	// fake primary start

	// 	time.Sleep(2 * time.Second)

	// 	data, stat, err := conn.Get(zconst.ServiceRoot + "/" + key)
	// 	So(err, ShouldBeNil)
	// 	So(len(data), ShouldNotBeZeroValue)
	// 	dec := gob.NewDecoder(bytes.NewBuffer(data))
	// 	newHosts := zookeeper.ZKServiceHost{}
	// 	err = dec.Decode(&newHosts)
	// 	So(err, ShouldBeNil)
	// 	So(len(newHosts.Value), ShouldBeZeroValue)
	// 	newHosts.Value = []string{testAddr}
	// 	buf := new(bytes.Buffer)
	// 	enc := gob.NewEncoder(buf)
	// 	err = enc.Encode(newHosts)
	// 	So(err, ShouldBeNil)
	// 	stat, err = conn.Set(zconst.ServiceRoot+"/"+key, buf.Bytes(), stat.Version)
	// 	So(err, ShouldBeNil)

	// 	time.Sleep(1 * time.Second)

	// 	// try to call server get source

	// 	connServer, err := grpc.Dial(testPort, grpc.WithInsecure())
	// 	So(err, ShouldBeNil)
	// 	// create stream
	// 	client := zsource.NewSourceClient(connServer)
	// 	res, err := client.Source(ctx, &zsource.SourceRequest{Key: "animal", Action: zconst.ActionRead})
	// 	So(err, ShouldBeNil)
	// 	So(res.Code, ShouldEqual, zsource.SourceCode_SOURCE_SUCCESS)
	// 	So(res.Addr, ShouldEqual, testAddr)

	// 	// test round robin

	// 	newHosts.Value = []string{testAddr, testAddrExt}
	// 	buf = new(bytes.Buffer)
	// 	enc = gob.NewEncoder(buf)
	// 	err = enc.Encode(newHosts)
	// 	So(err, ShouldBeNil)
	// 	stat, err = conn.Set(zconst.ServiceRoot+"/"+key, buf.Bytes(), stat.Version)
	// 	So(err, ShouldBeNil)

	// 	time.Sleep(1 * time.Second)

	// 	lastOne := testAddr
	// 	for i := 0; i < 4; i = i + 1 {
	// 		res, err = client.Source(ctx, &zsource.SourceRequest{Key: "animal", Action: zconst.ActionRead})
	// 		So(err, ShouldBeNil)
	// 		So(res.Code, ShouldEqual, zsource.SourceCode_SOURCE_SUCCESS)
	// 		So(res.Addr, ShouldNotEqual, lastOne)
	// 		lastOne = res.Addr
	// 	}

	// 	zeus.Stop()
	// })

	Convey("test master backup working", t, func() {
		zkAddr := []string{"localhost:2181"}
		conn, _, err := zk.Connect(zkAddr, 1*time.Second)
		So(err, ShouldBeNil)
		// clean up start
		cleanNode := []string{zconst.YurtRoot, zconst.SlotsRoot, zconst.ServiceRoot}
		for _, node := range cleanNode {
			deleteNode(node, conn)
		}
		// clean up end
		testAddr := "localhost:8888"
		testPort := ":40000"
		masterCtx, masterCancel := context.WithCancel(context.Background())
		backupCtx, backupCancel := context.WithCancel(context.Background())
		master := zeus.NewZeus(masterCtx, masterCancel, "master", 0, 11384)
		err = master.Start(testPort, []string{"localhost:2181"})
		So(err, ShouldBeNil)
		exist, _, err := conn.Exists(zconst.YurtRoot)
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)

		exist, _, err = conn.Exists(zconst.ServiceRoot)
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)

		exist, _, err = conn.Exists(zconst.SlotsRoot)
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)

		backup := zeus.NewZeus(backupCtx, backupCancel, "backup", 0, 11384)
		go func() {
			err = backup.Start(testPort, []string{"localhost:2181"})
		}()

		So(err, ShouldBeNil)

		key := "0-11384"
		newHosts := zookeeper.ZKServiceHost{}

		newHosts.Key = key
		newHosts.Value = []string{testAddr}
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)
		err = enc.Encode(newHosts)
		So(err, ShouldBeNil)
		// skip register step, inject service
		_, stat, err := conn.Get(zconst.ServiceRoot + "/" + key)
		So(err, ShouldBeNil)
		stat, err = conn.Set(zconst.ServiceRoot+"/"+key, buf.Bytes(), stat.Version)
		So(err, ShouldBeNil)

		time.Sleep(1 * time.Second)

		connServer, err := grpc.Dial(testPort, grpc.WithInsecure())
		client := zsource.NewSourceClient(connServer)
		res, err := client.Source(masterCtx, &zsource.SourceRequest{Key: "animal", Action: zconst.ActionRead})
		So(err, ShouldBeNil)
		So(res.Code, ShouldEqual, zsource.SourceCode_SOURCE_SUCCESS)
		So(res.Addr, ShouldEqual, testAddr)
		master.Stop()
		fmt.Println("master stop")
		time.Sleep(5 * time.Second)
		connServer, err = grpc.Dial(testPort, grpc.WithInsecure())
		So(err, ShouldBeNil)
		client = zsource.NewSourceClient(connServer)
		res, err = client.Source(backupCtx, &zsource.SourceRequest{Key: "animal", Action: zconst.ActionRead})
		So(err, ShouldBeNil)
		So(res.Code, ShouldEqual, zsource.SourceCode_SOURCE_SUCCESS)
		So(res.Addr, ShouldEqual, testAddr)
		backup.Stop()
		conn.Close()
	})

}

package test

import (
	"bytes"
	"context"
	"encoding/gob"
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
	// 	cleanNode := []string{zconst.YurtRoot, zconst.SlotsRoot, zconst.ServiceRoot, zconst.PreSlotsRoot}
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
	// 	data, stat, err := conn.Get(zconst.YurtRoot + "/idle0")
	// 	dec := gob.NewDecoder(bytes.NewBuffer(data))
	// 	reg0 := zookeeper.ZKRegister{}
	// 	err = dec.Decode(&reg0)
	// 	So(reg0.ServiceName, ShouldHaveLength, 10)
	// 	key := reg0.ServiceName
	// 	// fake primary start

	// 	time.Sleep(2 * time.Second)
	// 	err = conn.Delete(zconst.YurtRoot+"/idle0", stat.Version)
	// 	So(err, ShouldBeNil)
	// 	data, stat, err = conn.Get(zconst.ServiceRoot + "/" + key)
	// 	So(err, ShouldBeNil)
	// 	So(len(data), ShouldNotBeZeroValue)
	// 	dec = gob.NewDecoder(bytes.NewBuffer(data))
	// 	newHosts := zookeeper.ZKServiceHost{}
	// 	err = dec.Decode(&newHosts)
	// 	So(err, ShouldBeNil)
	// 	So(len(newHosts.Secondary), ShouldBeZeroValue)
	// 	So(newHosts.Primary, ShouldEqual, "")
	// 	newHosts.Primary = testAddr
	// 	newHosts.Secondary = []string{}
	// 	buf := new(bytes.Buffer)
	// 	enc := gob.NewEncoder(buf)
	// 	err = enc.Encode(newHosts)
	// 	So(err, ShouldBeNil)
	// 	stat, err = conn.Set(zconst.ServiceRoot+"/"+key, buf.Bytes(), stat.Version)
	// 	So(err, ShouldBeNil)

	// 	time.Sleep(2 * time.Second)

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

	// 	newHosts.Secondary = []string{testAddrExt}
	// 	buf = new(bytes.Buffer)
	// 	enc = gob.NewEncoder(buf)
	// 	err = enc.Encode(newHosts)
	// 	So(err, ShouldBeNil)
	// 	stat, err = conn.Set(zconst.ServiceRoot+"/"+key, buf.Bytes(), stat.Version)
	// 	So(err, ShouldBeNil)

	// 	time.Sleep(1 * time.Second)

	// 	lastOne := ""
	// 	// test read action
	// 	for i := 0; i < 4; i = i + 1 {
	// 		res, err = client.Source(ctx, &zsource.SourceRequest{Key: "animal", Action: zconst.ActionRead})
	// 		So(err, ShouldBeNil)
	// 		So(res.Code, ShouldEqual, zsource.SourceCode_SOURCE_SUCCESS)
	// 		So(res.Addr, ShouldNotEqual, lastOne)
	// 		lastOne = res.Addr
	// 	}

	// 	// test put/delete action
	// 	for i := 0; i < 4; i = i + 1 {
	// 		res, err = client.Source(ctx, &zsource.SourceRequest{Key: "animal", Action: zconst.ActionPut})
	// 		So(err, ShouldBeNil)
	// 		So(res.Code, ShouldEqual, zsource.SourceCode_SOURCE_SUCCESS)
	// 		So(res.Addr, ShouldEqual, testAddr)
	// 		lastOne = res.Addr
	// 	}

	// 	zeus.Stop()
	// })

	// Convey("test master backup working", t, func() {
	// 	zkAddr := []string{"localhost:2181"}
	// 	conn, _, err := zk.Connect(zkAddr, 1*time.Second)
	// 	So(err, ShouldBeNil)
	// 	// clean up start
	// 	cleanNode := []string{zconst.YurtRoot, zconst.SlotsRoot, zconst.ServiceRoot, zconst.PreSlotsRoot}
	// 	for _, node := range cleanNode {
	// 		deleteNode(node, conn)
	// 	}

	// 	// clean up end
	// 	testAddr := "localhost:8888"
	// 	testPort := ":40000"
	// 	masterCtx, masterCancel := context.WithCancel(context.Background())
	// 	backupCtx, backupCancel := context.WithCancel(context.Background())
	// 	master := zeus.NewZeus(masterCtx, masterCancel, "master", 0, 11384)
	// 	err = master.Start(testPort, []string{"localhost:2181"})
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

	// 	backup := zeus.NewZeus(backupCtx, backupCancel, "backup", 0, 11384)
	// 	go func() {
	// 		err = backup.Start(testPort, []string{"localhost:2181"})
	// 	}()

	// 	_, err = conn.Create(zconst.YurtRoot+"/idle0", []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	// 	So(err, ShouldBeNil)
	// 	time.Sleep(2 * time.Second)
	// 	data, _, err := conn.Get(zconst.YurtRoot + "/idle0")
	// 	So(err, ShouldBeNil)
	// 	dec := gob.NewDecoder(bytes.NewBuffer(data))
	// 	reg0 := zookeeper.ZKRegister{}
	// 	err = dec.Decode(&reg0)
	// 	So(err, ShouldBeNil)
	// 	So(reg0.ServiceName, ShouldHaveLength, 10)
	// 	key := reg0.ServiceName

	// 	newHosts := zookeeper.ZKServiceHost{}
	// 	newHosts.Service = key
	// 	newHosts.SlotBegin = 0
	// 	newHosts.SlotEnd = zconst.TotalSlotNum
	// 	newHosts.Primary = testAddr
	// 	newHosts.Secondary = []string{}
	// 	newHosts.SyncHost = ""
	// 	buf := new(bytes.Buffer)
	// 	enc := gob.NewEncoder(buf)
	// 	err = enc.Encode(newHosts)
	// 	So(err, ShouldBeNil)

	// 	time.Sleep(1 * time.Second)

	// 	_, stat, err := conn.Get(zconst.ServiceRoot + "/" + key)
	// 	So(err, ShouldBeNil)
	// 	_, err = conn.Set(zconst.ServiceRoot+"/"+key, buf.Bytes(), stat.Version)
	// 	So(err, ShouldBeNil)
	// 	connServer, err := grpc.Dial(testPort, grpc.WithInsecure())
	// 	client := zsource.NewSourceClient(connServer)
	// 	res, err := client.Source(masterCtx, &zsource.SourceRequest{Key: "animal", Action: zconst.ActionRead})
	// 	So(err, ShouldBeNil)
	// 	So(res.Code, ShouldEqual, zsource.SourceCode_SOURCE_SUCCESS)
	// 	So(res.Addr, ShouldEqual, testAddr)
	// 	master.Stop()
	// 	fmt.Println("master stop")
	// 	time.Sleep(5 * time.Second)
	// 	connServer, err = grpc.Dial(testPort, grpc.WithInsecure())
	// 	So(err, ShouldBeNil)
	// 	client = zsource.NewSourceClient(connServer)
	// 	res, err = client.Source(backupCtx, &zsource.SourceRequest{Key: "animal", Action: zconst.ActionRead})
	// 	So(err, ShouldBeNil)
	// 	So(res.Code, ShouldEqual, zsource.SourceCode_SOURCE_SUCCESS)
	// 	So(res.Addr, ShouldEqual, testAddr)
	// 	backup.Stop()
	// 	conn.Close()
	// })

	Convey("test service split working", t, func() {
		zkAddr := []string{"localhost:2181"}
		conn, _, err := zk.Connect(zkAddr, 1*time.Second)
		So(err, ShouldBeNil)
		// clean up start
		cleanNode := []string{zconst.YurtRoot, zconst.SlotsRoot, zconst.ServiceRoot, zconst.PreSlotsRoot}
		for _, node := range cleanNode {
			deleteNode(node, conn)
		}
		testAddr := "localhost:8888"
		testAddrExt := "localhost:7777"
		testSecond := "localhost:6666"
		testSecondExt := "localhost:8888"
		testPort := ":40000"
		ctx, cancel := context.WithCancel(context.Background())
		zeus := zeus.NewZeus(ctx, cancel, "zeus", 0, 11384)
		err = zeus.Start(testPort, []string{"localhost:2181"})
		_, err = conn.Create(zconst.YurtRoot+"/idle0", []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		data, stat, err := conn.Get(zconst.YurtRoot + "/idle0")
		dec := gob.NewDecoder(bytes.NewBuffer(data))
		reg0 := zookeeper.ZKRegister{}
		err = dec.Decode(&reg0)
		So(reg0.ServiceName, ShouldHaveLength, 10)
		key := reg0.ServiceName
		// fake primary start

		time.Sleep(2 * time.Second)
		err = conn.Delete(zconst.YurtRoot+"/idle0", stat.Version)
		So(err, ShouldBeNil)
		data, stat, err = conn.Get(zconst.ServiceRoot + "/" + key)
		So(err, ShouldBeNil)
		So(len(data), ShouldNotBeZeroValue)
		dec = gob.NewDecoder(bytes.NewBuffer(data))
		newHosts := zookeeper.ZKServiceHost{}
		err = dec.Decode(&newHosts)
		So(err, ShouldBeNil)
		So(len(newHosts.Secondary), ShouldBeZeroValue)
		So(newHosts.Primary, ShouldEqual, "")
		newHosts.Primary = testAddr
		newHosts.Secondary = []string{}
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)
		err = enc.Encode(newHosts)
		So(err, ShouldBeNil)
		stat, err = conn.Set(zconst.ServiceRoot+"/"+key, buf.Bytes(), stat.Version)
		So(err, ShouldBeNil)

		time.Sleep(2 * time.Second)

		newHosts.Secondary = []string{testAddrExt}
		buf = new(bytes.Buffer)
		enc = gob.NewEncoder(buf)
		err = enc.Encode(newHosts)
		So(err, ShouldBeNil)
		stat, err = conn.Set(zconst.ServiceRoot+"/"+key, buf.Bytes(), stat.Version)
		So(err, ShouldBeNil)

		// the third one to register
		_, err = conn.Create(zconst.YurtRoot+"/idle2", []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		data, stat, err = conn.Get(zconst.YurtRoot + "/idle2")
		dec = gob.NewDecoder(bytes.NewBuffer(data))
		reg2 := zookeeper.ZKRegister{}
		err = dec.Decode(&reg2)
		key = reg2.ServiceName
		So(reg2.ServiceName, ShouldHaveLength, 10)
		So(reg2.ServiceName, ShouldNotEqual, reg0.ServiceName)

		data, serviceStat, err := conn.Get(zconst.ServiceRoot + "/" + key)
		dec = gob.NewDecoder(bytes.NewBuffer(data))
		service2 := zookeeper.ZKServiceHost{}
		err = dec.Decode(&service2)
		So(err, ShouldBeNil)
		So(service2.SlotBegin, ShouldEqual, 5692)
		So(service2.SlotEnd, ShouldEqual, zconst.TotalSlotNum)
		key = service2.Service
		secondService := zookeeper.ZKServiceHost{}
		secondService.Primary = testSecond
		secondService.Secondary = []string{testSecondExt}
		secondService.SlotBegin = service2.SlotBegin
		secondService.SlotEnd = service2.SlotEnd
		secondService.Service = key
		secondService.SyncHost = "" // fake sync over
		buf = new(bytes.Buffer)
		enc = gob.NewEncoder(buf)
		err = enc.Encode(secondService)
		So(err, ShouldBeNil)
		stat, err = conn.Set(zconst.ServiceRoot+"/"+key, buf.Bytes(), serviceStat.Version)
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)

		connServer, err := grpc.Dial(testPort, grpc.WithInsecure())
		client := zsource.NewSourceClient(connServer)
		res, err := client.Source(ctx, &zsource.SourceRequest{Key: "dog", Action: zconst.ActionPut})
		So(err, ShouldBeNil)
		So(res.Code, ShouldEqual, zsource.SourceCode_SOURCE_SUCCESS)
		So(res.Addr, ShouldEqual, testSecond)
		res, err = client.Source(ctx, &zsource.SourceRequest{Key: "giraffe", Action: zconst.ActionPut})
		So(err, ShouldBeNil)
		So(res.Code, ShouldEqual, zsource.SourceCode_SOURCE_SUCCESS)
		So(res.Addr, ShouldEqual, testAddr)
	})

}

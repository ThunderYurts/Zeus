package test

import (
	"context"
	"fmt"
	"github.com/ThunderYurts/Zeus/action"
	"github.com/ThunderYurts/Zeus/zconst"
	"github.com/ThunderYurts/Zeus/zsource"
	"github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
	"testing"
	"time"
)

func TestCluster(t *testing.T) {
	convey.Convey("use client test", t, func() {
		connServer, err := grpc.Dial(":50001", grpc.WithInsecure())
		if err != nil {
			panic(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		zeusClient := zsource.NewSourceClient(connServer)
		animals := []string{"pig", "dog", "cat", "noob"}
		for i := 0; i < len(animals)*3; i = i + 1 {
			reply, err := zeusClient.Source(ctx, &zsource.SourceRequest{Key: "animal", Action: zconst.ActionPut})
			convey.So(reply, convey.ShouldNotBeNil)
			convey.So(reply.Code, convey.ShouldEqual, zsource.SourceCode_SOURCE_SUCCESS)
			connAction, err := grpc.Dial(reply.Addr, grpc.WithInsecure())
			convey.So(err, convey.ShouldBeNil)
			convey.So(connAction, convey.ShouldNotBeNil)
			yurtClient := action.NewActionClient(connAction)
			fmt.Println(reply.Addr)
			putReply, err := yurtClient.Put(ctx, &action.PutRequest{Key: "animal", Value: animals[i%len(animals)]})
			convey.So(err, convey.ShouldBeNil)
			convey.So(putReply, convey.ShouldNotBeNil)
			convey.So(reply.Code, convey.ShouldEqual, action.PutCode_PUT_SUCCESS)
			reply, err = zeusClient.Source(ctx, &zsource.SourceRequest{Key: "animal", Action: zconst.ActionRead})
			convey.So(err, convey.ShouldBeNil)
			connAction, err = grpc.Dial(reply.Addr, grpc.WithInsecure())
			convey.So(err, convey.ShouldBeNil)
			fmt.Println(reply.Addr)
			time.Sleep(1 * time.Second)
			yurtClient = action.NewActionClient(connAction)
			readReply, err := yurtClient.Read(ctx, &action.ReadRequest{Key: "animal"})
			convey.So(err, convey.ShouldBeNil)
			convey.So(reply.Code, convey.ShouldEqual, action.ReadCode_READ_SUCCESS)
			convey.So(readReply.Value, convey.ShouldEqual, animals[i%len(animals)])
		}
		//keys := []string{"dog", "giraffe"}

		//addr := mapset.NewSet()
		//for i := 0; i < 2; i = i + 1 {
		//	reply, err := zeusClient.Source(ctx, &zsource.SourceRequest{Key: keys[i], Action: zconst.ActionPut})
		//	convey.So(reply, convey.ShouldNotBeNil)
		//	convey.So(reply.Code, convey.ShouldEqual, zsource.SourceCode_SOURCE_SUCCESS)
		//	connAction, err := grpc.Dial(reply.Addr, grpc.WithInsecure())
		//	convey.So(err, convey.ShouldBeNil)
		//	addr.Add(reply.Addr)
		//	yurtClient := action.NewActionClient(connAction)
		//	putReply, err := yurtClient.Put(ctx, &action.PutRequest{Key: keys[i], Value: keys[i]})
		//	convey.So(err, convey.ShouldBeNil)
		//	convey.So(putReply, convey.ShouldNotBeNil)
		//	convey.So(reply.Code, convey.ShouldEqual, action.PutCode_PUT_SUCCESS)
		//	reply, err = zeusClient.Source(ctx, &zsource.SourceRequest{Key: keys[i], Action: zconst.ActionRead})
		//	convey.So(err, convey.ShouldBeNil)
		//	connAction, err = grpc.Dial(reply.Addr, grpc.WithInsecure())
		//	convey.So(err, convey.ShouldBeNil)
		//	addr.Add(reply.Addr)
		//	yurtClient = action.NewActionClient(connAction)
		//	readReply, err := yurtClient.Read(ctx, &action.ReadRequest{Key: keys[i]})
		//	convey.So(err, convey.ShouldBeNil)
		//	convey.So(reply.Code, convey.ShouldEqual, action.ReadCode_READ_SUCCESS)
		//	convey.So(readReply.Value, convey.ShouldEqual, keys[i])
		//}
		cancel()
		//convey.So(addr.Cardinality(), convey.ShouldEqual, 2)
		//t.Logf("addrs: %v\n", addr.ToSlice())
	})
}

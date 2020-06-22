package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/ThunderYurts/Zeus/action"
	"github.com/ThunderYurts/Zeus/zconst"
	"github.com/ThunderYurts/Zeus/zsource"
	"google.golang.org/grpc"
	"os"
	"strings"
)

func main() {
	for {

		inputReader := bufio.NewReader(os.Stdin)
		input, err := inputReader.ReadString('\n')
		if err != nil {
			panic(err)
		}
		// {master addr} {action} {specific param}
		param := strings.Fields(input)
		addr := param[0]
		connServer, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			panic(err)
		}
		ctx := context.Background()
		zeusClient := zsource.NewSourceClient(connServer)
		act := param[1]
		switch act {
		case "P":
			{
				// {master addr} {P} {Key} {Value}
				sourceReply, err := zeusClient.Source(ctx, &zsource.SourceRequest{Key: param[2], Action: zconst.ActionPut})
				if err != nil {
					fmt.Println(err.Error())
				} else {
					connAction, err := grpc.Dial(sourceReply.Addr, grpc.WithInsecure())
					if err != nil {
						fmt.Println(err.Error())
					}
					fmt.Printf("connect to %s\n", sourceReply.Addr)
					yurtClient := action.NewActionClient(connAction)
					putReply, err := yurtClient.Put(ctx, &action.PutRequest{Key: param[2], Value: param[3]})
					if err != nil {
						fmt.Println(err.Error())
					}
					fmt.Println(putReply)
				}
			}
		case "D":
			{
				// {master addr} {D} {Key}
				sourceReply, err := zeusClient.Source(ctx, &zsource.SourceRequest{Key: param[2], Action: zconst.ActionDelete})
				if err != nil {
					fmt.Println(err.Error())
				} else {
					connAction, err := grpc.Dial(sourceReply.Addr, grpc.WithInsecure())
					if err != nil {
						fmt.Println(err.Error())
					}
					fmt.Printf("connect to %s\n", sourceReply.Addr)
					yurtClient := action.NewActionClient(connAction)
					delReply, err := yurtClient.Delete(ctx, &action.DeleteRequest{Key: param[2]})
					if err != nil {
						fmt.Println(err.Error())
					}
					fmt.Println(delReply)
				}
			}
		case "R":
			{
				// {master addr} {R} {Key}
				sourceReply, err := zeusClient.Source(ctx, &zsource.SourceRequest{Key: param[2], Action: zconst.ActionRead})
				if err != nil {
					fmt.Println(err.Error())
				} else {
					connAction, err := grpc.Dial(sourceReply.Addr, grpc.WithInsecure())
					if err != nil {
						fmt.Println(err.Error())
					}
					fmt.Printf("connect to %s\n", sourceReply.Addr)
					yurtClient := action.NewActionClient(connAction)
					readReply, err := yurtClient.Read(ctx, &action.ReadRequest{Key: param[2]})
					if err != nil {
						fmt.Println(err.Error())
					}
					fmt.Println(readReply)
				}
			}
		case "RB":
			{
				reply, err := zeusClient.ReBalance(ctx, &zsource.ReBalanceRequest{})
				if err != nil {
					fmt.Println(err.Error())
				}
				fmt.Println(reply)
			}
		default:
			{
				fmt.Println("invalid action")
			}
		}
	}
}

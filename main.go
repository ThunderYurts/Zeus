package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/ThunderYurts/Zeus/zconst"
	"github.com/ThunderYurts/Zeus/zeus"
)

var (
	help       bool
	sourcePort string
	zkAddr string
	name       string
)

func init() {
	flag.BoolVar(&help, "h", false, "this help")
	flag.StringVar(&sourcePort, "sp", ":50001", "set sourcePort port")
	flag.StringVar(&zkAddr, "zk", "106.15.225.249:3030", "set zeus connection zookeeper cluster")
	flag.StringVar(&name, "n", "zeus", "set zeus name")
}

func main() {
	flag.Parse()
	if help {
		flag.Usage()
		return
	}
	rootContext, finalizeFunc := context.WithCancel(context.Background())
	// TODO split slots
	zeus := zeus.NewZeus(rootContext, finalizeFunc, name, 0, zconst.TotalSlotNum)
	// TODO change zk addr
	addrs := strings.Split(zkAddr, ";")
	err := zeus.Start(sourcePort, addrs)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	s := <-c

	fmt.Println("Got signal:", s)
	zeus.Stop()
}

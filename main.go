package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/ThunderYurts/Zeus/zeus"
)

var (
	help       bool
	sourcePort string
	name       string
)

func init() {
	flag.BoolVar(&help, "h", false, "this help")
	flag.StringVar(&sourcePort, "sp", ":30000", "set sourcePort port")
	flag.StringVar(&name, "n", "zeus", "set zeus name")
}

func main() {
	flag.Parse()
	if help {
		flag.Usage()
		return
	}
	rootContext, finalizeFunc := context.WithCancel(context.Background())

	zeus := zeus.NewZeus(rootContext, finalizeFunc, name)

	zeus.Start(sourcePort)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	s := <-c

	fmt.Println("Got signal:", s)
	zeus.Stop()
}

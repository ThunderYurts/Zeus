package zeus

import (
	"context"
	"sync"

	"github.com/ThunderYurts/Zeus/source"
	"github.com/ThunderYurts/Zeus/zroute"
	"github.com/ThunderYurts/Zeus/zslot"
)

// Config of Zeus contains Source Server config
type Config struct {
	SourceConfig *source.ServerConfig
	Name         string
}

// Zeus is control pannel of ThunderYurt
type Zeus struct {
	ctx           context.Context
	fianalizeFunc context.CancelFunc
	sourceServer  source.Server
	wg            *sync.WaitGroup
	config        *Config
}

// NewZeus is a help function
func NewZeus(ctx context.Context, fianalizeFunc context.CancelFunc, name string) Zeus {
	// TODO choose algo
	algo := zroute.NewRoundRobin()
	serviceHost := zroute.NewServiceHost()

	sourceConfig := source.NewServerConfig()
	sc := zslot.NewSlotCluster()
	config := Config{
		SourceConfig: &sourceConfig,
		Name:         name,
	}
	return Zeus{
		ctx:           ctx,
		fianalizeFunc: fianalizeFunc,
		sourceServer:  source.NewServer(ctx, &algo, &serviceHost, &sourceConfig, &sc),
		wg:            &sync.WaitGroup{},
		config:        &config,
	}
}

// Start Zues
func (z *Zeus) Start(sourcePort string) {
	z.sourceServer.Start(sourcePort, z.wg)

}

// Stop Zues
func (z *Zeus) Stop() {
	z.fianalizeFunc()
	z.wg.Wait()
}

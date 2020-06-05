package source

import (
	"context"
	"fmt"
	"net"
	sync "sync"

	"github.com/ThunderYurts/Zeus/zconst"
	"github.com/ThunderYurts/Zeus/zroute"
	"github.com/ThunderYurts/Zeus/zslot"
	grpc "google.golang.org/grpc"
)

// ServerConfig stores config sync from zookeeper
type ServerConfig struct {
	Locked bool
}

// NewServerConfig is return default ServerConfig
func NewServerConfig() ServerConfig {
	return ServerConfig{Locked: false}
}

// Server in source can support service address
type Server struct {
	ctx    context.Context
	algo   zroute.Algo
	hosts  *zroute.ServiceHost
	config *ServerConfig
	sc     *zslot.SlotCluster
}

// NewServer is a help function for new Server
func NewServer(ctx context.Context, algo zroute.Algo, hosts *zroute.ServiceHost, config *ServerConfig, sc *zslot.SlotCluster) Server {
	return Server{
		ctx:    ctx,
		algo:   algo,
		hosts:  hosts,
		config: config,
		sc:     sc,
	}
}

// Source return service addr by key
func (s *Server) Source(ctx context.Context, in *SourceRequest) (*SourceReply, error) {
	if in.Action != zconst.ActionRead && in.Action != zconst.ActionPut && in.Action != zconst.ActionDelete {
		return &SourceReply{Code: SourceCode_SOURCE_ERROR, Addr: ""}, nil
	}

	if (in.Action == zconst.ActionPut || in.Action == zconst.ActionDelete) && s.config.Locked {
		return &SourceReply{Code: SourceCode_SOURCE_LOCK, Addr: ""}, nil
	}

	h, err := s.sc.Hash(in.Key)

	if err != nil {
		fmt.Println(err.Error())
		return &SourceReply{Code: SourceCode_SOURCE_ERROR, Addr: ""}, err
	}

	addr, err := s.algo.Source(h, s.hosts)
	if err != nil {
		fmt.Println(err.Error())
		return &SourceReply{Code: SourceCode_SOURCE_ERROR, Addr: ""}, nil
	}
	return &SourceReply{Code: SourceCode_SOURCE_SUCCESS, Addr: addr}, nil
}

// Start server
func (s *Server) Start(sourcePort string, wg *sync.WaitGroup) error {
	sourceServer := grpc.NewServer()
	RegisterSourceServer(sourceServer, s)
	lis, err := net.Listen("tcp", sourcePort)
	if err != nil {
		return err
	}
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		fmt.Printf("action server listen on %s\n", sourcePort)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			select {
			case <-s.ctx.Done():
				{
					fmt.Println("source get Done")
					sourceServer.GracefulStop()
					return
				}
			}
		}(wg)
		sourceServer.Serve(lis)
	}(wg)
	return nil
}

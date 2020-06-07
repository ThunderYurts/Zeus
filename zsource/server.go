package zsource

import (
	"context"
	"fmt"
	"net"
	"strconv"
	sync "sync"

	"github.com/ThunderYurts/Zeus/zconst"
	"github.com/ThunderYurts/Zeus/zookeeper"
	"github.com/ThunderYurts/Zeus/zroute"
	zscheduler "github.com/ThunderYurts/Zeus/zshceduler"
	"github.com/ThunderYurts/Zeus/zslot"
	"github.com/samuel/go-zookeeper/zk"
	grpc "google.golang.org/grpc"
)

// ServerConfig stores config sync from zookeeper
type ServerConfig struct {
	Locked bool // TODO in level 1 we ignore
}

// NewServerConfig is return default ServerConfig
func NewServerConfig() ServerConfig {
	return ServerConfig{Locked: false}
}

// Server in source can support service address
type Server struct {
	ctx                context.Context
	algo               zroute.Algo
	hosts              *zroute.ServiceHost
	config             *ServerConfig
	sc                 *zslot.SlotCluster
	serviceHostChannel chan []byte
	serviceHostWatcher *zookeeper.ConfigWatcher
	scheduler          zscheduler.Scheduler
	conn               *zk.Conn
}

// NewServer is a help function for new Server
func NewServer(ctx context.Context, algo zroute.Algo, config *ServerConfig, sc *zslot.SlotCluster) Server {
	// sc init
	return Server{
		ctx:                ctx,
		algo:               algo,
		hosts:              nil,
		config:             config,
		sc:                 sc,
		serviceHostChannel: make(chan []byte, 100),
		serviceHostWatcher: nil,
		scheduler:          nil,
		conn:               nil,
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
func (s *Server) Start(sourcePort string, conn *zk.Conn, slotBegin uint32, slotEnd uint32, wg *sync.WaitGroup) error {
	s.conn = conn
	sourceServer := grpc.NewServer()
	RegisterSourceServer(sourceServer, s)
	lis, err := net.Listen("tcp", sourcePort)
	if err != nil {
		return err
	}

	segment := strconv.Itoa(int(slotBegin)) + "-" + strconv.Itoa(int(slotEnd))

	serviceHostName := zconst.ServiceRoot + "/" + segment

	cw := zookeeper.NewConfigWatcher(s.ctx, wg, s.serviceHostChannel, s.conn, serviceHostName)
	s.serviceHostWatcher = &cw
	s.serviceHostWatcher.Start()
	sh := zroute.NewServiceHost()
	s.hosts = &sh
	go func() {
		s.hosts.Sync(s.serviceHostChannel)
	}()
	// TODO simple scheduler
	scheduler, err := zscheduler.NewSimpleScheduler(s.ctx, wg, s.conn, 2, s.hosts)
	if err != nil {
		return err
	}
	s.scheduler = &scheduler
	go s.scheduler.Listen(zconst.YurtRoot)

	sc := zslot.NewSlotCluster(s.conn, segment)
	s.sc = &sc

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		fmt.Printf("source server listen on %s\n", sourcePort)
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

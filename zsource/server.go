package zsource

import (
	"context"
	"fmt"
	"net"
	sync "sync"
	"time"

	"github.com/ThunderYurts/Zeus/zconst"
	"github.com/ThunderYurts/Zeus/zroute"
	"github.com/ThunderYurts/Zeus/zscheduler"
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
	serviceHostChannel chan []byte
	scheduler          zscheduler.Scheduler
	conn               *zk.Conn
	sc                 *zslot.SlotCluster
	gaiaManager        *zroute.GaiaManager
}

// NewServer is a help function for new Server
func NewServer(ctx context.Context, algo zroute.Algo, config *ServerConfig, sc *zslot.SlotCluster) Server {
	// sc init
	return Server{
		ctx:                ctx,
		algo:               algo,
		hosts:              nil,
		config:             config,
		serviceHostChannel: make(chan []byte, 100),
		scheduler:          nil,
		conn:               nil,
		sc:                 sc,
		gaiaManager:        nil,
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

	addr, err := s.algo.Source(h, in.Action, s.hosts)
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
	// add gaiaManager
	gm := zroute.NewGaiaManager(s.ctx, conn)
	s.gaiaManager = &gm
	s.gaiaManager.Collect()

	// move configwatcher into ServiceHost
	segChannel := make(chan zroute.Segment)
	sh := zroute.NewServiceHost(s.ctx, wg, conn, segChannel)
	s.hosts = &sh
	s.hosts.Sync()
	// TODO simple scheduler
	sc := zslot.NewSlotCluster(conn)
	s.sc = &sc

	scheduler, err := zscheduler.NewSimpleScheduler(s.ctx, wg, s.conn, 2, s.hosts, &sc, segChannel)
	if err != nil {
		return err
	}
	s.scheduler = &scheduler
	go s.scheduler.Listen(zconst.YurtRoot)

	go func() {
		// here we will sync Algo data about other data
		for {
			select {
			case <-s.ctx.Done() : {
				return
			}
			default:
				{
					fmt.Printf("idles len %v\n", len(s.scheduler.GetIdles()))
					if len(s.scheduler.GetIdles()) > zconst.YurtPoolMaxSize {
						pre := s.scheduler.GetIdle().(string)
						err := s.scheduler.Schedule(pre)
						if err != nil {
							panic(err)
						}
					}

					if len(s.scheduler.GetIdles()) < zconst.YurtPoolMinSize {
						err = s.gaiaManager.Create("", "")
						fmt.Println("create a new idle")
						if err != nil {
							if err == zroute.NOT_ENOUGH_GAIA {
								fmt.Println(err.Error())
							} else {
								panic(err)
							}

						}
					}

					fmt.Printf("just print gaiamanager data %v\n", s.gaiaManager.Monitors)
					time.Sleep(2 * time.Second)
				}
			}
		}
	}()

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

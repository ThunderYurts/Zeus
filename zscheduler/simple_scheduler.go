package zscheduler

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	"math/rand"
	"sync"

	"github.com/ThunderYurts/Zeus/zconst"
	"github.com/ThunderYurts/Zeus/zookeeper"
	"github.com/ThunderYurts/Zeus/zroute"
	"github.com/ThunderYurts/Zeus/zslot"
	"github.com/samuel/go-zookeeper/zk"
)

// PreSlotsManager will manage pre dispatch and sync to
type PreSlotsManager struct {
	lock     *sync.Mutex
	conn     *zk.Conn
	segments []zroute.Segment
}

// NewPreSlotsManager will sync old data first
func NewPreSlotsManager(conn *zk.Conn) (PreSlotsManager, error) {
	exist, _, err := conn.Exists(zconst.PreSlotsRoot)
	if err != nil {
		return PreSlotsManager{}, err
	}
	if !exist {
		segs := []zroute.Segment{}
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)
		err := enc.Encode(segs)
		if err != nil {
			return PreSlotsManager{}, err
		}
		_, err = conn.Create(zconst.PreSlotsRoot, buf.Bytes(), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return PreSlotsManager{}, err
		}
		return PreSlotsManager{
			lock:     &sync.Mutex{},
			conn:     conn,
			segments: segs,
		}, nil
	}
	data, _, err := conn.Get(zconst.PreSlotsRoot)
	if err != nil {
		return PreSlotsManager{}, err
	}
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	segs := []zroute.Segment{}
	err = dec.Decode(&segs)
	if err != nil {
		return PreSlotsManager{}, err
	}
	return PreSlotsManager{
		lock:     &sync.Mutex{},
		conn:     conn,
		segments: segs,
	}, nil
}

// PreDispatch will block new segment
func (psm *PreSlotsManager) PreDispatch() (uint32, uint32, string, error) {
	mid := -1
	end := -1
	serviceName := ""
	psm.lock.Lock()
	defer psm.lock.Unlock()
	for index, seg := range psm.segments {
		fmt.Printf("in predispatch seg %v\n", seg)
		if seg.Block {
			continue
		} else {
			mid = int(seg.Begin+seg.End) / 2
			end = int(seg.End)
			psm.segments[index].End = uint32(mid)
			psm.segments[index].Block = false
			serviceName = seg.ServiceName
			break
		}
	}
	if mid == -1 && end == -1 {
		if len(psm.segments) == 0 {
			// init
			mid = 0
			end = zconst.TotalSlotNum
		} else {
			return 0, 0, "", errors.New("no segment can split")
		}
	}
	newSeg := zroute.Segment{Begin: uint32(mid), End: uint32(end), Block: true, ServiceName: ""}
	fmt.Printf("add new segment %v\n", newSeg)
	psm.segments = append(psm.segments, newSeg)
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(psm.segments)
	if err != nil {
		return 0, 0, "", err
	}
	_, stat, err := psm.conn.Get(zconst.PreSlotsRoot)
	if err != nil {
		return 0, 0, "", err
	}
	stat, err = psm.conn.Set(zconst.PreSlotsRoot, buf.Bytes(), stat.Version)
	if err != nil {
		return 0, 0, "", err
	}
	return uint32(mid), uint32(end), serviceName, nil

}

// Commit will free segment
func (psm *PreSlotsManager) Commit(begin uint32, end uint32, serviceName string) error {
	fmt.Printf("psm commit %v-%v\n", begin, end)
	psm.lock.Lock()
	defer psm.lock.Unlock()
	for i, seg := range psm.segments {
		fmt.Printf("begin: %v %v  == %v\n", begin, seg.Begin, begin == seg.Begin)
		fmt.Printf("begin: %v %v  == %v\n", end, seg.End, end == seg.End)
		if seg.Begin == begin && seg.End == end {
			fmt.Printf("set seg %v block false\n", seg)
			psm.segments[i].Block = false
			psm.segments[i].ServiceName = serviceName
			break
		}
	}
	_, stat, err := psm.conn.Get(zconst.PreSlotsRoot)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err = enc.Encode(psm.segments)
	if err != nil {
		return err
	}
	stat, err = psm.conn.Set(zconst.PreSlotsRoot, buf.Bytes(), stat.Version)
	if err != nil {
		return err
	}
	return nil
}

func randomName(n int, allowedChars ...[]rune) string {
	var defaultLetters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	var letters []rune

	if len(allowedChars) == 0 {
		letters = defaultLetters
	} else {
		letters = allowedChars[0]
	}

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

// GaiaManager will return
// TODO merge two segment

// SimpleScheduler is a simple algo for schedule idle yurt
type SimpleScheduler struct {
	ctx              context.Context
	wg               *sync.WaitGroup
	conn             *zk.Conn
	minRequest       int
	ServiceHost      *zroute.ServiceHost
	sc               *zslot.SlotCluster
	psm              *PreSlotsManager
	segChannel       chan zroute.Segment
	YurtPool         mapset.Set
}

// NewSimpleScheduler is a help function for new SimpleScheduler
func NewSimpleScheduler(ctx context.Context, wg *sync.WaitGroup, conn *zk.Conn, minRequest int, ServiceHost *zroute.ServiceHost, sc *zslot.SlotCluster, segChannel chan zroute.Segment) (SimpleScheduler, error) {
	psm, err := NewPreSlotsManager(conn)
	if err != nil {
		return SimpleScheduler{}, nil
	}
	return SimpleScheduler{
		ctx:              ctx,
		wg:               wg,
		conn:             conn,
		minRequest:       minRequest,
		ServiceHost:      ServiceHost,
		sc:               sc,
		psm:              &psm,
		segChannel:       segChannel,
		YurtPool: mapset.NewSet(),
	}, nil
}

// Schedule will dispatch yurt
func (ss *SimpleScheduler) Schedule(registerNode string) error {
	// check whether node has been dispatched
	preData, _, err := ss.conn.Get(registerNode)
	if err != nil {
		fmt.Println("err in 204")
		return err
	}
	if len(preData) > 0 {
		// has been dispatched
		return nil
	}
	ss.ServiceHost.Lock.RLock()
	fmt.Printf("schedule register node %s \n", registerNode)
	statistic := make(map[string]int)
	for key, value := range ss.ServiceHost.Hosts {
		statistic[key] = len(value)
	}
	ss.ServiceHost.Lock.RUnlock()
	fmt.Printf("statistic value %v\n", statistic)
	for key, value := range statistic {
		if value < ss.minRequest {
			// scheduler to a service for secondary (maybe)
			_, stat, err := ss.conn.Get(registerNode)
			if err != nil {
				fmt.Printf("in scheduler loop1 error : %v", err)
				return err
			}
			regInfo := zookeeper.ZKRegister{ServiceName: key}
			buf := new(bytes.Buffer)
			enc := gob.NewEncoder(buf)
			err = enc.Encode(regInfo)
			if err != nil {
				fmt.Printf("in scheduler loop2 error : %v", err)
				return err
			}
			stat, err = ss.conn.Set(registerNode, buf.Bytes(), stat.Version)

			if err != nil {
				fmt.Printf("in scheduler loop3 error : %v", err)
				return err
			}
			fmt.Printf("node %s is dispatched to %s\n", registerNode, key)
			return nil
		}
	}

	fmt.Printf("node %s is will create a new service\n", registerNode)
	// create new service
	newName := randomName(10)
	//TODO slot split

	begin, end, syncHost, err := ss.psm.PreDispatch()
	if err != nil {
		fmt.Printf("line 233 %v\n", err)
		return err
	}
	fmt.Printf("dispatch segment %v-%v syncHost: %v\n", syncHost, begin, end)
	newHosts := zookeeper.ZKServiceHost{SlotBegin: begin, SlotEnd: end, Service: newName, Primary: "", SecondarySyncHost: "", Secondary: []string{}, SyncHost: syncHost}
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err = enc.Encode(newHosts)
	if err != nil {
		fmt.Printf("line 241 %v\n", err)
		return err
	}
	_, err = ss.conn.Create(zconst.ServiceRoot+"/"+newName, buf.Bytes(), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		fmt.Printf("line 256 %v\n", err)
		return err
	}
	_, err = ss.conn.Create(zconst.ServiceRoot+"/"+newName+zconst.YurtRoot, []byte{}, 0, zk.WorldACL(zk.PermAll))

	if err != nil {
		fmt.Printf("line 262 %v\n", err)
		return err
	}
	_, stat, err := ss.conn.Get(registerNode)
	if err != nil {
		fmt.Printf("line 267 %v\n", err)
		return err
	}
	regInfo := zookeeper.ZKRegister{ServiceName: newName}
	buf = new(bytes.Buffer)
	enc = gob.NewEncoder(buf)
	err = enc.Encode(regInfo)
	stat, err = ss.conn.Set(registerNode, buf.Bytes(), stat.Version)
	if err != nil {
		fmt.Printf("line 276 %v\n", err)
		return err
	}
	fmt.Printf("create service %s\n", newName)
	return nil
}

// Listen will start a routine to ChildrenW now we just let idle into YurtPool
func (ss *SimpleScheduler) Listen(path string) error {
	idles, _, childChan, err := ss.conn.ChildrenW(path)
	fmt.Printf("listen %s start\n", path)
	fmt.Printf("idles %v\n", idles)
	go func() {
		for {
			select {
			case seg, ok := <-ss.segChannel:
				{
					if !ok {
						return
					}
					err := ss.psm.Commit(seg.Begin, seg.End, seg.ServiceName)
					if err != nil {
						fmt.Println(err.Error())
						return
					}
					err = ss.sc.Dispatch(seg.Begin, seg.End, seg.ServiceName)
					if err != nil {
						fmt.Println(err.Error())
						return
					}
				}
			}
		}
	}()
	if err != nil {
		fmt.Printf("in scheduler listen1 error : %v", err)
		return err
	}
	newIdles := mapset.NewSet()
	for _, idle := range idles {
		if data, _, err := ss.conn.Get(path + "/" + idle); err == nil && len(data) == 0 {
			fmt.Printf("dispatch yurt %s\n", idle)
			newIdles.Add(idle)
		}
	}
	ss.YurtPool = newIdles

	ss.wg.Add(1)
	defer ss.wg.Done()
	for {
		select {
		case <-ss.ctx.Done():
			{
				return err
			}
		case e := <-childChan:
			{
				fmt.Printf("simple_scheduler event: %v\n", e)
				if e.Type != zk.EventNodeDeleted {
					idles, _, childChan, err = ss.conn.ChildrenW(path)
					if err != nil {
						fmt.Printf("in scheduler listen2 error : %v", err)
					}
					newIdles := mapset.NewSet()

					for _, idle := range idles {
						fmt.Printf("in simple scheduler idles : %s\n", idle)
						if data, _, err := ss.conn.Get(path + "/" + idle); err == nil && len(data) == 0 {
							newIdles.Add(idle)
						}
					}
					ss.YurtPool = newIdles
				}
			}
		}
	}
}

func (ss *SimpleScheduler) GetIdles() []interface{} {
	return ss.YurtPool.ToSlice()
}

func (ss *SimpleScheduler) GetIdle() interface{} {
	return ss.YurtPool.Pop()
}

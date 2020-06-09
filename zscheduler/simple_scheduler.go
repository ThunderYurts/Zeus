package zscheduler

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"sync"

	"github.com/ThunderYurts/Zeus/zconst"
	"github.com/ThunderYurts/Zeus/zookeeper"
	"github.com/ThunderYurts/Zeus/zroute"
	"github.com/ThunderYurts/Zeus/zslot"
	"github.com/samuel/go-zookeeper/zk"
)

// Segment is present slot segment
type Segment struct {
	Begin int
	End   int
	Block bool
}

// PreSlotsManager will manage pre dispatch and sync to
type PreSlotsManager struct {
	conn     *zk.Conn
	segments []Segment
}

// NewPreSlotsManager will sync old data first
func NewPreSlotsManager(conn *zk.Conn) (PreSlotsManager, error) {
	exist, _, err := conn.Exists(zconst.PreSlotsRoot)
	if err != nil {
		return PreSlotsManager{}, err
	}
	if !exist {
		segs := []Segment{}
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
			conn:     conn,
			segments: segs,
		}, nil
	} else {
		data, _, err := conn.Get(zconst.PreSlotsRoot)
		if err != nil {
			return PreSlotsManager{}, err
		}
		dec := gob.NewDecoder(bytes.NewBuffer(data))
		segs := []Segment{}
		err = dec.Decode(&segs)
		if err != nil {
			return PreSlotsManager{}, err
		}
		return PreSlotsManager{
			conn:     conn,
			segments: segs,
		}, nil
	}
}

// PreDispatch will block new segment
func (psm *PreSlotsManager) PreDispatch() (Segment, error) {
	mid := -1
	end := -1
	for _, seg := range psm.segments {
		if seg.Block {
			continue
		} else {
			mid = (seg.Begin + seg.End) / 2
			end = seg.End
			seg.End = mid
			seg.Block = false
			break
		}
	}
	if mid == -1 && end == -1 {
		if len(psm.segments) == 0 {
			// init
			mid = 0
			end = zconst.TotalSlotNum
		} else {
			return Segment{}, errors.New("no segment can split")
		}
	}
	newSeg := Segment{Begin: mid, End: end, Block: true}
	psm.segments = append(psm.segments, newSeg)
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(psm.segments)
	if err != nil {
		return Segment{}, err
	}
	_, stat, err := psm.conn.Get(zconst.PreSlotsRoot)
	if err != nil {
		return Segment{}, err
	}
	stat, err = psm.conn.Set(zconst.PreSlotsRoot, buf.Bytes(), stat.Version)
	if err != nil {
		return Segment{}, err
	}
	return newSeg, nil

}

// Commit will free segment
func (psm *PreSlotsManager) Commit(begin int, end int) error {
	for _, seg := range psm.segments {
		if seg.Begin == begin && seg.End == end {
			seg.Block = false
			break
		}
	}
	_, stat, err := psm.conn.Get(zconst.PreSlotsRoot)
	if err != nil {
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

// TODO merge two segment

// SimpleScheduler is a simple algo for schedule idle yurt
type SimpleScheduler struct {
	ctx         context.Context
	wg          *sync.WaitGroup
	conn        *zk.Conn
	minRequest  int
	ServiceHost *zroute.ServiceHost
	sc          *zslot.SlotCluster
	psm         *PreSlotsManager
}

// NewSimpleScheduler is a help function for new SimpleScheduler
func NewSimpleScheduler(ctx context.Context, wg *sync.WaitGroup, conn *zk.Conn, minRequest int, ServiceHost *zroute.ServiceHost, sc *zslot.SlotCluster) (SimpleScheduler, error) {
	psm, err := NewPreSlotsManager(conn)
	if err != nil {
		return SimpleScheduler{}, nil
	}
	return SimpleScheduler{
		ctx:         ctx,
		wg:          wg,
		conn:        conn,
		minRequest:  minRequest,
		ServiceHost: ServiceHost,
		sc:          sc,
		psm:         &psm,
	}, nil
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

// Schedule will dispatch yurt
func (ss *SimpleScheduler) Schedule(registerNode string) error {
	fmt.Printf("schedule register node \n")
	ss.ServiceHost.Lock.RLock()
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
			stat, err = ss.conn.Set(registerNode, []byte(key), stat.Version)
			if err != nil {
				fmt.Printf("in scheduler loop2 error : %v", err)
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

	segment, err := ss.psm.PreDispatch()
	if err != nil {
		return err
	}
	newHosts := zookeeper.ZKServiceHost{SlotBegin: segment.Begin, SlotEnd: segment.End, Service: newName, Primary: "", Secondary: []string{}, SyncHost: ""}
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err = enc.Encode(newHosts)
	if err != nil {
		return err
	}
	_, err = ss.conn.Create(zconst.ServiceRoot+"/"+newName, buf.Bytes(), 0, zk.WorldACL(zk.PermAll))
	_, stat, err := ss.conn.Get(registerNode)
	regInfo := zookeeper.ZKRegister{ServiceName: newName}
	buf = new(bytes.Buffer)
	enc = gob.NewEncoder(buf)
	err = enc.Encode(regInfo)
	stat, err = ss.conn.Set(registerNode, buf.Bytes(), stat.Version)
	if err != nil {
		return err
	}
	return nil
}

// Listen will start a routine to ChildrenW
func (ss *SimpleScheduler) Listen(path string) error {
	idles, _, childChan, err := ss.conn.ChildrenW(path)
	fmt.Printf("listen %s start\n", path)
	fmt.Printf("idles %v\n", idles)
	if err != nil {
		fmt.Printf("in scheduler listen1 error : %v", err)
		return err
	}
	for _, idle := range idles {
		ss.Schedule(path + "/" + idle)
	}
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
					for _, idle := range idles {
						ss.Schedule(path + "/" + idle)
					}
				}
			}
		}
	}

}

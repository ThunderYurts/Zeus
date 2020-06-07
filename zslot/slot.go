package zslot

import (
	"bytes"
	"encoding/gob"
	"errors"
	"hash/crc32"
	"sync"

	"github.com/ThunderYurts/Zeus/zconst"
	"github.com/samuel/go-zookeeper/zk"
)

// SlotCluster is a map to return service host
type SlotCluster struct {
	conn  *zk.Conn
	lock  sync.RWMutex
	slots map[uint32]string
}

// NewSlotCluster is a help function, we will try to read from the zk, if it is not existed, we will init it and dispatch all to default service
func NewSlotCluster(conn *zk.Conn, ServiceName string) SlotCluster {
	exist, _, err := conn.Exists(zconst.SlotsRoot)
	if err != nil {
		panic(err)
	}
	config := make(map[uint32]string)
	// here we do not need to worry about race, becase you should be zeus master can reach here
	if exist {
		data, _, err := conn.Get(zconst.SlotsRoot)
		if err != nil {
			panic(err)
		}
		dec := gob.NewDecoder(bytes.NewBuffer(data))
		err = dec.Decode(&config)
	} else {
		// default init
		for i := uint32(0); i < zconst.TotalSlotNum; i = i + 1 {
			config[i] = ServiceName
		}
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)
		err := enc.Encode(config)
		if err != nil {
			panic(err)
		}
		// TODO store stat
		_, err = conn.Create(zconst.SlotsRoot, buf.Bytes(), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			panic(err)
		}
	}
	return SlotCluster{
		conn:  conn,
		lock:  sync.RWMutex{},
		slots: config,
	}
}

// Hash will hash Key and mod TotalSlotsNum, choose a slot and return
func (sc *SlotCluster) Hash(Key string) (string, error) {
	index := crc32.ChecksumIEEE([]byte(Key)) % zconst.TotalSlotNum
	sc.lock.RLock()
	defer sc.lock.RUnlock()
	slot, exist := sc.slots[index]
	if !exist {
		return "", errors.New("slot does not exist")
	}
	return slot, nil
}

//Dispatch will let some slot belong to some service
func (sc *SlotCluster) Dispatch(begin uint32, end uint32, service string) {
	sc.lock.Lock()
	// TODO SET rollback machanism
	defer sc.lock.Unlock()
	for i := begin; i < end; i = i + 1 {
		sc.slots[i] = service
	}
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(sc.slots)
	if err != nil {
		panic(err)
	}
	// TODO STORE VERSION AND PANIC
	_, stat, err := sc.conn.Get(zconst.SlotsRoot)
	if err != nil {
		panic(err)
	}
	_, err = sc.conn.Set(zconst.SlotsRoot, buf.Bytes(), stat.Version)
	if err != nil {
		panic(err)
	}
}

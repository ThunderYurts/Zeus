package zslot

import (
	"errors"
	"hash/crc32"
	"sync"
)

/*
	attention: all the data is from zookeeper in the future
*/
const (
	// TotalSlotsNum is a
	TotalSlotsNum = 11384
)

// Node is a small cluster's primary addr
type Node struct {
	string
}

// Slot is a virtual node a key belong to, a slot must belong to a Node
type Slot struct {
	belongto Node
}

// SlotCluster is a map to return service host
type SlotCluster struct {
	lock  sync.RWMutex
	slots map[uint32]Slot
}

// NewSlotCluster is a help function for
func NewSlotCluster() SlotCluster {
	return SlotCluster{
		lock:  sync.RWMutex{},
		slots: make(map[uint32]Slot),
	}
}

// Hash will hash Key and mod TotalSlotsNum, choose a slot and return
func (sc *SlotCluster) Hash(Key string) (string, error) {
	index := crc32.ChecksumIEEE([]byte(Key))
	sc.lock.RLock()
	defer sc.lock.RUnlock()
	slot, exist := sc.slots[index]
	if !exist {
		return "", errors.New("slot does not exist")
	}
	return slot.belongto.string, nil
}

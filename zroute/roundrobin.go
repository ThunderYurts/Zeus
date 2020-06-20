package zroute

import (
	"errors"
	"github.com/ThunderYurts/Zeus/zconst"
)

var SERVICE_NOT_FOUND = errors.New("service not found")

// RoundRobin is a basic algo for load balance
type RoundRobin struct {
	readMod int
	index   int
}

// NewRoundRobin is a help function for new RoundRobin
func NewRoundRobin() RoundRobin {
	return RoundRobin{readMod: 0, index: 0}
}

// Source func will return yurt addr chosen
func (r *RoundRobin) Source(service string, action string, hosts *ServiceHost) (string, error) {
	hosts.Lock.RLock()
	defer hosts.Lock.RUnlock()
	switch action {
	case zconst.ActionDelete, zconst.ActionPut:
		{
			addr, exist := hosts.PrimaryHosts[service]
			if !exist {
				return "", SERVICE_NOT_FOUND
			}
			return addr, nil

		}
	case zconst.ActionRead:
		{
			addrs, exist := hosts.Hosts[service]
			if !exist {
				return "", SERVICE_NOT_FOUND
			}
			l := len(addrs)
			if l == 0 {
				return "", errors.New("no active host found")
			}
			// should check dynamic hosts add and delete
			r.readMod = l
			r.index = (r.index + 1) % r.readMod
			return addrs[r.index], nil
		}
	default:
		{
			return "", errors.New("invalid action")
		}
	}
}

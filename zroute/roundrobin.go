package zroute

import "errors"

// RoundRobin is a basic algo for load balance
type RoundRobin struct {
	mod   int
	index int
}

// NewRoundRobin is a help function for new RoundRobin
func NewRoundRobin() RoundRobin {
	return RoundRobin{mod: 0, index: 0}
}

// Source func will return yurt addr chosen
func (r *RoundRobin) Source(service string, hosts *ServiceHost) (string, error) {
	hosts.Lock.RLock()
	defer hosts.Lock.RUnlock()
	addrs, exist := hosts.Hosts[service]
	if !exist {
		return "", errors.New("service not found")
	}
	l := len(addrs)
	if l == 0 {
		return "", errors.New("no active host found")
	}
	// should check dynamic hosts add and delete
	r.mod = l
	r.index = (r.index + 1) % r.mod
	return addrs[r.index], nil
}

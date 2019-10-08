/*
@Time : 2019-10-08 16:18
@Author : zr
*/
package service_manager

import (
	"github.com/samuel/go-zookeeper/zk"
)

type Manager struct {
	conn  *zk.Conn
	hosts []string
}

func NewManager(hosts []string) *Manager {
	s := &Manager{}
	s.hosts = hosts

	return s
}

/*
@Time : 2019-10-08 16:28
@Author : zr
*/
package service_manager

import (
	"encoding/json"
	"github.com/samuel/go-zookeeper/zk"
	"math/rand"
	"strings"
	"time"
)

type Consumer struct {
	path    string
	conn    *zk.Conn
	zkHosts []string
}

func NewConsumer(path string, zkHosts []string) *Consumer {
	s := &Consumer{}
	s.zkHosts = zkHosts
	path = strings.Trim(path, "/")
	s.path = strings.Join([]string{prefix, path}, "/")
	return s
}
func (c *Consumer) GetServerList() (list []string, err error) {
	list, _, err = c.conn.Children(c.path)
	return
}

func (c *Consumer) GetServiceHost() (host string, err error) {
	list, err := c.GetServerList()
	if err != nil {
		return
	}
	serviceCount := len(list)
	if serviceCount == 0 {
		err = ErrNoServiceAlive
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	hostPath := list[r.Intn(serviceCount)]

	dataBytes, _, err := c.conn.Get(c.path + "/" + hostPath)

	data := &Data{}

	err = json.Unmarshal(dataBytes, data)
	if err != nil {
		return
	}
	return
}

func (c *Consumer) Open() (err error) {
	conn, _, err := zk.Connect(c.zkHosts, time.Second*5)
	if err != nil {
		return
	}
	c.conn = conn

	exist, _, err := c.conn.Exists(c.path)
	if err != nil {
		return
	}
	if !exist {
		err = ErrNoService
		return
	}
	return
}

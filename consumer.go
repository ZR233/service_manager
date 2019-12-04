/*
@Time : 2019-10-08 16:28
@Author : zr
*/
package service_manager

import (
	"context"
	"encoding/json"
	"github.com/samuel/go-zookeeper/zk"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type state int

const (
	stateDisconnected state = iota
	stateConnecting
	stateConnected
	stateDisconnecting
	stateClosing
	stateClosed
)

type Consumer struct {
	path         string
	conn         *zk.Conn
	zkHosts      []string
	aliveHosts   []string
	aliveHostsMu sync.Mutex
	ctx          context.Context
	stop         context.CancelFunc
	stateMu      sync.Mutex
	state        state
}

func NewConsumer(path string, zkHosts []string) *Consumer {
	s := &Consumer{}
	s.zkHosts = zkHosts
	path = strings.Trim(path, "/")
	s.path = strings.Join([]string{prefix, path}, "/")
	s.state = stateDisconnected
	s.ctx, s.stop = context.WithCancel(context.Background())
	return s
}

func (c *Consumer) Close() {
	c.stateMu.Lock()
	if c.state >= stateClosing {
		c.stateMu.Unlock()
		return
	}
	c.state = stateClosing
	c.stateMu.Unlock()

	c.stop()
	c.conn.Close()

	c.state = stateClosed
}

func (c *Consumer) updateServerList() (err error) {
	list, _, err := c.conn.Children(c.path)
	if err != nil {
		return
	}

	c.aliveHostsMu.Lock()
	defer c.aliveHostsMu.Unlock()

	c.aliveHosts = nil
	for _, v := range list {
		var dataBytes []byte
		dataBytes, _, err = c.conn.Get(c.path + "/" + v)

		data := &NodeInfo{}
		err = json.Unmarshal(dataBytes, data)
		if err != nil {
			return
		}
		c.aliveHosts = append(c.aliveHosts, data.Host)
	}
	return
}

func (c *Consumer) GetServiceHost() (host string, err error) {
	c.aliveHostsMu.Lock()
	defer c.aliveHostsMu.Unlock()

	serviceCount := len(c.aliveHosts)
	if serviceCount == 0 {
		err = ErrNoServiceAlive
		return
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	host = c.aliveHosts[r.Intn(serviceCount)]
	return
}

func (c *Consumer) Open() (err error) {

	c.stateMu.Lock()
	if c.state == stateConnecting ||
		c.state == stateConnected ||
		c.state == stateDisconnecting {

		c.stateMu.Unlock()
		return
	}
	c.state = stateConnecting
	c.stateMu.Unlock()

	conn, _, err := zk.Connect(c.zkHosts, time.Second*5)
	if err != nil {
		return
	}
	c.state = stateConnected
	c.conn = conn

	exist, _, err := c.conn.Exists(c.path)
	if err != nil {
		return
	}
	if !exist {
		err = ErrNoService
		return
	}

	err = c.updateServerList()
	if err != nil {
		return
	}

	return
}

func (c *Consumer) updateLoop() {
	for {
		if c.state >= stateClosing {
			return
		}
		_, _, events, err := c.conn.ChildrenW(c.path)
		if err != nil {
			time.Sleep(time.Millisecond * 500)
			continue
		}
		select {
		case <-c.ctx.Done():
			return
		case <-events:
		_:
			c.updateServerList()
		}
	}
}

func (c *Consumer) Run() {
	err := c.Open()
	if err != nil {
		panic(err)
	}

	go c.updateLoop()

	return
}

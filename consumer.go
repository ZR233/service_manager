/*
@Time : 2019-10-08 16:28
@Author : zr
*/
package service_manager

import "C"
import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	pool "github.com/ZR233/goutils/conn_pool"
	"github.com/samuel/go-zookeeper/zk"
	"io"
	"sync"
	"time"
)

//type status int
//
//const (
//	stateInit status = iota
//	stateConnecting
//	stateZkCheckReady
//	stateConnected
//	stateDisconnecting
//	stateClosing
//	stateClosed
//)

type ConnFactory func(host string) (io.Closer, error)

type Consumer struct {
	manager *Manager
	// host-conn
	connPools   map[string]*pool.Pool
	aliveHosts  []string
	connHostMap map[io.Closer]string

	connMax      int
	connMin      int
	connFactory  ConnFactory
	connTestFunc pool.ConnTestFunc

	ctx    context.Context
	cancel context.CancelFunc
	sync.Mutex
	status  Status
	service *Service

	nodeWatchCancel []context.CancelFunc
	funcChan        chan func() error

	hostIter int
}

func (c *Consumer) release() {

}

func (c *Consumer) Close() {
	c.cancel()
}

func (c *Consumer) debug(args ...interface{}) {
	c.manager.print("Debug", "[consumer]"+c.service.Path, args...)
}
func (c *Consumer) info(args ...interface{}) {
	c.manager.print("Info", "[consumer]"+c.service.Path, args...)
}
func (c *Consumer) warn(args ...interface{}) {
	c.manager.print("Warn", "[consumer]"+c.service.Path, args...)
}
func (c *Consumer) panic(args ...interface{}) {
	c.manager.print("Panic", "[consumer]"+c.service.Path, args...)
}

func (c *Consumer) watchService() (err error) {
	data, _, eventChan, err := c.manager.conn.GetW(c.service.Path)
	if err != nil {
		return
	}

	select {
	case event_ := <-eventChan:
		if event_.Type != zk.EventNodeDataChanged {
			return
		}
		err = c.updateService(data, false)

		go c.connUpdateLoop()

	case <-c.ctx.Done():
		return
	}
	return
}

func (c *Consumer) updateService(data []byte, force bool) (err error) {
	oldData, err_ := json.Marshal(c.service)
	if err_ != nil {
		err = fmt.Errorf("get service info error\n%w", err_)
		return
	}
	if bytes.Equal(oldData, data) && force == false {
		return
	}

	c.Lock()
	defer c.Unlock()

	for _, f := range c.nodeWatchCancel {
		f()
	}

	err = json.Unmarshal(data, c.service)
	if err != nil {
		err = fmt.Errorf("get service info error\n%w", err)
		return
	}
	msg := fmt.Sprintf("Service Info Updated\nprotocol:%s\ntype:%s", c.service.Protocol, c.service.Type)

	c.info(msg)
	if c.connFactory == nil {
		switch c.service.Protocol {
		case ProtocolGRPC:
			c.connFactory = GRpcFactory()
			c.connTestFunc = GRpcConnTest()
		}
	}

	return
}

func (c *Consumer) getService() (err error) {
	data, _, err := c.manager.conn.Get(c.service.Path)
	if err != nil {
		return
	}
	err = c.updateService(data, true)
	return
}

func (c *Consumer) updateNode(nodeChangePre bool) (eventChan <-chan zk.Event, err error) {
	c.Lock()
	defer c.Unlock()
	c.aliveHosts, _, eventChan, err = c.manager.conn.ChildrenW(c.service.Path)
	if err != nil {
		return
	}
	c.hostIter = 0
	if nodeChangePre {
		mapNode := map[string]bool{}

		msg := "----node change----\n"
		for _, node := range c.aliveHosts {
			msg += node + "\n"
			mapNode[node] = true
		}
		msg += "----node change----\n"
		c.info(msg)
		//defer func() {
		//	recover()
		//}()

		// 关闭缺少的
		for host, connPool := range c.connPools {
			if _, ok := mapNode[host]; !ok {
				_ = connPool.Shutdown()
			}
		}

		// 创建新增的
		for host := range mapNode {
			if _, ok := c.connPools[host]; !ok {
				c.connPools[host], err = pool.NewPool(
					c.poolConnFactory(host),
					c.poolErrorHandler,
					c.connTestFunc,
					pool.OptionMaxOpen(c.connMax),
					pool.OptionMinOpen(c.connMin),
				)
				if err != nil {
					c.panic("create conn pool error\n", err)
				}
			}
		}
	}

	return
}

func (c *Consumer) poolErrorHandler(err error) {
	if err != nil {
		c.panic("create conn error\n", err)
	}
}
func (c *Consumer) poolConnFactory(host string) pool.ConnFactory {
	return func() (closer io.Closer, err error) {
		closer, err = c.connFactory(host)
		return
	}
}

func (c *Consumer) GetConn() (conn io.Closer, err error) {
	c.Lock()
	defer c.Unlock()
	if len(c.connPools) < 1 {
		err = ErrNoServiceAlive
		return
	}

	if c.hostIter > len(c.aliveHosts)-1 {
		c.hostIter = 0
	}
	host := c.aliveHosts[c.hostIter]
	conn, err = c.connPools[host].Acquire()
	c.connHostMap[conn] = host

	return
}
func (c *Consumer) ReleaseConn(conn io.Closer) {
	c.Lock()
	defer c.Unlock()
	defer func() {
		recover()
	}()
	host := c.connHostMap[conn]

	if v, ok := c.connPools[host]; ok {
		v.Release(conn)
	} else {
		_ = conn.Close()
	}
}

func (c *Consumer) watchNodes(nodeChangePre bool) (nodeChange bool, err error) {
	eventChan, err := c.updateNode(nodeChangePre)
	if err != nil {
		return
	}

	select {
	case event := <-eventChan:
		nodeChange = event.Type == zk.EventNodeChildrenChanged
	case <-c.ctx.Done():
		return
	}
	return
}
func (c *Consumer) cleanConnPools() {
	for _, conn := range c.connPools {
		_ = conn.Shutdown()
	}
	c.connPools = map[string]*pool.Pool{}
	c.aliveHosts = nil
}

func (c *Consumer) connUpdateLoop() {
	defer c.debug("connUpdateLoop stop")

	c.debug("close all connections")
	c.cleanConnPools()

	ctx, cancel := context.WithCancel(c.ctx)
	c.nodeWatchCancel = append(c.nodeWatchCancel, cancel)
	nodeChangePre := true
	var err error
	c.debug("watch nodes")
	for {
		select {
		case <-ctx.Done():
			return

		default:
			nodeChangePre, err = c.watchNodes(nodeChangePre)
			if err != nil {
				c.panic("conn update error:\n", err)
			}
		}
	}
}

func (c *Consumer) serviceUpdateLoop() {
	defer c.debug("serviceUpdateLoop stop")
	c.debug("watch service")
	for {
		if c.status >= StatusStopping {
			return
		}

		select {
		case <-c.ctx.Done():
			return

		default:
			err := c.watchService()
			if err != nil {
				c.panic("service info error:\n", err)
			}
		}
	}

}

func (c *Consumer) funcDealerLoop() {
	defer c.debug("funcDealerLoop stop")

	for {
		if c.status >= StatusStopping {
			return
		}

		if c.status < StatusOpenSuccess {
			time.Sleep(time.Millisecond * 20)
			continue
		}

		select {
		case <-c.ctx.Done():
			return
		case f := <-c.funcChan:
			if f == nil {
				continue
			}
			err := f()
			if err != nil {
				c.warn(err)
			}
		}
	}
}

func (c *Consumer) Open() (err error) {

	err = c.getService()
	if err != nil {
		err = fmt.Errorf("Consumer Open error:\n%s\n%w", c.service.Path, err)
		return
	}
	c.status = StatusOpenSuccess
	go c.funcDealerLoop()

	go func() {
		<-c.ctx.Done()
		c.release()
	}()

	return
}

func (c *Consumer) getEventCallback() zk.EventCallback {
	return func(event zk.Event) {
		if event.Type == zk.EventSession && event.State == zk.StateHasSession {
			c.info("zk login success")
			defer func() {
				recover()
			}()

			c.funcChan <- func() error {
				go c.connUpdateLoop()
				go c.serviceUpdateLoop()
				return nil
			}
		}
	}
}

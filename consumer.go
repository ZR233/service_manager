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
	"github.com/samuel/go-zookeeper/zk"
	"net"
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

type ConnFactory func(host string) (net.Conn, error)

type Consumer struct {
	manager *Manager
	// host-conn
	conns  map[string]net.Conn
	ctx    context.Context
	cancel context.CancelFunc
	sync.Mutex
	status          Status
	service         *Service
	connFactory     ConnFactory
	nodeWatchCancel []context.CancelFunc
	funcChan        chan func() error
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

		oldData, err_ := json.Marshal(c.service)
		if err_ != nil {
			err = fmt.Errorf("get service info error\n%w", err_)
			return
		}
		if bytes.Equal(oldData, data) {
			return
		}

		c.Lock()
		defer c.Unlock()

		for _, f := range c.nodeWatchCancel {
			f()
		}

		c.debug(event_.Type)
		err = json.Unmarshal(data, c.service)
		if err != nil {
			err = fmt.Errorf("get service info error\n%w", err)
			return
		}
		msg := fmt.Sprintf("Service Info Updated\nprotocol:%s\ntype:%s", c.service.Protocol, c.service.Type)

		c.info(msg)
		//err = c.getNodes()
		//if err != nil {
		//	err = fmt.Errorf("get nodes error\n%w", err)
		//	return
		//}
		go c.connUpdateLoop()

	case <-c.ctx.Done():
		return
	}
	return
}

func (c *Consumer) GetService() (err error) {
	data, _, err := c.manager.conn.Get(c.service.Path)
	if err != nil {
		return
	}

	err = json.Unmarshal(data, c.service)
	if err != nil {
		err = fmt.Errorf("get service info error\n%w", err)
		return
	}
	msg := fmt.Sprintf("Service Info Updated\nprotocol:%s\ntype:%s", c.service.Protocol, c.service.Type)

	c.info(msg)

	return
}

func (c *Consumer) getNodes() (err error) {
	list, _, err := c.manager.conn.Children(c.service.Path)
	if err != nil {
		return
	}

	c.Lock()
	defer c.Unlock()

	msg := "----node change----\n"
	for _, node := range list {
		msg += node + "\n"
	}
	msg += "----node change----\n"

	c.info(msg)

	return
}
func (c *Consumer) updateNode(nodeChangePre bool) (eventChan <-chan zk.Event, err error) {
	c.Lock()
	defer c.Unlock()
	list, _, eventChan, err := c.manager.conn.ChildrenW(c.service.Path)
	if err != nil {
		return
	}
	if nodeChangePre {
		mapNode := map[string]bool{}

		msg := "----node change----\n"
		for _, node := range list {
			msg += node + "\n"
			mapNode[node] = true
		}
		msg += "----node change----\n"
		c.info(msg)
		defer func() {
			recover()
		}()

		// 关闭缺少的
		for host, conn := range c.conns {
			if _, ok := mapNode[host]; !ok {
				_ = conn.Close()
			}
		}

		// 创建新增的
		for host := range mapNode {
			if _, ok := c.conns[host]; !ok {
				c.conns[host], err = c.connFactory(host)
				if err != nil {
					c.warn("create conn error\n", err)
				}
			}
		}
	}

	return
}

func (c *Consumer) GetConn() (conn net.Conn, err error) {
	c.Lock()
	defer c.Unlock()

	return
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

func (c *Consumer) connUpdateLoop() {
	defer c.debug("connUpdateLoop stop")
	c.debug("watch nodes")
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	ctx, cancel := context.WithCancel(c.ctx)
	c.nodeWatchCancel = append(c.nodeWatchCancel, cancel)
	nodeChangePre := true
	var err error
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

func (c *Consumer) zkOnConnected() (err error) {

	for _, conn := range c.conns {
		_ = conn.Close()
	}
	c.conns = make(map[string]net.Conn)
	err = c.GetService()
	c.status = StatusOpenSuccess

	return
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

	err = c.GetService()
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

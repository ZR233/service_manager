/*
@Time : 2019-10-08 16:28
@Author : zr
*/
package service_manager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	pool "github.com/ZR233/goutils/conn_pool"
	"github.com/samuel/go-zookeeper/zk"
	"io"
)

type ConnFactory func(host string) (io.Closer, error)

type Consumer struct {
	producerConsumerBase

	// host-conn
	connPool   *pool.Pool
	aliveHosts []string
	//connHostMap map[io.Closer]string

	connMax      int
	connMin      int
	connFactory  ConnFactory
	connTestFunc pool.ConnTestFunc

	serviceCtx    context.Context
	serviceCancel context.CancelFunc

	nodeWatchCancel []context.CancelFunc
	hostIter        int
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
		c.ready("Consumer", err)
		return
	}
	if c.status < StatusReady {
		err = c.updateService(data, true)
		c.ready("Consumer", err)
		if err != nil {
			return
		}
		go c.connUpdateLoop()
	}
	stopChan := c.zkCtx.Done()
	select {
	case <-stopChan:
		return

	case event_ := <-eventChan:
		if event_.Type != zk.EventNodeDataChanged {
			return
		}
		err = c.updateService(data, false)
		if err != nil {
			return
		}

		go c.connUpdateLoop()
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

//func (c *Consumer) getService() (err error) {
//	data, _, err := c.manager.conn.Get(c.service.Path)
//	if err != nil {
//		return
//	}
//	err = c.updateService(data, true)
//	return
//}

func (c *Consumer) updateNode(nodeChangePre bool) (eventChan <-chan zk.Event, err error) {
	c.debug("watch children")
	aliveHosts, _, eventChan, err := c.manager.conn.ChildrenW(c.service.Path)
	if err != nil {
		return
	}

	msg := "----node----\n"
	for _, node := range aliveHosts {
		msg += node + "\n"
	}
	msg += "----node----\n"
	c.debug(msg)
	c.hostIter = 0

	c.Lock()
	defer c.Unlock()
	c.aliveHosts = aliveHosts

	if nodeChangePre {

		//mapNode := map[string]bool{}
		//
		//msg := "----node change----\n"
		//for _, node := range c.aliveHosts {
		//	msg += node + "\n"
		//	mapNode[node] = true
		//}
		//msg += "----node change----\n"
		//c.info(msg)
		//defer func() {
		//	recover()
		//}()

		// 关闭缺少的
		//for host, connPool := range c.connPools {
		//	if _, ok := mapNode[host]; !ok {
		//		_ = connPool.Close()
		//	}
		//}

		// 创建新增的
		//for host := range mapNode {
		//	if _, ok := c.connPools[host]; !ok {
		//		c.connPools[host], err = pool.NewPool(
		//			c.poolConnFactory(host),
		//			c.poolErrorHandler,
		//			c.connTestFunc,
		//			pool.OptionMaxOpen(c.connMax),
		//			pool.OptionMinOpen(c.connMin),
		//		)
		//		if err != nil {
		//			c.panic("create conn pool error\n", err)
		//		}
		//	}
		//}
	}

	return
}

func (c *Consumer) poolErrorHandler(err error) {
	if err != nil {
		c.manager.logger.Error(c.service.Path, "\ncreate conn error\n", err)
	}
}
func (c *Consumer) getOneHost() (host string, err error) {
	c.Lock()
	defer c.Unlock()
	if len(c.aliveHosts) < 1 {
		err = ErrNoServiceAlive
		return
	}
	if c.hostIter > len(c.aliveHosts)-1 {
		c.hostIter = 0
	}
	host = c.aliveHosts[c.hostIter]
	c.hostIter++

	return
}

func (c *Consumer) poolConnFactory() pool.ConnFactory {
	return func() (closer io.Closer, err error) {
		host, err := c.getOneHost()
		if err != nil {
			return
		}

		closer, err = c.connFactory(host)
		c.manager.logger.Debug("create connection: ", host)
		return
	}
}

func (c *Consumer) GetConn() (conn io.Closer, err error) {

	conn, err = c.connPool.Acquire()
	return
}
func (c *Consumer) ReleaseConn(conn io.Closer) {
	defer func() {
		recover()
	}()

	c.connPool.Release(conn)
}

func (c *Consumer) watchNodes(nodeChangePre bool) (nodeChange bool, err error) {
	c.debug("watch nodes")
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

	c.aliveHosts = nil
}

func (c *Consumer) serviceStop() {
	defer func() {
		recover()
	}()
	c.serviceCancel()
}

func (c *Consumer) connUpdateLoop() {
	defer c.debug("connUpdateLoop stop")

	c.debug("close all connections")
	c.cleanConnPools()

	c.serviceStop()
	c.serviceCtx, c.serviceCancel = context.WithCancel(c.zkCtx)

	nodeChangePre := true
	var err error
	stopChan := c.serviceCtx.Done()
	for {
		if c.status >= StatusStopping {
			return
		}

		select {
		case <-stopChan:
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

	stopChan := c.zkCtx.Done()
	for {
		if c.status >= StatusStopping {
			return
		}

		select {
		case <-stopChan:
			return

		default:
			err := c.watchService()
			if err != nil {
				c.panic("service info error:\n", err)
			}

		}
	}
}

func (c *Consumer) Open() (err error) {

	c.status = StatusOpenSuccess
	go c.funcDealerLoop("Consumer")

	go func() {
		<-c.ctx.Done()
		c.release()
	}()

	return
}

func (c *Consumer) getEventCallback() zk.EventCallback {
	return c.onZkConnect("Consumer", func() error {

		//go c.connUpdateLoop()
		go c.serviceUpdateLoop()

		return nil
	})
}

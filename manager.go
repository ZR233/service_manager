/*
@Time : 2019-10-08 16:18
@Author : zr
*/
package service_manager

import (
	"context"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"os"
	"path"
	"time"
)

type Manager struct {
	conn      *zk.Conn
	hosts     []string
	logger    Logger
	producers []*Producer
	consumers []*Consumer

	ctx    context.Context
	cancel context.CancelFunc
}

type ServiceOptionProtocol Protocol

func (o ServiceOptionProtocol) set(s *Service) {
	s.Protocol = Protocol(o)
}

type ServiceOptionServiceType ServiceType

func (o ServiceOptionServiceType) set(s *Service) {
	s.Type = ServiceType(o)
}

type ServiceOption interface {
	set(s *Service)
}

func (m *Manager) NewService(sPath string, options ...ServiceOption) *Service {
	s := &Service{}

	s.Path = path.Join(prefix, sPath)
	for _, option := range options {
		option.set(s)
	}
	return s
}

type ConsumerOptionConnFactory ConnFactory

func (o ConsumerOptionConnFactory) set(s *Consumer) {
	s.connFactory = ConnFactory(o)
}

type ConsumerOption interface {
	set(c *Consumer)
}

func (m *Manager) NewConsumer(service *Service, options ...ConsumerOption) *Consumer {
	c := &Consumer{}
	for _, v := range options {
		v.set(c)
	}
	c.manager = m
	m.consumers = append(m.consumers, c)
	c.service = service
	c.ctx, c.cancel = context.WithCancel(m.ctx)
	c.funcChan = make(chan func() error, 5)
	return c
}

type ProducerOption interface {
	set(s *Producer)
}

// ipv6 地址 格式为 [::1234]
type OptionHost string

func (o OptionHost) set(s *Producer) {
	s.Host = string(o)
}

func (m *Manager) NewProducer(service *Service, port int, options ...ProducerOption) (producer *Producer, err error) {
	producer = &Producer{}
	producer.Port = port
	producer.service = service
	producer.manager = m
	for _, v := range options {
		v.set(producer)
	}

	producer.HostName, err = os.Hostname()
	if err != nil {
		if producer.Host == "" {
			err = fmt.Errorf("host is empty and can't get hostname:\n%w", err)
			return
		}
		err = nil
	}
	m.producers = append(m.producers, producer)

	if producer.Host == "" {
		producer.Host = producer.HostName
	}

	producer.funcChan = make(chan func() error, 5)
	producer.ctx, producer.cancel = context.WithCancel(m.ctx)

	return
}

func (m *Manager) release() {
	m.conn.Close()
}

func (m *Manager) Close() error {
	m.cancel()
	return nil
}
func (m *Manager) SetLogger(logger Logger) {
	m.logger = logger
}

func (m *Manager) RunSync() (err error) {

	cb := func(event zk.Event) {
		for _, v := range m.producers {
			f := v.getEventCallback()
			f(event)
		}
		for _, v := range m.consumers {
			f := v.getEventCallback()
			f(event)
		}

	}
	option := zk.WithEventCallback(cb)
	m.conn, _, err = zk.Connect(m.hosts, time.Second*5,
		zk.WithLogger(zkLogger{m.logger}),
		option)
	if err != nil {
		return
	}

	for _, v := range m.producers {
		err = v.Open()
		if err != nil {
			err = fmt.Errorf("producer open fail\n%w", err)
			return
		}
	}
	for _, v := range m.consumers {
		err = v.Open()
		if err != nil {
			err = fmt.Errorf("consumer open fail\n%w", err)
			return
		}
	}
	return
}

func (m *Manager) print(level, prefix string, args ...interface{}) {
	a := []interface{}{
		prefix + "\n",
	}
	a = append(a, args...)
	switch level {
	case "Debug":
		m.logger.Debug(a...)
	case "Info":
		m.logger.Info(a...)
	case "Warn":
		m.logger.Warn(a...)
	case "Panic":
		m.logger.Panic(a...)
	}
}

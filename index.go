/*
@Time : 2019-10-08 11:06
@Author : zr
*/
package service_manager

import (
	"context"
	"fmt"

	"os"
	"path"
)

const prefix = "/service"

type Option interface {
	set(s *Service)
}

// ipv6 地址 格式为 [::1234]
type OptionHost string

func (o OptionHost) set(s *Service) {
	s.Host = string(o)
}

type OptionProtocol Protocol

func (o OptionProtocol) set(s *Service) {
	s.Protocol = Protocol(o)
}

type OptionServiceType ServiceType

func (o OptionServiceType) set(s *Service) {
	s.Type = ServiceType(o)
}

func NewService(servicePath string, port int, zkHosts []string, options ...Option) (service *Service, err error) {
	service = &Service{}
	service.Port = port
	service.path = path.Join(prefix, servicePath)
	for _, v := range options {
		v.set(service)
	}

	service.HostName, err = os.Hostname()
	if err != nil {
		if service.Host == "" {
			err = fmt.Errorf("host is empty and can't get hostname:\n%w", err)
			return
		}
		err = nil
	}

	if service.Host == "" {
		service.Host = service.HostName
	}

	service.zkHosts = zkHosts

	service.SetLogger(defaultLogger{})
	service.funcChan = make(chan func() error, 5)
	service.ctx, service.cancel = context.WithCancel(context.Background())
	return
}

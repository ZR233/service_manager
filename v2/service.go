package service

import (
	"fmt"
	"github.com/hashicorp/consul/api"
	"net/http"
	"os"
)

type Check interface {
	Exec(reg *api.AgentServiceRegistration)
}
type CheckHttp struct {
}
type CheckWithNoServer struct {
	options *Options
}

func (o *Options) CheckWithNoServer() {
	c := &CheckWithNoServer{options: o}
	o.Check = c
}

func (c *CheckWithNoServer) Exec(reg *api.AgentServiceRegistration) {
	reg.Check = &api.AgentServiceCheck{ // 健康检查
		Interval:                       "5s", // 健康检查间隔
		HTTP:                           fmt.Sprintf("http://%s:%d/%s", "127.0.0.1", c.options.Port, "v1.Health/Check"),
		DeregisterCriticalServiceAfter: "10s", // 注销时间，相当于过期时间
	}
	http.HandleFunc("/v1.Health/Check", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("success"))
	})
	go func() {
		err := http.ListenAndServe(fmt.Sprintf(":%d", c.options.Port), nil)
		if err != nil {
			panic(err)
		}
	}()
}

type RunType int

const (
	RunModelNormal RunType = iota
	RunModelSingle
)

type Options struct {
	Name         string
	Host         string
	Port         int
	ConsulConfig *api.Config
	Check        Check
	RunType      RunType
}

func NewOptions() *Options {
	o := &Options{
		ConsulConfig: api.DefaultConfig(),
	}
	o.ConsulConfig.Address = "127.0.0.1:8500"

	return o
}

type Service struct {
	Id           string
	options      *Options
	consulClient *api.Client
}

func (s *Service) Close() error {
	s.consulClient.Agent().ServiceDeregister(s.Id)
	return nil
}
func (s *Service) checkSingle() {
	list, _, err := s.consulClient.Health().Service(s.options.Name, "", true, nil)
	if err != nil {
		panic(err)
	}
	if len(list) > 0 {
		msg := "single service already running on: "
		for _, v := range list {
			msg += fmt.Sprintf("\n%s", v.Node.Node)
		}
		msg += "\n"
		err = fmt.Errorf("%s", msg)
		panic(err)
	}
}
func (s *Service) start() {
	client, err := api.NewClient(s.options.ConsulConfig)
	if err != nil {
		panic(err)
	}
	s.consulClient = client
	agent := client.Agent()

	if s.options.RunType == RunModelSingle {
		s.checkSingle()
	}

	hostname, _ := os.Hostname()
	s.Id = fmt.Sprintf("%v-%v", s.options.Name, hostname)

	reg := &api.AgentServiceRegistration{
		ID:   s.Id,           // 服务节点的名称
		Name: s.options.Name, // 服务名称
		Port: s.options.Port, // 服务端口
	}

	if s.options.Check == nil {
		s.options.CheckWithNoServer()
	}
	s.options.Check.Exec(reg)

	if err := agent.ServiceRegister(reg); err != nil {
		panic(err)
	}
}

func NewService(options *Options) (s *Service, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%s", p)
		}
	}()

	if options.Port <= 0 {
		panic(fmt.Errorf("service port error: %d", options.Port))
	}

	s = &Service{
		options: options,
	}
	s.start()
	return
}

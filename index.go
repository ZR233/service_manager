/*
@Time : 2019-10-08 11:06
@Author : zr
*/
package service_manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"os"
	"strings"
	"time"
)

const prefix = "/service"

var (
	ErrNoService          = errors.New("no service")
	ErrNoServiceAlive     = errors.New("no service alive")
	ErrConnectToZooKeeper = zk.ErrNoServer
	ErrInvalidPath        = zk.ErrInvalidPath
)

type Data struct {
	Pid      int
	Host     string
	HostName string
}

type Service struct {
	host     string
	path     string
	conn     *zk.Conn
	zkHosts  []string
	pathReal string
	logger   Logger
	stopFlag bool
	funcChan chan func()
}

func (s *Service) SetLogger(logger Logger) {
	s.logger = logger
}

func NewService(path, host string, zkHosts []string) *Service {
	s := &Service{}
	path = strings.Trim(path, "/")
	s.path = strings.Join([]string{prefix, path}, "/")
	s.zkHosts = zkHosts
	s.host = host
	s.SetLogger(defaultLogger{})
	s.funcChan = make(chan func(), 5)
	go func() {
		for {
			if f, ok := <-s.funcChan; ok {
				f()
			} else {
				return
			}
		}
	}()
	return s
}

func (s *Service) connect() (event <-chan zk.Event, err error) {
	option := zk.WithEventCallback(s.eventCallback)
	s.conn, event, err = zk.Connect(s.zkHosts, time.Second*5,
		zk.WithLogger(zkLogger{s.logger}),
		option)
	if err != nil {
		return
	}
	return
}

func (s *Service) eventCallback(event zk.Event) {
	if event.Type == zk.EventSession && event.State == zk.StateHasSession {
		s.logger.Info("register service")
		defer func() {
			recover()
		}()
		s.funcChan <- func() {
			err := s.register()
			if err != nil {
				s.logger.Warn(err.Error())
				return
			}
		}
	}
}

func (s *Service) register() (err error) {
	hostName, err := os.Hostname()
	host := hostName

	hostArr := strings.Split(s.host, ":")
	if hostArr[0] != "" {
		host = s.host
	}
	if len(hostArr) > 1 {
		host = host + ":" + hostArr[1]
	}

	dataStruct := Data{Pid: os.Getpid()}
	dataStruct.Host = host
	dataStruct.HostName = hostName
	data, err := json.Marshal(dataStruct)
	if err != nil {
		return
	}
	pathName := s.path + "/" + host
	ok := false
	if ok, _, err = s.conn.Exists(pathName); err != nil {
		return
	} else {
		if !ok {
			s.pathReal, err = s.conn.Create(pathName, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
			if err != nil {
				return
			}
		}
	}
	return
}

func (s *Service) Close() {
	defer func() {
		recover()
	}()
	s.stopFlag = true
	close(s.funcChan)
	s.conn.Close()
	return
}
func (s *Service) registerLoop() {
	defer func() {
		if e := recover(); e != nil {
			s.logger.Warn(fmt.Sprintf("%s", e))
		}
	}()

	if s.conn.State() == zk.StateHasSession {
		err := s.register()
		if err != nil {
			s.logger.Warn(err.Error())
		}
	}
	time.Sleep(time.Second)
}

func (s *Service) Open() (err error) {
	_, err = s.connect()
	if err != nil {
		return
	}
	path := strings.TrimLeft(s.path, "/")
	pathSlice := strings.Split(path, "/")
	path = ""
	pathLayLen := len(pathSlice)
	for i := 0; i < pathLayLen; i++ {
		path += "/" + pathSlice[i]
		exist := false
		exist, _, err = s.conn.Exists(path)
		if err != nil {
			return
		}
		if !exist {
			// permission
			var acls = zk.WorldACL(zk.PermAll)
			// create
			var flags int32 = 0

			_, err = s.conn.Create(path, []byte(""), flags, acls)
			if err != nil {
				return
			}
		}
	}

	return
}

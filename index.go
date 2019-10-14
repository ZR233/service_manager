/*
@Time : 2019-10-08 11:06
@Author : zr
*/
package service_manager

import (
	"encoding/json"
	"errors"
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

	return s
}

func (s *Service) tryToConnect() (event <-chan zk.Event) {
	for {
		var err error
		event, err = s.connect()
		if err != nil {
			time.Sleep(time.Second)
			s.logger.Warn(err.Error())
		} else {
			return
		}
	}
}

func (s *Service) Open() (err error) {
	event := s.tryToConnect()

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

	host, err := os.Hostname()

	dataStruct := Data{Pid: os.Getpid()}
	dataStruct.Host = s.host

	dataStruct.HostName = host
	data, err := json.Marshal(dataStruct)
	if err != nil {
		return
	}

	s.pathReal, err = s.conn.CreateProtectedEphemeralSequential(s.path+"/node", data, zk.WorldACL(zk.PermAll))
	if err != nil {
		return
	}

	// 短线重连
	go func(event <-chan zk.Event) {
		for {
			e := <-event
			if e.Type == zk.EventSession && e.State == zk.StateExpired {
				s.logger.Warn("zk reconnect")
				s.conn.Close()
				event = s.tryToConnect()
			}
		}

	}(event)

	return
}

func (s *Service) connect() (event <-chan zk.Event, err error) {
	s.conn, event, err = zk.Connect(s.zkHosts, time.Second*5,
		zk.WithLogger(zkLogger{s.logger}))
	if err != nil {
		return
	}
	return
}

func (s Service) Close() {
	defer func() {
		recover()
	}()
	s.conn.Close()
	return
}

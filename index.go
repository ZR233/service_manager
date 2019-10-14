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

func (s *Service) Open() (err error) {
	conn, event, err := zk.Connect(s.zkHosts, time.Second*5,
		zk.WithLogger(zkLogger{s.logger}))
	if err != nil {
		return
	}
	s.conn = conn

	go func() {
		for {
			e := <-event
			if e.Type == zk.EventSession && e.State == zk.StateExpired {
				s.logger.Warn("zk reconnect")
				conn.Close()
				for {
					err = s.Open()
					if err != nil {
						time.Sleep(time.Second)
						s.logger.Warn(err.Error())
					} else {
						break
					}
				}
				break
			}
		}
	}()

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

			_, err = conn.Create(path, []byte(""), flags, acls)
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

	s.pathReal, err = conn.CreateProtectedEphemeralSequential(s.path+"/node", data, zk.WorldACL(zk.PermAll))
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

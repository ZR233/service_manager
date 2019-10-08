/*
@Time : 2019-10-08 11:06
@Author : zr
*/
package service_manager

import (
	"encoding/json"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"net"
	"os"
	"strings"
	"time"
)

const prefix = "/service"

type Service struct {
	path  string
	conn  *zk.Conn
	hosts []string
}

func NewService(path string, hosts []string) *Service {
	s := &Service{}
	path = strings.Trim(path, "/")
	s.path = strings.Join([]string{prefix, path}, "/")
	s.hosts = hosts

	return s
}

func (s *Service) Open() (err error) {
	conn, _, err := zk.Connect(s.hosts, time.Second*5)
	if err != nil {
		return
	}
	s.conn = conn
	path := strings.TrimLeft(s.path, "/")
	pathSlice := strings.Split(path, "/")
	path = ""
	pathLayLen := len(pathSlice)
	for i := 0; i < pathLayLen-1; i++ {
		path += "/" + pathSlice[i]
		exist := false
		exist, _, err = s.conn.Exists(path)
		if err != nil {
			fmt.Println(err)
			return
		}
		if !exist {
			// permission
			var acls = zk.WorldACL(zk.PermAll)
			// create
			var flags int32 = 0
			p := ""
			p, err = conn.Create(path, []byte(""), flags, acls)
			if err != nil {
				return
			}
			fmt.Println("created:", p)
		}

	}

	type Data struct {
		Pid      int
		IP       []string
		HostName string
	}
	addrs, err := net.InterfaceAddrs()
	host, err := os.Hostname()

	dataStruct := Data{Pid: os.Getpid()}
	for _, v := range addrs {
		dataStruct.IP = append(dataStruct.IP, v.String())
	}
	dataStruct.HostName = host
	data, err := json.Marshal(dataStruct)
	if err != nil {
		return
	}
	p := ""
	p, err = conn.CreateProtectedEphemeralSequential(s.path+"/node", data, zk.WorldACL(zk.PermAll))
	if err != nil {
		return
	}
	fmt.Println("created:", p)
	return
}

func (s Service) Close() {
	s.conn.Close()
	return
}

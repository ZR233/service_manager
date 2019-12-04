package service_manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"os"
	"path"
	"strings"
	"time"
)

type NodeInfo struct {
	Pid      int
	Host     string
	HostName string
	Port     int
}

type Service struct {
	path     string
	conn     *zk.Conn
	zkHosts  []string
	pathReal string
	logger   Logger
	stopFlag bool
	funcChan chan func() error
	ServiceInfo
	NodeInfo
}

func (s *Service) SetLogger(logger Logger) {
	s.logger = logger
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
		s.funcChan <- func() error {
			err := s.register()
			if err != nil {
				err := fmt.Errorf("register service\n%w", err)
				return err
			}
			return nil
		}
	}
}

func (s *Service) register() (err error) {

	s.Pid = os.Getpid()

	data, err := json.Marshal(s.NodeInfo)
	if err != nil {
		err = fmt.Errorf("node info to json\n%w", err)
		return
	}

	host := ""

	if s.Type == ServiceTypeSingleton {
		host = "single"
	} else {
		host = fmt.Sprintf("%s:%d", s.Host, s.Port)
	}
	pathName := path.Join(s.path, host)

	s.pathReal, err = s.conn.Create(pathName, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		if errors.Is(err, zk.ErrNodeExists) {
			if s.Type == ServiceTypeSingleton {
				err = ErrSingletonServiceIsRunning
			}

			err = &NodeRunningError{
				RunningHost: host,
				Err:         err,
			}
			return
		}

		err = &ZKError{
			Msg: "Create: " + pathName + "\n",
			Err: err,
		}
		return
	}

	//ok := false
	//if ok, _, err = s.conn.Exists(pathName); err != nil {
	//	err = &ZKError{
	//		Msg: "check exists: " +pathName + "\n",
	//		Err: err,
	//	}
	//	return
	//} else {
	//	if ok{
	//		if s.Type == ServiceTypeSingleton{
	//			err = fmt.Errorf("%w\nrunning host:[%s]", ErrSingletonServiceIsRunning, )
	//			return
	//		}
	//	}else{
	//		s.pathReal, err = s.conn.Create(pathName, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	//		if err != nil {
	//			err = &ZKError{
	//				Msg: "Create: " +pathName + "\n",
	//				Err: err,
	//			}
	//			return
	//		}
	//	}
	//}
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
	go func() {
		for {
			if f, ok := <-s.funcChan; ok {
				err := f()
				e := &NodeRunningError{}
				if errors.As(err, &e) {
					panic(err)
				}
			} else {
				return
			}
		}
	}()

	_, err = s.connect()
	if err != nil {
		return
	}
	err = s.ensureServicePathExist()
	return
}

func (s *Service) ensureServicePathExist() (err error) {
	pathStr := strings.TrimLeft(s.path, "/")
	pathSlice := strings.Split(pathStr, "/")
	pathStr = ""
	pathLayLen := len(pathSlice)
	for i := 0; i < pathLayLen; i++ {
		pathStr += "/" + pathSlice[i]
		exist := false
		exist, _, err = s.conn.Exists(pathStr)
		if err != nil {
			return
		}
		if !exist {
			// permission
			var acls = zk.WorldACL(zk.PermAll)
			// create
			var flags int32 = 0

			_, err = s.conn.Create(pathStr, []byte(""), flags, acls)
			if err != nil {
				return
			}
		}
	}
	data, err := json.Marshal(s.ServiceInfo)
	if err != nil {
		return
	}

	_, state, err := s.conn.Get(s.path)
	if err != nil {
		return
	}
	_, err = s.conn.Set(s.path, data, state.Version)
	return
}

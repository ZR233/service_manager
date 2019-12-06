package service_manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type Status int

const (
	StatusInit Status = iota
	StatusOpen
	StatusOpenSuccess
	StatusRegistered
	StatusStopping
	StatusStopped
)

type NodeInfo struct {
	Pid      int
	Host     string
	HostName string
	Port     int
	ExecPath string
}

type Producer struct {
	//private:
	sync.Mutex
	pathReal string
	manager  *Manager
	funcChan chan func() error
	service  *Service
	status   Status
	ctx      context.Context
	cancel   context.CancelFunc

	//public:
	NodeInfo
}

func (p *Producer) debug(args ...interface{}) {
	p.manager.print("Debug", "[producer]"+p.service.Path, args...)
}
func (p *Producer) info(args ...interface{}) {
	p.manager.print("Info", "[producer]"+p.service.Path, args...)
}
func (p *Producer) warn(args ...interface{}) {
	p.manager.print("Warn", "[producer]"+p.service.Path, args...)
}
func (p *Producer) panic(args ...interface{}) {
	p.manager.print("Panic", "[producer]"+p.service.Path, args...)
}

func (p *Producer) getEventCallback() zk.EventCallback {
	return func(event zk.Event) {
		if event.Type == zk.EventSession && event.State == zk.StateHasSession {
			p.info("register service")
			defer func() {
				recover()
			}()

			p.funcChan <- func() error {
				err := p.register()
				if err != nil {
					err := fmt.Errorf("register service\n%w", err)
					return err
				}
				return nil
			}
		}
	}
}

func (p *Producer) register() (err error) {
	p.Lock()
	defer p.Unlock()
	if p.status >= StatusStopping {
		return
	}

	p.Pid = os.Getpid()

	data, err := json.Marshal(p.NodeInfo)
	if err != nil {
		err = fmt.Errorf("node info to json\n%w", err)
		return
	}

	host := ""
	if p.service.Type == ServiceTypeSingleton {
		host = "single"
	} else {
		host = fmt.Sprintf("%s:%d", p.Host, p.Port)
	}
	pathName := path.Join(p.service.Path, host)

	p.pathReal, err = p.manager.conn.Create(pathName, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		if errors.Is(err, zk.ErrNodeExists) {
			if p.service.Type == ServiceTypeSingleton {
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
	p.status = StatusRegistered

	//ok := false
	//if ok, _, err = p.conn.Exists(pathName); err != nil {
	//	err = &ZKError{
	//		Msg: "check exists: " +pathName + "\n",
	//		Err: err,
	//	}
	//	return
	//} else {
	//	if ok{
	//		if p.Type == ServiceTypeSingleton{
	//			err = fmt.Errorf("%w\nrunning host:[%p]", ErrSingletonServiceIsRunning, )
	//			return
	//		}
	//	}else{
	//		p.pathReal, err = p.conn.Create(pathName, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	//		if err != nil {
	//			err = &ZKError{
	//				Msg: "Create: " +pathName + "\n",
	//				Err: err,
	//			}
	//			return
	//		}
	//	}
	//}
	p.debug("register success: ", pathName)
	return
}

func (p *Producer) release() {
	defer func() {
		recover()
	}()
	p.status = StatusStopping
	close(p.funcChan)
}

func (p *Producer) Close() error {
	p.cancel()
	return nil
}
func (p *Producer) registerLoop() {
	defer func() {
		if e := recover(); e != nil {
			p.warn(fmt.Sprintf("%p", e))
		}
	}()

	if p.manager.conn.State() == zk.StateHasSession {
		err := p.register()
		if err != nil {
			p.warn(err.Error())
		}
	}
	time.Sleep(time.Second)
}
func GetCurrentPath() (string, error) {
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		return "", err
	}
	path, err := filepath.Abs(file)
	if err != nil {
		return "", err
	}
	//fmt.Println("path111:", path)
	if runtime.GOOS == "windows" {
		path = strings.Replace(path, "\\", "/", -1)
	}
	return path, nil
}

func (p *Producer) Open() (err error) {
	go func() {
		<-p.ctx.Done()
		p.release()
	}()

	p.ExecPath, _ = GetCurrentPath()

	p.status = StatusOpen
	go func() {
		defer func() {
			p.status = StatusStopped
			p.debug("service stop")
		}()

		for {
			if p.status >= StatusStopping {
				return
			}

			if p.status < StatusOpenSuccess {
				time.Sleep(time.Millisecond * 20)
				continue
			}

			select {
			case <-p.ctx.Done():
				return
			case f := <-p.funcChan:
				if f == nil {
					continue
				}

				err := f()
				e := &NodeRunningError{}
				if errors.As(err, &e) {
					panic(err)
				}

			}
		}
	}()

	p.Lock()
	defer p.Unlock()
	err = p.ensureServicePathExist()
	if err != nil {
		err = fmt.Errorf("service path error:\n%s\n%w", p.service.Path, err)
		return
	}

	p.status = StatusOpenSuccess
	p.debug("service path check ok")
	return
}

func (p *Producer) ensureServicePathExist() (err error) {
	pathStr := strings.TrimLeft(p.service.Path, "/")
	pathSlice := strings.Split(pathStr, "/")
	pathStr = ""
	pathLayLen := len(pathSlice)
	for i := 0; i < pathLayLen; i++ {
		pathStr += "/" + pathSlice[i]
		exist := false
		exist, _, err = p.manager.conn.Exists(pathStr)
		if err != nil {
			return
		}
		if !exist {
			// permission
			var acls = zk.WorldACL(zk.PermAll)
			// create
			var flags int32 = 0

			_, err = p.manager.conn.Create(pathStr, []byte(""), flags, acls)
			if err != nil {
				return
			}
		}
	}
	data, err := json.Marshal(p.service)
	if err != nil {
		return
	}

	_, state, err := p.manager.conn.Get(p.service.Path)
	if err != nil {
		return
	}
	_, err = p.manager.conn.Set(p.service.Path, data, state.Version)
	return
}

func (p *Producer) Status() Status {
	return p.status
}

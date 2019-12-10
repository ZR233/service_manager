package service_manager

import (
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
	"time"
)

type NodeInfo struct {
	Pid      int
	Host     string
	HostName string
	Port     int
	ExecPath string
	version  int32
}

type Producer struct {
	//private:
	producerConsumerBase
	pathReal string

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

//func (p *Producer) getEventCallback() zk.EventCallback {
//	return func(event zk.Event) {
//		if event.Type == zk.EventSession && event.State == zk.StateHasSession {
//			p.info("register service")
//			defer func() {
//				recover()
//			}()
//
//			p.funcChan <- func() error {
//				err := p.register()
//				if err != nil {
//					err := fmt.Errorf("register service\n%w", err)
//					return err
//				}
//				return nil
//			}
//		}
//	}
//}

func (p *Producer) getEventCallback() zk.EventCallback {
	return p.onZkConnect("Producer", func() error {
		err := p.ensureServicePathExist()
		if err != nil {
			p.ready("Producer", err)
			err = fmt.Errorf("service path error:\n%s\n%w", p.service.Path, err)
			return err
		}

		err = p.register()
		p.ready("Producer", err)
		if err != nil {
			err := fmt.Errorf("register service\n%w", err)
			return err
		}

		return nil
	})
}

func (p *Producer) register() (err error) {
	if p.status >= StatusStopping {
		return
	}

	p.Pid = os.Getpid()

	host := ""
	if p.service.Type == ServiceTypeSingleton {
		host = "single"
	} else {
		host = fmt.Sprintf("%s:%d", p.Host, p.Port)
	}
	pathName := path.Join(p.service.Path, host)

	nodeData, err := json.Marshal(p.NodeInfo)
	if err != nil {
		err = fmt.Errorf("node info to json\n%w", err)
		err = &NodeRunningError{
			RunningHost: host,
			Err:         err,
		}
		return
	}

	data, _, err := p.manager.conn.Get(pathName)
	if err != nil {
		// 临时节点不存在，创建
		if err == zk.ErrNoNode {
			p.pathReal, err = p.manager.conn.Create(pathName, nodeData, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
			if err != nil {
				if errors.Is(err, zk.ErrNodeExists) {
					err = &NodeRunningError{
						RunningHost: host,
						Err:         err,
					}
				}
			}
		}
		return
	}
	// 临时节点已存在，判断是否是自身
	nodeInfoOld := &NodeInfo{}
	err = json.Unmarshal(data, nodeInfoOld)
	if err != nil {
		err = &NodeRunningError{
			RunningHost: host,
			Err:         err,
		}
		return
	}

	// 若为自身， 删除重建
	if nodeInfoOld.HostName == p.HostName && nodeInfoOld.Pid == p.Pid {
		err = p.manager.conn.Delete(pathName, -1)
		if err != nil {
			err = fmt.Errorf("delete fail\n%s\n%w", pathName, err)
			return
		}

		p.pathReal, err = p.manager.conn.Create(pathName, nodeData, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if err != nil {
			if errors.Is(err, zk.ErrNodeExists) {
				err = &NodeRunningError{
					RunningHost: host,
					Err:         err,
				}
			}
		}

	} else {
		// 非自身，退出
		err = &NodeRunningError{
			RunningHost: host,
			Err:         zk.ErrNodeExists,
		}
		return
	}

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
	pathStr, err := filepath.Abs(file)
	if err != nil {
		return "", err
	}
	//fmt.Println("path111:", pathStr)
	if runtime.GOOS == "windows" {
		pathStr = strings.Replace(pathStr, "\\", "/", -1)
	}
	return pathStr, nil
}

func (p *Producer) Open() (err error) {
	go func() {
		<-p.ctx.Done()
		p.release()
	}()
	p.status = StatusOpenSuccess
	p.ExecPath, _ = GetCurrentPath()

	p.debug("service path check ok")

	go p.funcDealerLoop("Producer")
	return
}

//func (p *Producer) dealFuncLoop() {
//	defer func() {
//		p.status = StatusStopped
//		p.debug("service stop")
//	}()
//	for {
//		if p.status >= StatusStopping {
//			return
//		}
//
//		select {
//		case <-p.ctx.Done():
//			return
//		case f := <-p.funcChan:
//			if f == nil {
//				continue
//			}
//
//			err := f()
//			e := &NodeRunningError{}
//			if errors.As(err, &e) {
//				panic(err)
//			}
//
//		}
//	}
//}

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

package service_manager

import (
	"errors"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	ErrNoService                 = errors.New("no service")
	ErrNoServiceAlive            = errors.New("no service alive")
	ErrHost                      = errors.New("host error")
	ErrConnectToZooKeeper        = zk.ErrNoServer
	ErrInvalidPath               = zk.ErrInvalidPath
	ErrSingletonServiceIsRunning = errors.New("service type is singleton and there is one already running")
)

type ZKError struct {
	Msg string
	Err error
}

func (z *ZKError) Error() string {
	return z.Msg + ":" + z.Err.Error()
}

func (z *ZKError) Unwrap() error { return z.Err }

type NodeRunningError struct {
	RunningHost string
	Err         error
}

func (e *NodeRunningError) Error() string {
	return fmt.Sprintf("host(%s) is running\n%s", e.RunningHost, e.Err)
}

func (e *NodeRunningError) Unwrap() error { return e.Err }

func test() {

}

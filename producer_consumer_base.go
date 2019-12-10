package service_manager

import (
	"context"
	"github.com/samuel/go-zookeeper/zk"
	"sync"
	"time"
)

type Status int

const (
	StatusInit Status = iota
	StatusOpen
	StatusOpenSuccess
	StatusReady
	StatusZkConnected
	StatusStopping
	StatusStopped
)

type producerConsumerBase struct {
	sync.Mutex
	manager   *Manager
	funcChan  chan func() error
	service   *Service
	status    Status
	ctx       context.Context
	cancel    context.CancelFunc
	zkCtx     context.Context
	zkCancel  context.CancelFunc
	readyChan chan bool
	err       error
}

func (p *producerConsumerBase) init(ctx context.Context, manager *Manager, service *Service) {
	p.manager = manager
	p.service = service
	p.ctx, p.cancel = context.WithCancel(ctx)

	p.funcChan = make(chan func() error, 5)
	p.readyChan = make(chan bool, 1)
}

func (p *producerConsumerBase) statusChangeOnConnect() {
	p.Lock()
	defer p.Unlock()

	if p.status >= StatusReady && p.status <= StatusStopping {
		p.status = StatusZkConnected
	}
}

func (p *producerConsumerBase) onZkConnect(name string, handler func() error) zk.EventCallback {
	return func(event zk.Event) {
		if event.Type == zk.EventSession && event.State == zk.StateHasSession {
			p.zkStop()
			p.statusChangeOnConnect()

			p.zkCtx, p.zkCancel = context.WithCancel(p.ctx)
			p.manager.logger.Info("[" + name + "]zk login success")
			defer func() {
				recover()
			}()

			p.funcChan <- handler
		}
	}
}

func (p *producerConsumerBase) ready(name string, err error) {
	defer func() {
		recover()
	}()
	p.err = err
	select {
	case p.readyChan <- true:
		p.manager.logger.Debug("[" + name + "]ready")
	default:
	}

	p.Lock()
	defer p.Unlock()
	if p.status < StatusReady {
		p.status = StatusReady
	}
}

func (p *producerConsumerBase) waitForReady() error {
	select {
	case <-p.ctx.Done():
	case <-p.readyChan:
	}

	return p.err
}
func (p *producerConsumerBase) zkStop() {
	defer func() {
		recover()
	}()
	p.zkCancel()
	<-time.After(time.Microsecond * 10)
}

func (p *producerConsumerBase) funcDealerLoop(name string) {
	for {
		if p.status >= StatusStopping {
			return
		}

		select {
		case <-p.ctx.Done():
			return
		case f, ok := <-p.funcChan:
			if !ok {
				return
			}
			err := f()
			if err != nil {
				p.manager.logger.Error("["+name+"]:", err)
			}
		}
	}
}

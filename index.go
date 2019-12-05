/*
@Time : 2019-10-08 11:06
@Author : zr
*/
package service_manager

import (
	"context"
)

const prefix = "/service"

func NewManager(hosts []string) *Manager {
	s := &Manager{}
	s.hosts = hosts
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.SetLogger(defaultLogger{})

	go func() {
		<-s.ctx.Done()
		s.release()
	}()

	return s
}

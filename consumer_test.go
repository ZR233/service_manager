/*
@Time : 2019-10-08 16:37
@Author : zr
*/
package service_manager

import (
	"testing"
	"time"
)

func TestConsumer_Open(t *testing.T) {

	m := NewManager([]string{"192.168.0.3:2181"})
	_ = m.NewConsumer(m.NewService("test"))

	err := m.RunSync()
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(time.Second * 150)

}

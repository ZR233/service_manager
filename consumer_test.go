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
	c, err := m.NewConsumer(m.NewService("test"))
	if err != nil {
		t.Error(err)
		return
	}
	err = m.RunSync()
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(time.Second * 2)

	conn, err := c.GetConn()
	if err != nil {
		t.Error(err)
		return
	}
	c.ReleaseConn(conn)

	time.Sleep(time.Second * 150)

}

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
	c := NewConsumer("mac_info", []string{"192.168.0.3:2181"})
	c.Run()
	host, err := c.GetServiceHost()
	if err != nil {
		t.Error(err)
	}
	t.Log(host)
	time.Sleep(time.Minute * 5)
}

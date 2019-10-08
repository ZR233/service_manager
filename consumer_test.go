/*
@Time : 2019-10-08 16:37
@Author : zr
*/
package service_manager

import (
	"testing"
)

func TestConsumer_Open(t *testing.T) {
	c := NewConsumer("wifidig/log_manager", []string{"192.168.0.3:2181"})
	c.Open()
	host, err := c.GetServiceHost()
	if err != nil {
		t.Error(err)
	}
	t.Log(host)
}

/*
@Time : 2019-10-08 14:02
@Author : zr
*/
package service_manager

import (
	"testing"
	"time"
)

func TestNewService(t *testing.T) {
	s := NewService("/test", []string{"192.168.0.3:2181"})
	err := s.Open()
	if err != nil {
		t.Error(err)
	}
	defer s.Close()
	time.Sleep(time.Minute * 10)
}

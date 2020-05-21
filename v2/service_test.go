package service

import (
	"testing"
	"time"
)

func TestNewService(t *testing.T) {
	options := NewOptions()
	options.Name = "digger/history_check/maintenance"
	options.Port = 19999
	options.RunType = RunModelSingle
	s, err := NewService(options)
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(time.Second * 10)
	s.Close()

}

package service

import (
	"testing"
	"time"
)

func TestNewService(t *testing.T) {
	options := NewOptions()
	options.Name = "test"
	options.Port = 19999

	s, err := NewService(options)
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(time.Second * 10)
	s.Close()

}

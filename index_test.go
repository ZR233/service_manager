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
	m := NewManager([]string{"192.168.0.3:2181"})
	_, err := m.NewProducer(m.NewService("test"), 6000)
	if err != nil {
		t.Error(err)
		return
	}

	err = m.RunSync()
	if err != nil {
		t.Error(err)
		return
	}
	//go func() {
	//	time.Sleep(time.Second*5)
	//	s.Close()
	//}()
	defer m.Close()
	time.Sleep(time.Minute * 10)
}
func TestManager_Close(t *testing.T) {
	m := NewManager([]string{"192.168.0.3:2181"})
	_, err := m.NewProducer(m.NewService("test"), 6000)
	if err != nil {
		t.Error(err)
		return
	}

	err = m.RunSync()
	if err != nil {
		t.Error(err)
		return
	}
	go func() {
		time.Sleep(time.Second * 3)
		m.Close()
	}()
	time.Sleep(time.Second * 10)
}

func TestNewService2(t *testing.T) {
	//s, err := NewService("/test", 6000, []string{"192.168.0.3:2181"}, ServiceOptionServiceType(ServiceTypeSingleton))
	//if err != nil {
	//	t.Error(err)
	//}
	//
	//err = s.Open()
	//if err != nil {
	//	t.Error(err)
	//}
	//go func() {
	//	time.Sleep(time.Second * 5)
	//	s.Close()
	//}()
	//defer s.Close()
	//time.Sleep(time.Minute * 10)
}

func TestNewService3(t *testing.T) {
	//s, err := NewService("/test", 6000, []string{"192.168.0.3:2181"}, ServiceOptionServiceType(ServiceTypeSingleton))
	//if err != nil {
	//	t.Error(err)
	//}
	//
	//err = s.Open()
	//if err != nil {
	//	t.Error(err)
	//}
	////go func() {
	////	time.Sleep(time.Second*5)
	////	s.Close()
	////}()
	//defer s.Close()
	//time.Sleep(time.Minute * 10)
}

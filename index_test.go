/*
@Time : 2019-10-08 14:02
@Author : zr
*/
package service_manager

import (
	"io"
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
	println("service ok")
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

type testServers struct {
	aliveServer map[string]bool
}

type testConn struct {
	close bool
	host  string
}

func (t testConn) Close() error {
	t.close = true
	return nil
}

func testConnFactory() ConnFactory {
	return func(host string) (closer io.Closer, err error) {
		closer = &testConn{
			host: host,
		}
		return
	}
}
func (t *testServers) testTestConnFunc() ConsumerOptionConnTestFunc {
	return func(closer io.Closer) bool {
		test := closer.(*testConn)
		_, ok := t.aliveServer[test.host]
		return ok
	}
}

func TestNewService3(t *testing.T) {
	service := &testServers{
		map[string]bool{
			"test1:6000": true,
			"test2:6000": true,
		},
	}

	m := NewManager([]string{"192.168.0.3:2181"})
	defer m.Close()

	_, err := m.NewProducer(m.NewService("test"), 6000, OptionHost("test1"))
	if err != nil {
		t.Error(err)
		return
	}

	m2 := NewManager([]string{"192.168.0.3:2181"})
	defer m2.Close()
	_, err = m2.NewProducer(m2.NewService("test"), 6000, OptionHost("test2"))
	if err != nil {
		t.Error(err)
		return
	}
	err = m.RunSync()
	if err != nil {
		t.Error(err)
		return
	}
	err = m2.RunSync()
	if err != nil {
		t.Error(err)
		return
	}
	m3 := NewManager([]string{"192.168.0.3:2181"})
	defer m.Close()

	c, err := m3.NewConsumer(m3.NewService("test"),
		ConsumerOptionConnMax(2),
		ConsumerOptionConnFactory(testConnFactory()),
		service.testTestConnFunc())
	if err != nil {
		t.Error(err)
		return
	}
	err = m3.RunSync()
	if err != nil {
		t.Error(err)
		return
	}
	println("初始化完成")
	println("获取连接")

	conn1, err := c.GetConn()
	if err != nil {
		t.Error(err)
		return
	}
	c.ReleaseConn(conn1)

	println("服务挂，但端口没挂")
	delete(service.aliveServer, "test2:6000")
	conn1, err = c.GetConn()
	if err != nil {
		t.Error(err)
		return
	}
	conn2, err := c.GetConn()
	if err != nil {
		t.Error(err)
		return
	}
	if conn1.(*testConn).host != "test1:6000" {
		t.Error(conn2.(*testConn).host)
		return
	}
	if conn2.(*testConn).host != "test1:6000" {
		t.Error(conn2.(*testConn).host)
		return
	}

	println("放回连接")
	c.ReleaseConn(conn1)
	c.ReleaseConn(conn2)

	m2.Close()
	println("关闭test2")
	time.Sleep(time.Second)

	println("获取连接")
	conn1, err = c.GetConn()
	if err != nil {
		t.Error(err)
		return
	}
	conn2, err = c.GetConn()
	if err != nil {
		t.Error(err)
		return
	}

	if conn1.(*testConn).host != "test1:6000" {
		t.Error(conn1.(*testConn).host)
		return
	}

	if conn2.(*testConn).host != "test1:6000" {
		t.Error(conn2.(*testConn).host)
		return
	}

	println("都挂了")
	m.Close()

	time.Sleep(time.Millisecond * 500)

	conn1, err = c.GetConn()
	if err == nil {
		t.Error("未报错")
		return
	}
	println(err.Error())

}

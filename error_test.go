package service_manager

import (
	"net"
	"testing"
)

func Test_test(t *testing.T) {
	_, err := net.Dial("tcp", "[2604:A880:0002:00D0:0000:0000:20A5:1001]:22")
	t.Log(err)
}

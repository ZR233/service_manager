package service_manager

import (
	pool "github.com/ZR233/goutils/conn_pool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"io"
)

func GRpcFactory() ConnFactory {
	return func(host string) (conn io.Closer, err error) {
		conn, err = grpc.Dial(host, grpc.WithInsecure())
		return
	}
}
func GRpcConnTest() pool.ConnTestFunc {
	return func(closer io.Closer) bool {
		conn := closer.(*grpc.ClientConn)
		return conn.GetState() == connectivity.Ready
	}
}

package check_grpc

import (
	"fmt"
	service "github.com/ZR233/service_manager/v2"
	"github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func NewCheck(grpcServer *grpc.Server) service.Check {
	c := &CheckGrpc{}
	grpc_health_v1.RegisterHealthServer(grpcServer, &HealthImpl{})
	return c
}

type CheckGrpc struct {
	options *service.Options
}

func (c *CheckGrpc) Set(o *service.Options) {
	c.options = o
}

func (c *CheckGrpc) Exec(reg *api.AgentServiceRegistration) {
	reg.Check = &api.AgentServiceCheck{ // 健康检查
		Interval: "5s", // 健康检查间隔
		// grpc 支持，执行健康检查的地址，service 会传到 Health.Check 函数中
		GRPC:                           fmt.Sprintf("%v:%v/%v", "127.0.0.1", c.options.Port, "grpc.health.v1.Health/Check"),
		DeregisterCriticalServiceAfter: "10s", // 注销时间，相当于过期时间
	}
}

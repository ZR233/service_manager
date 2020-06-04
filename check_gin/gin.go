package check_gin

import (
	"fmt"
	"github.com/ZR233/service_manager/v2"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/consul/api"
	"net/http"
)

func NewCheck(router gin.IRouter) service.Check {
	c := &CheckGin{router: router}
	return c
}

type CheckGin struct {
	options *service.Options
	router  gin.IRouter
}

func (c *CheckGin) Set(o *service.Options) {
	c.options = o
}
func (c *CheckGin) Exec(reg *api.AgentServiceRegistration) {
	reg.Check = &api.AgentServiceCheck{ // 健康检查
		Interval:                       "5s", // 健康检查间隔
		HTTP:                           fmt.Sprintf("http://%s:%d/%s", "127.0.0.1", c.options.Port, "v1.Health/Check"),
		DeregisterCriticalServiceAfter: "10s", // 注销时间，相当于过期时间
	}

	c.router.GET("/v1.Health/Check", func(ctx *gin.Context) {
		ctx.String(http.StatusOK, "success")
	})
}

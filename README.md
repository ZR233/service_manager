# service_manager
服务管理系统 基于zookeeper
## 开始使用
服务生产者

```go
import "github.com/ZR233/service_manager"
//zookeeper集群地址
zkHosts = ["192.168.0.2:2181", "192.168.0.3:2181"]
//服务名称
serviceName = "test_project/auth"
//服务地址
serviceHost = "localhost:9999"
service = service_manager.NewService(serviceName,
                          		serviceHost,
                          		zkHosts)
//服务注册，此函数不会阻塞
service.Open()

if err != nil {
    panic(err)
}
//服务注销
defer service.Close()

``` 

服务消费者
```go
import "github.com/ZR233/service_manager"

zkHosts = ["192.168.0.2:2181", "192.168.0.3:2181"]
consumer = service_manager.NewConsumer("cam_detect", zkHosts)
//Run()函数不会阻塞
consumer.Run()

address, err := consumer.GetServiceHost()
if err != nil {
    return
}

```  

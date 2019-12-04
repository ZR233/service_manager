package service_manager

type Protocol int

const (
	ProtocolGRPC Protocol = iota
	ProtocolHttp
	ProtocolThrift
)

type ServiceType int

const (
	ServiceTypeNormal    ServiceType = iota
	ServiceTypeSingleton             //只允许运行一个实例
)

type ServiceInfo struct {
	Protocol Protocol
	Type     ServiceType
}

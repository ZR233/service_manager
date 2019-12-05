package service_manager

type Protocol int

const (
	ProtocolGRPC Protocol = iota
	ProtocolHttp
	ProtocolThrift
)

func (p Protocol) String() string {
	switch p {
	case ProtocolGRPC:
		return "gRpc"
	case ProtocolHttp:
		return "http"
	case ProtocolThrift:
		return "thrift"
	}
	return "unknown"
}

type ServiceType int

const (
	ServiceTypeNormal    ServiceType = iota
	ServiceTypeSingleton             //只允许运行一个实例
)

func (s ServiceType) String() string {
	switch s {
	case ServiceTypeNormal:
		return "normal"
	case ServiceTypeSingleton:
		return "singleton"
	}
	return "unknown"
}

type Service struct {
	Path     string `json:"-"`
	Protocol Protocol
	Type     ServiceType
}

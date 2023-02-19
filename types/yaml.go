package types

type YamlConfig interface {
	GetHost() string
	GetPort() string
}

type YamlConfigProvider interface {
	ProvideConfig() (YamlConfig, error)
}

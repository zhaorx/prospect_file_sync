package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

var Cfg Config

const configPath = "./config.yml"

// 加载log
func init() {
	// load cfg
	data, err := os.ReadFile(configPath)
	if err != nil {
		panic(err.Error())
	}

	Cfg = NewConfigWithDefault()
	err = yaml.Unmarshal(data, &Cfg)
	if err != nil {
		panic(err)
	}
}

// cfg 缺省设置
func NewConfigWithDefault() Config {
	c := Config{
		Profile: "dev",
		Beat:    12,
	}
	return c
}

type Config struct {
	Profile string       `yaml:"profile"` // 执行环境 dev/prod/history/org
	Beat    int          `yaml:"beat"`    // 执行间隔周期（小时）
	Target  RegionConfig `yaml:"target"`  // 目标服务器和数据库

	Regions []RegionConfig `yaml:"regions"`
}

type RegionConfig struct {
	Name    string `yaml:"name"`
	BaseUrl string `yaml:"baseUrl"` // http://guest:guest@localhost/files/
	RootDir string `yaml:"rootDir"` // D:\KTXXWD\
	DB      DB     `yaml:"db"`
}

// oracle数据库配置
type DB struct {
	Host        string `yaml:"host"`
	Port        int    `yaml:"port"`
	Username    string `yaml:"username"`
	Password    string `yaml:"password"`
	ServiceName string `yaml:"serviceName"`
	LogTable    string `yaml:"logTable"`
	FileTable   string `yaml:"fileTable"`
}

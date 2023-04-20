package main

import (
	"log"
	"os"
	"sync"

	"github.com/robfig/cron/v3"
	"gopkg.in/natefinch/lumberjack.v2"
	"prospect_file_sync/config"
	_ "prospect_file_sync/config"
)

var wg sync.WaitGroup
var cfg = config.Cfg
var logger *log.Logger

func main() {
	// 1. init log
	if cfg.Profile == "prod" {
		logger = log.New(&lumberjack.Logger{
			Filename:   "./prospect.log",
			MaxSize:    6, // megabytes
			MaxBackups: 3,
			MaxAge:     30, // days
		}, "", log.Lshortfile|log.Ldate|log.Ltime)
	} else {
		logger = log.New(os.Stdout, "", log.Lshortfile|log.Ldate|log.Ltime)
	}

	// 2. init 目标库连接
	InitTargetDB(cfg)

	// 3. 注册每日任务
	registerDailyJob()

	// 4. 即刻执行一次job
	runJob()

	select {}
}

func runJob() {
	for _, rc := range cfg.Regions {
		SyncFiles(rc)
	}
}

func registerDailyJob() {
	if len(cfg.Cron) == 0 {
		panic("Cron表达式为空!")
	}

	c := newWithSeconds()
	_, err := c.AddFunc(cfg.Cron, func() {
		logger.Println("执行一次job")
		runJob()
	})
	if err != nil {
		panic(err)
	}
	c.Start()
}

// 返回一个支持至 秒 级别的 cron
func newWithSeconds() *cron.Cron {
	secondParser := cron.NewParser(cron.Second | cron.Minute |
		cron.Hour | cron.Dom | cron.Month | cron.DowOptional | cron.Descriptor)
	return cron.New(cron.WithParser(secondParser), cron.WithChain())
}

package main

import (
	"log"
	"os"
	"sync"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
	"prospect_file_sync/config"
	_ "prospect_file_sync/config"
)

var wg sync.WaitGroup
var cfg = config.Cfg
var logger *log.Logger

func main() {
	wg.Add(1) // 阻塞main方法

	// 1. init log
	if cfg.Profile == "prod" {
		logger = log.New(&lumberjack.Logger{
			Filename:   "./prospect.log",
			MaxSize:    1, // megabytes
			MaxBackups: 3,
			MaxAge:     30, // days
		}, "", log.Lshortfile|log.Ldate|log.Ltime)
	} else {
		logger = log.New(os.Stdout, "", log.Lshortfile|log.Ldate|log.Ltime)
	}

	// 2. init 目标库连接
	InitTargetDB(cfg)

	// 3. 即刻执行一次job
	runJob()

	// 4. 周期性执行每个油田的SyncFiles
	ticker := time.Tick(time.Duration(cfg.Beat) * time.Hour)
	for _ = range ticker {
		runJob()
	}

	wg.Wait()
}

func runJob() {
	for _, rc := range cfg.Regions {
		go SyncFiles(rc)
	}
}

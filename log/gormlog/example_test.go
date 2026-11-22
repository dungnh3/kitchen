package gormlog_test

import (
	"time"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/log/gormlog"
	"gorm.io/gorm/logger"
)

func ExampleNew() {
	// Records go to log.Default() (package kitchen/log), the process logger
	// main installs with log.Setup; set Config.Logger to use another one.
	l := gormlog.New(gormlog.Config{
		SlowThreshold:        500 * time.Millisecond,
		IgnoreRecordNotFound: true,
	})
	// gorm.Open(dialector, &gorm.Config{Logger: l})
	_ = l
}

func ExampleNew_debug() {
	// Local debugging: every query at INFO, with parameter values. Never
	// enable LogParams in production: parameters often hold personal data
	// or secrets.
	l := gormlog.New(gormlog.Config{
		Logger:    log.Default(),
		Level:     logger.Info,
		LogParams: true,
	})
	_ = l
}

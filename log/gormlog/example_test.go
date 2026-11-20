package gormlog_test

import (
	"log/slog"
	"os"
	"time"

	"github.com/dungnh3/kitchen/log/gormlog"
)

func ExampleNew() {
	l := gormlog.New(gormlog.Config{
		Logger:               slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		SlowThreshold:        500 * time.Millisecond,
		IgnoreRecordNotFound: true,
	})
	// gorm.Open(dialector, &gorm.Config{Logger: l})
	_ = l
}

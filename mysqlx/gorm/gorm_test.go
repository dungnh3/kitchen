package mysqlgorm_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"gorm.io/gorm"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/mysqlx"
	mysqlgorm "github.com/dungnh3/kitchen/mysqlx/gorm"
)

func TestOpenNilDB(t *testing.T) {
	if _, err := mysqlgorm.Open(nil, mysqlgorm.Options{}); err == nil {
		t.Fatal("want error")
	}
}

func TestMapError(t *testing.T) {
	if !errors.Is(mysqlgorm.MapError(gorm.ErrRecordNotFound), errs.ErrNotFound) {
		t.Fatal("record not found")
	}
	if !errors.Is(mysqlgorm.MapError(&mysql.MySQLError{Number: 1062}), errs.ErrAlreadyExists) {
		t.Fatal("duplicate")
	}
}

type Item struct {
	ID   int64
	Name string `gorm:"size:32;uniqueIndex"`
}

func TestSharedDB(t *testing.T) {
	if testing.Short() || os.Getenv("KITCHEN_SKIP_DOCKER") != "" {
		t.Skip("docker tests disabled")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	c, err := tcmysql.Run(ctx, "mysql:8.4", tcmysql.WithDatabase("app"), tcmysql.WithUsername("app"), tcmysql.WithPassword("app"))
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatal(err)
	}
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "3306/tcp")
	db, err := mysqlx.Open(ctx, mysqlx.Config{Addr: host + ":" + port.Port(), User: "app", Password: "app", Database: "app"}, mysqlx.WithoutPoolStats())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	gdb, err := mysqlgorm.Open(db, mysqlgorm.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := gdb.AutoMigrate(&Item{}); err != nil {
		t.Fatal(err)
	}
	if err := gdb.Create(&Item{Name: "a"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := gdb.Create(&Item{Name: "a"}).Error; !errors.Is(mysqlgorm.MapError(err), errs.ErrAlreadyExists) {
		t.Fatalf("duplicate: %v", err)
	}
	var it Item
	if err := gdb.First(&it, "name = ?", "zzz").Error; !errors.Is(mysqlgorm.MapError(err), errs.ErrNotFound) {
		t.Fatalf("not found: %v", err)
	}
}

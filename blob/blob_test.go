package blob_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"

	"github.com/dungnh3/kitchen/blob"
	"github.com/dungnh3/kitchen/blob/memblob"
)

func TestPutExistsReadAll(t *testing.T) {
	ctx := context.Background()
	b := memblob.New()
	defer func() { _ = b.Close() }()

	ok, err := blob.Exists(ctx, b, "a/b.txt")
	if err != nil || ok {
		t.Fatalf("Exists before put = %v, %v", ok, err)
	}
	if err := blob.Put(ctx, b, "a/b.txt", strings.NewReader("hello"), &blob.WriterOptions{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	ok, err = blob.Exists(ctx, b, "a/b.txt")
	if err != nil || !ok {
		t.Fatalf("Exists after put = %v, %v", ok, err)
	}
	data, err := blob.ReadAll(ctx, b, "a/b.txt")
	if err != nil || string(data) != "hello" {
		t.Fatalf("ReadAll = %q, %v", data, err)
	}
	if _, err := blob.ReadAll(ctx, b, "missing"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("ReadAll missing err = %v", err)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("boom") }

func TestPutAbortsOnReadError(t *testing.T) {
	ctx := context.Background()
	b := memblob.New()
	err := blob.Put(ctx, b, "k", io.MultiReader(strings.NewReader("partial"), failingReader{}), nil)
	if err == nil {
		t.Fatal("want error")
	}
	if ok, _ := blob.Exists(ctx, b, "k"); ok {
		t.Fatal("partial object must not be stored")
	}
}

func TestNewObjectKey(t *testing.T) {
	re := regexp.MustCompile(`^image/avatar/\d{8}/[0-9a-v]{20}\.png$`)
	for _, ext := range []string{".png", "png"} {
		if k := blob.NewObjectKey(ext, "image", "avatar"); !re.MatchString(k) {
			t.Errorf("NewObjectKey(%q) = %q", ext, k)
		}
	}
	if k := blob.NewObjectKey(""); !regexp.MustCompile(`^\d{8}/[0-9a-v]{20}$`).MatchString(k) {
		t.Errorf("NewObjectKey no prefix = %q", k)
	}
}

func TestPublicURL(t *testing.T) {
	tests := []struct {
		base, key, want string
		wantErr         bool
	}{
		{"https://cdn.example.com", "a/b.png", "https://cdn.example.com/a/b.png", false},
		{"https://cdn.example.com/media/", "/a b/c?.png", "https://cdn.example.com/media/a%20b/c%3F.png", false},
		{"cdn.example.com", "a", "", true},
	}
	for _, tt := range tests {
		got, err := blob.PublicURL(tt.base, tt.key)
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("PublicURL(%q,%q) = %q, %v; want %q", tt.base, tt.key, got, err, tt.want)
		}
	}
}

func TestExtensionAndCategory(t *testing.T) {
	tests := []struct{ mt, ext, cat string }{
		{"image/jpeg", ".jpg", "image"},
		{"text/plain; charset=utf-8", ".txt", "text"},
		{"video/mp4", ".mp4", "video"},
		{"application/x-unknown-thing", "", "application"},
		{"", "", "file"},
		{"garbage", "", "file"},
	}
	for _, tt := range tests {
		if got := blob.ExtensionByType(tt.mt); got != tt.ext {
			t.Errorf("ExtensionByType(%q) = %q, want %q", tt.mt, got, tt.ext)
		}
		if got := blob.Category(tt.mt); got != tt.cat {
			t.Errorf("Category(%q) = %q, want %q", tt.mt, got, tt.cat)
		}
	}
}

func ExamplePut() {
	ctx := context.Background()
	var b blob.Bucket = memblob.New() // or gcs.Open(...)
	defer func() { _ = b.Close() }()

	key := "reports/q1.csv"
	if err := blob.Put(ctx, b, key, strings.NewReader("a,b\n1,2\n"), &blob.WriterOptions{ContentType: "text/csv"}); err != nil {
		panic(err)
	}
	attrs, _ := b.Attributes(ctx, key)
	fmt.Println(attrs.Size, attrs.ContentType)
	// Output: 8 text/csv
}

func ExamplePublicURL() {
	u, _ := blob.PublicURL("https://cdn.example.com", "image/20251120/photo 1.png")
	fmt.Println(u)
	// Output: https://cdn.example.com/image/20251120/photo%201.png
}

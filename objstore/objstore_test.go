package objstore_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/objstore"
	"github.com/dungnh3/kitchen/objstore/memstore"
)

func TestPutExistsReadAll(t *testing.T) {
	ctx := context.Background()
	b := memstore.New()
	defer func() { _ = b.Close() }()

	ok, err := objstore.Exists(ctx, b, "a/b.txt")
	if err != nil || ok {
		t.Fatalf("Exists before put = %v, %v", ok, err)
	}
	if err := objstore.Put(ctx, b, "a/b.txt", strings.NewReader("hello"), &objstore.WriterOptions{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	ok, err = objstore.Exists(ctx, b, "a/b.txt")
	if err != nil || !ok {
		t.Fatalf("Exists after put = %v, %v", ok, err)
	}
	data, err := objstore.ReadAll(ctx, b, "a/b.txt")
	if err != nil || string(data) != "hello" {
		t.Fatalf("ReadAll = %q, %v", data, err)
	}
	if _, err := objstore.ReadAll(ctx, b, "missing"); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("ReadAll missing err = %v", err)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("boom") }

func TestPutAbortsOnReadError(t *testing.T) {
	ctx := context.Background()
	b := memstore.New()
	err := objstore.Put(ctx, b, "k", io.MultiReader(strings.NewReader("partial"), failingReader{}), nil)
	if err == nil {
		t.Fatal("want error")
	}
	if ok, _ := objstore.Exists(ctx, b, "k"); ok {
		t.Fatal("partial object must not be stored")
	}
}

func TestNewObjectKey(t *testing.T) {
	re := regexp.MustCompile(`^image/avatar/\d{8}/[0-9a-v]{20}\.png$`)
	for _, ext := range []string{".png", "png"} {
		if k := objstore.NewObjectKey(ext, "image", "avatar"); !re.MatchString(k) {
			t.Errorf("NewObjectKey(%q) = %q", ext, k)
		}
	}
	if k := objstore.NewObjectKey(""); !regexp.MustCompile(`^\d{8}/[0-9a-v]{20}$`).MatchString(k) {
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
		got, err := objstore.PublicURL(tt.base, tt.key)
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
		if got := objstore.ExtensionByType(tt.mt); got != tt.ext {
			t.Errorf("ExtensionByType(%q) = %q, want %q", tt.mt, got, tt.ext)
		}
		if got := objstore.Category(tt.mt); got != tt.cat {
			t.Errorf("Category(%q) = %q, want %q", tt.mt, got, tt.cat)
		}
	}
}

func ExamplePut() {
	ctx := context.Background()
	var b objstore.Bucket = memstore.New() // or gcs.Open(...)
	defer func() { _ = b.Close() }()

	key := "reports/q1.csv"
	if err := objstore.Put(ctx, b, key, strings.NewReader("a,b\n1,2\n"), &objstore.WriterOptions{ContentType: "text/csv"}); err != nil {
		panic(err)
	}
	attrs, _ := b.Attributes(ctx, key)
	fmt.Println(attrs.Size, attrs.ContentType)
	// Output: 8 text/csv
}

func ExamplePublicURL() {
	u, _ := objstore.PublicURL("https://cdn.example.com", "image/20251120/photo 1.png")
	fmt.Println(u)
	// Output: https://cdn.example.com/image/20251120/photo%201.png
}

func TestValidKey(t *testing.T) {
	for _, k := range []string{"a", "a/b.png", "avatars/2025/user-42.jpg", "vi/ảnh đẹp.png"} {
		if err := objstore.ValidKey(k); err != nil {
			t.Errorf("ValidKey(%q) = %v", k, err)
		}
	}
	for _, k := range []string{"", "/a", "a//b", "a/", "../a", "a/../b", "./a", "a\x00b", "a\nb", "\xff", strings.Repeat("a", objstore.MaxKeyLen+1)} {
		if err := objstore.ValidKey(k); !errors.Is(err, objstore.ErrInvalidKey) || errs.Code(err) != codes.InvalidArgument {
			t.Errorf("ValidKey(%q) = %v", k, err)
		}
	}
}

func TestErrorsAreErrs(t *testing.T) {
	ctx := context.Background()
	b := memstore.New()
	_, err := b.NewReader(ctx, "missing")
	if errs.Code(err) != codes.NotFound || !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("missing: %v (%v)", err, errs.Code(err))
	}
}

func TestInstrument(t *testing.T) {
	ctx := context.Background()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	b := objstore.Instrument(memstore.New(), objstore.WithTracerProvider(tp), objstore.WithSystem("mem"))
	if err := objstore.Put(ctx, b, "a/b.txt", strings.NewReader("hello"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := b.NewReader(ctx, "missing"); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := b.SignedURL(ctx, "a/b.txt", objstore.SignOptions{}); err != nil {
		t.Fatal(err)
	}
	spans := rec.Ended()
	if len(spans) != 3 {
		t.Fatalf("spans = %d", len(spans))
	}
	w, r := spans[0], spans[1]
	if w.Name() != "objstore.Write" || !hasAttr(w.Attributes(), "objstore.bytes", "5") || !hasAttr(w.Attributes(), "objstore.system", "mem") {
		t.Fatalf("write span %s %v", w.Name(), w.Attributes())
	}
	if r.Name() != "objstore.Read" || r.Status().Code != otelcodes.Error {
		t.Fatalf("read span %s %v", r.Name(), r.Status())
	}
	plain := objstore.Instrument(onlyBucket{memstore.New()})
	if _, err := plain.SignedURL(ctx, "k", objstore.SignOptions{}); errs.Code(err) != codes.Unimplemented {
		t.Fatalf("non-signer: %v", err)
	}
}

type onlyBucket struct{ objstore.Bucket }

func hasAttr(attrs []attribute.KeyValue, k, v string) bool {
	for _, a := range attrs {
		if string(a.Key) == k && a.Value.String() == v {
			return true
		}
	}
	return false
}

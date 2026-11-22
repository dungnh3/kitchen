package memstore_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dungnh3/kitchen/objstore"
	"github.com/dungnh3/kitchen/objstore/memstore"
)

func TestAbortedWriteStoresNothing(t *testing.T) {
	b := memstore.New()
	ctx, cancel := context.WithCancel(context.Background())
	w, err := b.NewWriter(ctx, "k", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte("x"))
	cancel()
	if err := w.Close(); err == nil {
		t.Fatal("Close after cancel: want error")
	}
	if len(b.Keys()) != 0 {
		t.Fatalf("keys = %v", b.Keys())
	}
}

func TestAttributesDeleteClose(t *testing.T) {
	ctx := context.Background()
	b := memstore.New()
	md := map[string]string{"k": "v"}
	if err := objstore.Put(ctx, b, "x/y", strings.NewReader("abc"), &objstore.WriterOptions{ContentType: "text/plain", Metadata: md}); err != nil {
		t.Fatal(err)
	}
	md["k"] = "mutated"
	a, err := b.Attributes(ctx, "x/y")
	if err != nil || a.Size != 3 || a.Metadata["k"] != "v" || a.ETag == "" {
		t.Fatalf("Attributes = %+v, %v", a, err)
	}
	if err := b.Delete(ctx, "x/y"); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(ctx, "x/y"); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("second Delete = %v", err)
	}
	if _, err := b.NewWriter(ctx, "/bad", nil); err == nil {
		t.Fatal("leading slash key: want error")
	}
	_ = b.Close()
	if _, err := b.Attributes(ctx, "x/y"); err == nil {
		t.Fatal("after Close: want error")
	}
}

func TestSigner(t *testing.T) {
	ctx := context.Background()
	b := memstore.New()
	u, err := b.SignedURL(ctx, "a/b.png", objstore.SignOptions{Method: "PUT", ContentType: "image/png"})
	if err != nil || !strings.Contains(u, "method=PUT") || !strings.Contains(u, "content-type=image%2Fpng") {
		t.Fatalf("SignedURL = %q, %v", u, err)
	}
	p, err := b.PostPolicy(ctx, "a/b.png", objstore.PostPolicyOptions{MaxBytes: 10, ContentTypePrefix: "image/"})
	if err != nil || p.Fields["key"] != "a/b.png" || p.Fields["max-bytes"] != "10" {
		t.Fatalf("PostPolicy = %+v, %v", p, err)
	}
	if _, err := b.PostPolicy(ctx, "a/b.png", objstore.PostPolicyOptions{}); !errors.Is(err, objstore.ErrInvalidOptions) {
		t.Fatalf("policy without content type rule: %v", err)
	}
	if _, err := b.SignedURL(ctx, "a/b.png", objstore.SignOptions{Method: "PUT"}); !errors.Is(err, objstore.ErrInvalidOptions) {
		t.Fatalf("PUT without content type: %v", err)
	}
}

func TestIfNotExists(t *testing.T) {
	ctx := context.Background()
	b := memstore.New()
	opts := &objstore.WriterOptions{IfNotExists: true}
	if err := objstore.Put(ctx, b, "k", strings.NewReader("1"), opts); err != nil {
		t.Fatal(err)
	}
	if err := objstore.Put(ctx, b, "k", strings.NewReader("2"), opts); !errors.Is(err, objstore.ErrExists) {
		t.Fatalf("second write: %v", err)
	}
	if got, _ := objstore.ReadAll(ctx, b, "k"); string(got) != "1" {
		t.Fatalf("overwritten: %q", got)
	}
}

func TestEveryOperationValidatesKey(t *testing.T) {
	ctx := context.Background()
	b := memstore.New()
	const bad = "a/../b"
	if _, err := b.NewReader(ctx, bad); !errors.Is(err, objstore.ErrInvalidKey) {
		t.Errorf("NewReader: %v, want ErrInvalidKey", err)
	}
	if _, err := b.Attributes(ctx, bad); !errors.Is(err, objstore.ErrInvalidKey) {
		t.Errorf("Attributes: %v, want ErrInvalidKey", err)
	}
	if err := b.Delete(ctx, bad); !errors.Is(err, objstore.ErrInvalidKey) {
		t.Errorf("Delete: %v, want ErrInvalidKey", err)
	}
}

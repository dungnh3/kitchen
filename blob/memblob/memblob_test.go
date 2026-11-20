package memblob_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dungnh3/kitchen/blob"
	"github.com/dungnh3/kitchen/blob/memblob"
)

func TestAbortedWriteStoresNothing(t *testing.T) {
	b := memblob.New()
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
	b := memblob.New()
	md := map[string]string{"k": "v"}
	if err := blob.Put(ctx, b, "x/y", strings.NewReader("abc"), &blob.WriterOptions{ContentType: "text/plain", Metadata: md}); err != nil {
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
	if err := b.Delete(ctx, "x/y"); !errors.Is(err, blob.ErrNotFound) {
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
	b := memblob.New()
	u, err := b.SignedURL(ctx, "a/b.png", blob.SignOptions{Method: "PUT", ContentType: "image/png"})
	if err != nil || !strings.Contains(u, "method=PUT") || !strings.Contains(u, "content-type=image%2Fpng") {
		t.Fatalf("SignedURL = %q, %v", u, err)
	}
	p, err := b.PostPolicy(ctx, "a/b.png", blob.PostPolicyOptions{MaxBytes: 10})
	if err != nil || p.Fields["key"] != "a/b.png" || p.Fields["max-bytes"] != "10" {
		t.Fatalf("PostPolicy = %+v, %v", p, err)
	}
}

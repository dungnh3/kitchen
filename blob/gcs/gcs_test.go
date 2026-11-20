package gcs_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"

	"github.com/dungnh3/kitchen/blob"
	"github.com/dungnh3/kitchen/blob/gcs"
)

func fakeBucket(t *testing.T) *gcs.Bucket {
	t.Helper()
	srv, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		NoListener:     true,
		InitialObjects: []fakestorage.Object{{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "media", Name: "seed.txt"}, Content: []byte("seed")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	b, err := gcs.New(srv.Client(), gcs.Config{Bucket: "media", CDNBaseURL: "https://cdn.example.com/m"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestReadWrite(t *testing.T) {
	ctx := context.Background()
	b := fakeBucket(t)

	if err := blob.Put(ctx, b, "a/b.txt", strings.NewReader("hello"), &blob.WriterOptions{ContentType: "text/plain", Metadata: map[string]string{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	data, err := blob.ReadAll(ctx, b, "a/b.txt")
	if err != nil || string(data) != "hello" {
		t.Fatalf("ReadAll = %q, %v", data, err)
	}
	a, err := b.Attributes(ctx, "a/b.txt")
	if err != nil || a.Size != 5 || a.ContentType != "text/plain" || a.Metadata["k"] != "v" {
		t.Fatalf("Attributes = %+v, %v", a, err)
	}
	ok, err := blob.Exists(ctx, b, "seed.txt")
	if err != nil || !ok {
		t.Fatalf("Exists(seed) = %v, %v", ok, err)
	}
	if err := b.Delete(ctx, "a/b.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.NewReader(ctx, "a/b.txt"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("NewReader deleted err = %v", err)
	}
	if err := b.Delete(ctx, "a/b.txt"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("Delete missing err = %v", err)
	}
	if _, err := b.NewWriter(ctx, "/abs", nil); err == nil {
		t.Fatal("leading slash: want error")
	}
	u, err := b.PublicURL("img/a b.png")
	if err != nil || u != "https://cdn.example.com/m/img/a%20b.png" {
		t.Fatalf("PublicURL = %q, %v", u, err)
	}
}

func signingBucket(t *testing.T) *gcs.Bucket {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	srv, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	b, err := gcs.New(srv.Client(), gcs.Config{Bucket: "media", GoogleAccessID: "svc@proj.iam.gserviceaccount.com", PrivateKey: pemKey, MaxUploadBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSignedURL(t *testing.T) {
	ctx := context.Background()
	b := signingBucket(t)
	raw, err := b.SignedURL(ctx, "docs/r.pdf", blob.SignOptions{ResponseContentDisposition: `attachment; filename="r.pdf"`})
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(raw)
	q := u.Query()
	if !strings.HasSuffix(u.Path, "/media/docs/r.pdf") || q.Get("X-Goog-Algorithm") != "GOOG4-RSA-SHA256" ||
		(q.Get("X-Goog-Expires") != "300" && q.Get("X-Goog-Expires") != "299") || q.Get("response-content-disposition") != `attachment; filename="r.pdf"` {
		t.Fatalf("signed URL = %s", raw)
	}
	put, err := b.SignedURL(ctx, "up.png", blob.SignOptions{Method: "PUT", ContentType: "image/png", Expires: 60_000_000_000})
	if err != nil || !strings.Contains(put, "X-Goog-Expires=") || !strings.Contains(put, "content-type") {
		t.Fatalf("PUT URL = %s, %v", put, err)
	}
	if _, err := b.SignedURL(ctx, "", blob.SignOptions{}); err == nil {
		t.Fatal("empty key: want error")
	}
}

func TestPostPolicy(t *testing.T) {
	ctx := context.Background()
	b := signingBucket(t)
	p, err := b.PostPolicy(ctx, "u/1.png", blob.PostPolicyOptions{ContentType: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	if p.URL == "" || p.Fields["key"] != "u/1.png" || p.Fields["policy"] == "" || p.Fields["x-goog-signature"] == "" || p.Fields["content-type"] != "image/png" {
		t.Fatalf("policy = %+v", p)
	}
	if _, err := b.PostPolicy(ctx, "k", blob.PostPolicyOptions{MinBytes: 10, MaxBytes: 5}); err == nil {
		t.Fatal("bad range: want error")
	}
}

func TestConfigValidation(t *testing.T) {
	if _, err := gcs.New(nil, gcs.Config{Bucket: "b"}); err == nil {
		t.Fatal("nil client: want error")
	}
	srv, _ := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true})
	defer srv.Stop()
	for _, cfg := range []gcs.Config{{}, {Bucket: "b", CDNBaseURL: "cdn.example.com"}, {Bucket: "b", SignExpiry: 8 * 24 * 3600 * 1e9}} {
		if _, err := gcs.New(srv.Client(), cfg); err == nil {
			t.Errorf("%+v: want error", cfg)
		}
	}
	b, _ := gcs.New(srv.Client(), gcs.Config{Bucket: "b"})
	if _, err := b.PublicURL("k"); err == nil {
		t.Fatal("PublicURL without CDN: want error")
	}
}

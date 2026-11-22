package gcs_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/objstore"
	"github.com/dungnh3/kitchen/objstore/gcs"
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

	if err := objstore.Put(ctx, b, "a/b.txt", strings.NewReader("hello"), &objstore.WriterOptions{ContentType: "text/plain", Metadata: map[string]string{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	data, err := objstore.ReadAll(ctx, b, "a/b.txt")
	if err != nil || string(data) != "hello" {
		t.Fatalf("ReadAll = %q, %v", data, err)
	}
	a, err := b.Attributes(ctx, "a/b.txt")
	if err != nil || a.Size != 5 || a.ContentType != "text/plain" || a.Metadata["k"] != "v" {
		t.Fatalf("Attributes = %+v, %v", a, err)
	}
	ok, err := objstore.Exists(ctx, b, "seed.txt")
	if err != nil || !ok {
		t.Fatalf("Exists(seed) = %v, %v", ok, err)
	}
	if err := b.Delete(ctx, "a/b.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.NewReader(ctx, "a/b.txt"); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("NewReader deleted err = %v", err)
	}
	if err := b.Delete(ctx, "a/b.txt"); !errors.Is(err, objstore.ErrNotFound) {
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
	raw, err := b.SignedURL(ctx, "docs/r.pdf", objstore.SignOptions{ResponseContentDisposition: `attachment; filename="r.pdf"`})
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(raw)
	q := u.Query()
	if !strings.HasSuffix(u.Path, "/media/docs/r.pdf") || q.Get("X-Goog-Algorithm") != "GOOG4-RSA-SHA256" ||
		(q.Get("X-Goog-Expires") != "300" && q.Get("X-Goog-Expires") != "299") || q.Get("response-content-disposition") != `attachment; filename="r.pdf"` {
		t.Fatalf("signed URL = %s", raw)
	}
	put, err := b.SignedURL(ctx, "up.png", objstore.SignOptions{Method: "PUT", ContentType: "image/png", Expires: 60_000_000_000})
	if err != nil || !strings.Contains(put, "X-Goog-Expires=") || !strings.Contains(put, "content-type") {
		t.Fatalf("PUT URL = %s, %v", put, err)
	}
	if _, err := b.SignedURL(ctx, "", objstore.SignOptions{}); err == nil {
		t.Fatal("empty key: want error")
	}
}

func TestPostPolicy(t *testing.T) {
	ctx := context.Background()
	b := signingBucket(t)
	p, err := b.PostPolicy(ctx, "u/1.png", objstore.PostPolicyOptions{ContentType: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	if p.URL == "" || p.Fields["key"] != "u/1.png" || p.Fields["policy"] == "" || p.Fields["x-goog-signature"] == "" || p.Fields["content-type"] != "image/png" {
		t.Fatalf("policy = %+v", p)
	}
	if _, err := b.PostPolicy(ctx, "k", objstore.PostPolicyOptions{MinBytes: 10, MaxBytes: 5}); err == nil {
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

func TestSafetyRules(t *testing.T) {
	ctx := context.Background()
	b := signingBucket(t)

	put, err := b.SignedURL(ctx, "up.png", objstore.SignOptions{Method: "PUT", ContentType: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	signed := mustQuery(t, put).Get("X-Goog-SignedHeaders")
	if !strings.Contains(signed, "x-goog-content-length-range") || !strings.Contains(signed, "x-goog-if-generation-match") {
		t.Fatalf("PUT URL does not sign size/overwrite headers: %s", signed)
	}
	over, err := b.SignedURL(ctx, "up.png", objstore.SignOptions{Method: "PUT", ContentType: "image/png", AllowOverwrite: true})
	if err != nil || strings.Contains(mustQuery(t, over).Get("X-Goog-SignedHeaders"), "if-generation-match") {
		t.Fatalf("AllowOverwrite: %v", err)
	}
	if _, err := b.SignedURL(ctx, "up.png", objstore.SignOptions{Method: "PUT"}); !errors.Is(err, objstore.ErrInvalidOptions) {
		t.Fatalf("PUT without content type: %v", err)
	}
	if _, err := b.SignedURL(ctx, "up.bin", objstore.SignOptions{Method: "PUT", AllowAnyContentType: true}); err != nil {
		t.Fatalf("AllowAnyContentType: %v", err)
	}
	if _, err := b.PostPolicy(ctx, "u/1.png", objstore.PostPolicyOptions{}); !errors.Is(err, objstore.ErrInvalidOptions) {
		t.Fatalf("policy without content type rule: %v", err)
	}
	p, err := b.PostPolicy(ctx, "u/1.png", objstore.PostPolicyOptions{ContentTypePrefix: "image/"})
	if err != nil {
		t.Fatal(err)
	}
	policy, _ := base64.StdEncoding.DecodeString(p.Fields["policy"])
	if !strings.Contains(string(policy), `["starts-with","$Content-Type","image/"]`) {
		t.Fatalf("policy = %s", policy)
	}
	for _, k := range []string{"../etc/passwd", "a//b", "a/./b", "a\nb", ""} {
		if _, err := b.SignedURL(ctx, k, objstore.SignOptions{}); !errors.Is(err, objstore.ErrInvalidKey) || errs.Code(err) != codes.InvalidArgument {
			t.Errorf("key %q: %v", k, err)
		}
	}
}

func TestIfNotExistsAndErrs(t *testing.T) {
	ctx := context.Background()
	b := fakeBucket(t)
	err := objstore.Put(ctx, b, "seed.txt", strings.NewReader("x"), &objstore.WriterOptions{IfNotExists: true})
	if !errors.Is(err, objstore.ErrExists) || errs.Code(err) != codes.AlreadyExists {
		t.Fatalf("overwrite with IfNotExists: %v", err)
	}
	if err := objstore.Put(ctx, b, "fresh.txt", strings.NewReader("x"), &objstore.WriterOptions{IfNotExists: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Attributes(ctx, "missing"); errs.Code(err) != codes.NotFound {
		t.Fatalf("missing: %v", err)
	}
	if _, err := b.NewReader(ctx, "../x"); !errors.Is(err, objstore.ErrInvalidKey) {
		t.Fatalf("reader key check: %v", err)
	}
}

func mustQuery(t *testing.T, raw string) url.Values {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Query()
}

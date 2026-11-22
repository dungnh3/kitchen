package s3_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/objstore"
	"github.com/dungnh3/kitchen/objstore/s3"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func fakeBucket(t *testing.T, cfg s3.Config) *s3.Bucket {
	t.Helper()
	srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(srv.Close)
	client := awss3.New(awss3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("AKID", "SECRET", ""),
	})
	if _, err := client.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String("media")}); err != nil {
		t.Fatal(err)
	}
	if cfg.Bucket == "" {
		cfg.Bucket = "media"
	}
	b, err := s3.New(client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestReadWrite(t *testing.T) {
	ctx := t.Context()
	b := fakeBucket(t, s3.Config{CDNBaseURL: "https://cdn.example.com/m"})
	opts := &objstore.WriterOptions{ContentType: "text/plain", Metadata: map[string]string{"k": "v"}}
	if err := objstore.Put(ctx, b, "a/b.txt", strings.NewReader("hello"), opts); err != nil {
		t.Fatal(err)
	}
	data, err := objstore.ReadAll(ctx, b, "a/b.txt")
	if err != nil || string(data) != "hello" {
		t.Fatalf("ReadAll = %q, %v", data, err)
	}
	a, err := b.Attributes(ctx, "a/b.txt")
	if err != nil || a.Size != 5 || a.ContentType != "text/plain" || a.ETag == "" || strings.Contains(a.ETag, `"`) {
		t.Fatalf("Attributes = %+v, %v", a, err)
	}
	if ok, err := objstore.Exists(ctx, b, "a/b.txt"); err != nil || !ok {
		t.Fatalf("Exists = %v, %v", ok, err)
	}
	if err := b.Delete(ctx, "a/b.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.NewReader(ctx, "a/b.txt"); !errors.Is(err, objstore.ErrNotFound) || errs.Code(err) != codes.NotFound {
		t.Fatalf("NewReader deleted: %v", err)
	}
	if err := b.Delete(ctx, "a/b.txt"); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("Delete missing: %v", err)
	}
	if _, err := b.NewWriter(ctx, "../x", nil); !errors.Is(err, objstore.ErrInvalidKey) {
		t.Fatalf("bad key: %v", err)
	}
	u, err := b.PublicURL("img/a b.png")
	if err != nil || u != "https://cdn.example.com/m/img/a%20b.png" {
		t.Fatalf("PublicURL = %q, %v", u, err)
	}
}

func TestMultipart(t *testing.T) {
	ctx := t.Context()
	b := fakeBucket(t, s3.Config{PartSize: 5 << 20})
	payload := bytes.Repeat([]byte("0123456789"), (11<<20)/10) // 11 MiB: 3 parts
	if err := objstore.Put(ctx, b, "big.bin", bytes.NewReader(payload), &objstore.WriterOptions{ContentType: "application/octet-stream"}); err != nil {
		t.Fatal(err)
	}
	got, err := objstore.ReadAll(ctx, b, "big.bin")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("multipart round trip: %d bytes, %v", len(got), err)
	}
}

func TestAbortOnCancel(t *testing.T) {
	b := fakeBucket(t, s3.Config{PartSize: 5 << 20})
	ctx, cancel := context.WithCancel(t.Context())
	w, err := b.NewWriter(ctx, "partial.bin", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(bytes.Repeat([]byte("x"), 6<<20)); err != nil { // starts a multipart upload
		t.Fatal(err)
	}
	cancel()
	if err := w.Close(); err == nil {
		t.Fatal("Close after cancel: want error")
	}
	if ok, _ := objstore.Exists(t.Context(), b, "partial.bin"); ok {
		t.Fatal("aborted upload is visible")
	}
}

func TestIfNotExists(t *testing.T) {
	ctx := t.Context()
	b := fakeBucket(t, s3.Config{})
	opts := &objstore.WriterOptions{IfNotExists: true}
	if err := objstore.Put(ctx, b, "k", strings.NewReader("1"), opts); err != nil {
		t.Fatal(err)
	}
	err := objstore.Put(ctx, b, "k", strings.NewReader("2"), opts)
	if !errors.Is(err, objstore.ErrExists) || errs.Code(err) != codes.AlreadyExists {
		t.Fatalf("second write: %v", err)
	}
	if got, _ := objstore.ReadAll(ctx, b, "k"); string(got) != "1" {
		t.Fatalf("overwritten: %q", got)
	}
}

func TestSignedURL(t *testing.T) {
	ctx := t.Context()
	b := fakeBucket(t, s3.Config{})
	raw, err := b.SignedURL(ctx, "docs/r.pdf", objstore.SignOptions{ResponseContentDisposition: `attachment; filename="r.pdf"`})
	if err != nil {
		t.Fatal(err)
	}
	q := mustQuery(t, raw)
	if q.Get("X-Amz-Signature") == "" || q.Get("X-Amz-Expires") != "300" || q.Get("response-content-disposition") != `attachment; filename="r.pdf"` {
		t.Fatalf("GET URL = %s", raw)
	}
	if _, err := b.SignedURL(ctx, "up.png", objstore.SignOptions{Method: "PUT", ContentType: "image/png"}); !errors.Is(err, objstore.ErrInvalidOptions) {
		t.Fatalf("PUT without AllowUnboundedSize: %v", err)
	}
	put, err := b.SignedURL(ctx, "up.png", objstore.SignOptions{Method: "PUT", ContentType: "image/png", AllowUnboundedSize: true})
	if err != nil {
		t.Fatal(err)
	}
	if h := mustQuery(t, put).Get("X-Amz-SignedHeaders"); !strings.Contains(h, "if-none-match") || !strings.Contains(h, "content-type") {
		t.Fatalf("PUT signed headers = %s", h)
	}
	if _, err := b.SignedURL(ctx, "x", objstore.SignOptions{Method: "DELETE"}); !errors.Is(err, objstore.ErrInvalidOptions) {
		t.Fatalf("DELETE: %v", err)
	}
}

func TestPostPolicy(t *testing.T) {
	ctx := t.Context()
	b := fakeBucket(t, s3.Config{MaxUploadBytes: 1 << 20})
	p, err := b.PostPolicy(ctx, "u/1.png", objstore.PostPolicyOptions{ContentTypePrefix: "image/"})
	if err != nil {
		t.Fatal(err)
	}
	policy, _ := base64.StdEncoding.DecodeString(p.Fields["policy"])
	for _, want := range []string{`["content-length-range",0,1048576]`, `["starts-with","$Content-Type","image/"]`, `"u/1.png"`} {
		if !strings.Contains(string(policy), want) {
			t.Errorf("policy %s lacks %s", policy, want)
		}
	}
	if p.Fields["key"] != "u/1.png" || p.Fields["X-Amz-Signature"] == "" {
		t.Fatalf("fields = %v", p.Fields)
	}
	exact, err := b.PostPolicy(ctx, "u/2.png", objstore.PostPolicyOptions{ContentType: "image/png"})
	if err != nil || exact.Fields["Content-Type"] != "image/png" {
		t.Fatalf("exact content type: %+v %v", exact, err)
	}
	if _, err := b.PostPolicy(ctx, "u/1.png", objstore.PostPolicyOptions{}); !errors.Is(err, objstore.ErrInvalidOptions) {
		t.Fatalf("no content type rule: %v", err)
	}
}

func TestConfig(t *testing.T) {
	if _, err := s3.New(nil, s3.Config{Bucket: "b"}); err == nil {
		t.Error("nil client accepted")
	}
	c := awss3.New(awss3.Options{Region: "us-east-1"})
	for _, cfg := range []s3.Config{{}, {Bucket: "b", CDNBaseURL: "cdn"}, {Bucket: "b", PartSize: 1 << 20}} {
		if _, err := s3.New(c, cfg); err == nil {
			t.Errorf("config %+v accepted", cfg)
		}
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

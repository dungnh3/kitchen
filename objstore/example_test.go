package objstore_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/objstore"
	"github.com/dungnh3/kitchen/objstore/memstore"
)

func ExampleValidKey() {
	for _, key := range []string{"avatars/2025/user-42.jpg", "../etc/passwd", "/abs", "a//b"} {
		err := objstore.ValidKey(key)
		fmt.Println(key, errors.Is(err, objstore.ErrInvalidKey), errs.HTTPStatus(err))
	}
	// Output:
	// avatars/2025/user-42.jpg false 200
	// ../etc/passwd true 400
	// /abs true 400
	// a//b true 400
}

func ExampleBucket_NewReader() {
	ctx := context.Background()
	b := memstore.New()
	defer func() { _ = b.Close() }()

	_ = objstore.Put(ctx, b, "notes/a.txt", strings.NewReader("hello"), &objstore.WriterOptions{ContentType: "text/plain"})

	r, err := b.NewReader(ctx, "notes/a.txt")
	if err != nil {
		panic(err)
	}
	defer func() { _ = r.Close() }()
	data, _ := io.ReadAll(r)
	fmt.Println(string(data))

	_, err = b.NewReader(ctx, "notes/missing.txt")
	fmt.Println(errors.Is(err, objstore.ErrNotFound))
	// Output:
	// hello
	// true
}

func ExampleWriterOptions_ifNotExists() {
	ctx := context.Background()
	b := memstore.New()
	defer func() { _ = b.Close() }()

	create := &objstore.WriterOptions{ContentType: "application/json", IfNotExists: true}
	first := objstore.Put(ctx, b, "invoices/2025-001.json", strings.NewReader(`{"n":1}`), create)
	second := objstore.Put(ctx, b, "invoices/2025-001.json", strings.NewReader(`{"n":2}`), create)
	fmt.Println(first)
	fmt.Println(errors.Is(second, objstore.ErrExists), errs.HTTPStatus(second))
	// Output:
	// <nil>
	// true 409
}

func ExampleSigner() {
	ctx := context.Background()
	var s objstore.Signer = memstore.New() // or gcs.Open / s3.Open

	// A PUT URL is create-only and needs a content type.
	u, err := s.SignedURL(ctx, "avatars/u42.png", objstore.SignOptions{
		Method:      http.MethodPut,
		ContentType: "image/png",
		MaxBytes:    5 << 20,
	})
	fmt.Println(u != "", err)

	// Without ContentType the signer fails closed.
	_, err = s.SignedURL(ctx, "avatars/u42.png", objstore.SignOptions{Method: http.MethodPut})
	fmt.Println(errors.Is(err, objstore.ErrInvalidOptions), errs.HTTPStatus(err))
	// Output:
	// true <nil>
	// true 400
}

func ExampleSigner_postPolicy() {
	ctx := context.Background()
	var s objstore.Signer = memstore.New()

	key := objstore.NewObjectKey(objstore.ExtensionByType("image/png"), "image")
	pp, err := s.PostPolicy(ctx, key, objstore.PostPolicyOptions{
		ContentTypePrefix: "image/",
		MaxBytes:          10 << 20,
	})
	if err != nil {
		panic(err)
	}
	// Return pp as JSON; the browser POSTs pp.Fields then the "file" field to pp.URL.
	fmt.Println(pp.URL, pp.Fields["key"] == key)
	// Output: mem://bucket/ true
}

func ExampleInstrument() {
	ctx := context.Background()
	b := objstore.Instrument(memstore.New(), objstore.WithSystem("memstore"))
	defer func() { _ = b.Close() }()

	// Every call runs in an "objstore.<Op>" span; errors are recorded with otelx.
	_ = objstore.Put(ctx, b, "reports/q1.csv", strings.NewReader("a,b\n"), &objstore.WriterOptions{ContentType: "text/csv"})
	data, err := objstore.ReadAll(ctx, b, "reports/q1.csv")
	fmt.Printf("%q %v\n", data, err)
	// Output: "a,b\n" <nil>
}

func Example_httpErrors() {
	ctx := context.Background()
	b := memstore.New()

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := b.Attributes(ctx, strings.TrimPrefix(r.URL.Path, "/files/"))
		if err != nil {
			errs.WriteHTTP(w, r, err) // ErrNotFound → 404, ErrInvalidKey → 400
			return
		}
		w.WriteHeader(http.StatusNoContent) // the object exists
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/files/missing.txt", nil))
	fmt.Println(rec.Code, strings.Contains(rec.Body.String(), "OBJECT_NOT_FOUND"))
	// Output: 404 true
}

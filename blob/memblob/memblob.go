// Package memblob is an in-memory [blob.Bucket] and [blob.Signer] for tests.
package memblob

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // ETag only, not security.
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dungnh3/kitchen/blob"
)

var errClosed = errors.New("memblob: bucket closed")

type object struct {
	data  []byte
	attrs blob.Attrs
}

// Bucket is an in-memory bucket. The zero value is not usable; call [New].
type Bucket struct {
	mu      sync.RWMutex
	objects map[string]object
	closed  bool
	now     func() time.Time
}

var (
	_ blob.Bucket = (*Bucket)(nil)
	_ blob.Signer = (*Bucket)(nil)
)

// New returns an empty bucket.
func New() *Bucket {
	return &Bucket{objects: make(map[string]object), now: time.Now}
}

// Keys returns all stored keys in sorted order.
func (b *Bucket) Keys() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return slices.Sorted(maps.Keys(b.objects))
}

func validKey(key string) error {
	if key == "" || strings.HasPrefix(key, "/") {
		return fmt.Errorf("memblob: invalid key %q", key)
	}
	return nil
}

type writer struct {
	ctx  context.Context
	b    *Bucket
	key  string
	opts blob.WriterOptions
	buf  bytes.Buffer
	done bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.done {
		return 0, errors.New("memblob: write after close")
	}
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.buf.Write(p)
}

func (w *writer) Close() error {
	if w.done {
		return nil
	}
	w.done = true
	if err := w.ctx.Err(); err != nil {
		return fmt.Errorf("memblob: upload aborted: %w", err)
	}
	sum := md5.Sum(w.buf.Bytes()) //nolint:gosec // ETag only.
	w.b.mu.Lock()
	defer w.b.mu.Unlock()
	if w.b.closed {
		return errClosed
	}
	w.b.objects[w.key] = object{
		data: bytes.Clone(w.buf.Bytes()),
		attrs: blob.Attrs{
			Size:               int64(w.buf.Len()),
			ContentType:        w.opts.ContentType,
			CacheControl:       w.opts.CacheControl,
			ContentDisposition: w.opts.ContentDisposition,
			ETag:               hex.EncodeToString(sum[:]),
			Updated:            w.b.now(),
			Metadata:           maps.Clone(w.opts.Metadata),
		},
	}
	return nil
}

// NewWriter implements [blob.Bucket].
func (b *Bucket) NewWriter(ctx context.Context, key string, opts *blob.WriterOptions) (io.WriteCloser, error) {
	if err := validKey(key); err != nil {
		return nil, err
	}
	w := &writer{ctx: ctx, b: b, key: key}
	if opts != nil {
		w.opts = *opts
	}
	return w, nil
}

func (b *Bucket) get(key string) (object, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return object{}, errClosed
	}
	o, ok := b.objects[key]
	if !ok {
		return object{}, fmt.Errorf("memblob: %q: %w", key, blob.ErrNotFound)
	}
	return o, nil
}

// NewReader implements [blob.Bucket].
func (b *Bucket) NewReader(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o, err := b.get(key)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(o.data)), nil
}

// Attributes implements [blob.Bucket].
func (b *Bucket) Attributes(ctx context.Context, key string) (*blob.Attrs, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o, err := b.get(key)
	if err != nil {
		return nil, err
	}
	a := o.attrs
	a.Metadata = maps.Clone(a.Metadata)
	return &a, nil
}

// Delete implements [blob.Bucket].
func (b *Bucket) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return errClosed
	}
	if _, ok := b.objects[key]; !ok {
		return fmt.Errorf("memblob: %q: %w", key, blob.ErrNotFound)
	}
	delete(b.objects, key)
	return nil
}

// Close implements [blob.Bucket]. Later calls fail.
func (b *Bucket) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

// SignedURL returns a fake, unsigned "mem://" URL that encodes the request,
// so tests can assert what would have been signed.
func (b *Bucket) SignedURL(_ context.Context, key string, opts blob.SignOptions) (string, error) {
	if err := validKey(key); err != nil {
		return "", err
	}
	method := opts.Method
	if method == "" {
		method = "GET"
	}
	q := url.Values{"method": {method}}
	if opts.Expires > 0 {
		q.Set("expires", opts.Expires.String())
	}
	if opts.ContentType != "" {
		q.Set("content-type", opts.ContentType)
	}
	if opts.ResponseContentType != "" {
		q.Set("response-content-type", opts.ResponseContentType)
	}
	if opts.ResponseContentDisposition != "" {
		q.Set("response-content-disposition", opts.ResponseContentDisposition)
	}
	return (&url.URL{Scheme: "mem", Host: "bucket", Path: "/" + key, RawQuery: q.Encode()}).String(), nil
}

// PostPolicy returns a fake policy whose fields echo the options.
func (b *Bucket) PostPolicy(_ context.Context, key string, opts blob.PostPolicyOptions) (*blob.PostPolicy, error) {
	if err := validKey(key); err != nil {
		return nil, err
	}
	f := map[string]string{"key": key, "max-bytes": fmt.Sprint(opts.MaxBytes)}
	if opts.ContentType != "" {
		f["Content-Type"] = opts.ContentType
	}
	return &blob.PostPolicy{URL: "mem://bucket/", Fields: f}, nil
}

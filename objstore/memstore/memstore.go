// Package memstore is an in-memory [objstore.Bucket] and [objstore.Signer] for tests.
package memstore

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
	"sync"
	"time"

	"github.com/dungnh3/kitchen/objstore"
)

var errClosed = errors.New("memstore: bucket closed")

type object struct {
	data  []byte
	attrs objstore.Attrs
}

// Bucket is an in-memory bucket. The zero value is not usable; call [New].
type Bucket struct {
	mu      sync.RWMutex
	objects map[string]object
	closed  bool
	now     func() time.Time
}

var (
	_ objstore.Bucket = (*Bucket)(nil)
	_ objstore.Signer = (*Bucket)(nil)
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

func validKey(key string) error { return objstore.ValidKey(key) }

type writer struct {
	ctx  context.Context
	b    *Bucket
	key  string
	opts objstore.WriterOptions
	buf  bytes.Buffer
	done bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.done {
		return 0, errors.New("memstore: write after close")
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
		return fmt.Errorf("memstore: upload aborted: %w", err)
	}
	sum := md5.Sum(w.buf.Bytes()) //nolint:gosec // ETag only.
	w.b.mu.Lock()
	defer w.b.mu.Unlock()
	if w.b.closed {
		return errClosed
	}
	if _, exists := w.b.objects[w.key]; exists && w.opts.IfNotExists {
		return fmt.Errorf("memstore: %q: %w", w.key, objstore.ErrExists)
	}
	w.b.objects[w.key] = object{
		data: bytes.Clone(w.buf.Bytes()),
		attrs: objstore.Attrs{
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

// NewWriter implements [objstore.Bucket].
func (b *Bucket) NewWriter(ctx context.Context, key string, opts *objstore.WriterOptions) (io.WriteCloser, error) {
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
	if err := validKey(key); err != nil {
		return object{}, err
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return object{}, errClosed
	}
	o, ok := b.objects[key]
	if !ok {
		return object{}, fmt.Errorf("memstore: %q: %w", key, objstore.ErrNotFound)
	}
	return o, nil
}

// NewReader implements [objstore.Bucket].
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

// Attributes implements [objstore.Bucket].
func (b *Bucket) Attributes(ctx context.Context, key string) (*objstore.Attrs, error) {
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

// Delete implements [objstore.Bucket].
func (b *Bucket) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validKey(key); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return errClosed
	}
	if _, ok := b.objects[key]; !ok {
		return fmt.Errorf("memstore: %q: %w", key, objstore.ErrNotFound)
	}
	delete(b.objects, key)
	return nil
}

// Close implements [objstore.Bucket]. Later calls fail.
func (b *Bucket) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

// SignedURL returns a fake, unsigned "mem://" URL that encodes the request,
// so tests can assert what would have been signed.
func (b *Bucket) SignedURL(_ context.Context, key string, opts objstore.SignOptions) (string, error) {
	if err := opts.Check(key); err != nil {
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
func (b *Bucket) PostPolicy(_ context.Context, key string, opts objstore.PostPolicyOptions) (*objstore.PostPolicy, error) {
	maxBytes := opts.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 20 << 20
	}
	if err := opts.Check(key, maxBytes); err != nil {
		return nil, err
	}
	f := map[string]string{"key": key, "max-bytes": fmt.Sprint(opts.MaxBytes)}
	if opts.ContentType != "" {
		f["Content-Type"] = opts.ContentType
	}
	if opts.ContentTypePrefix != "" {
		f["content-type-prefix"] = opts.ContentTypePrefix
	}
	return &objstore.PostPolicy{URL: "mem://bucket/", Fields: f}, nil
}

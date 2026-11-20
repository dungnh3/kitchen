// Package blob defines a small, backend-neutral object storage API.
//
// [Bucket] covers reads, writes, attributes and deletes; [Signer] covers
// browser-direct uploads and downloads (signed URLs and POST policies).
// Implementations live in their own packages: blob/memblob (in-memory, for
// tests) in this module and blob/gcs (Google Cloud Storage) as a submodule.
package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

// ErrNotFound is returned (possibly wrapped) when an object does not exist.
var ErrNotFound = errors.New("blob: object not found")

// Attrs describes a stored object.
type Attrs struct {
	Size               int64
	ContentType        string
	CacheControl       string
	ContentDisposition string
	ETag               string
	Updated            time.Time
	Metadata           map[string]string
}

// WriterOptions configures a new object. A nil *WriterOptions means defaults.
type WriterOptions struct {
	ContentType        string
	CacheControl       string
	ContentDisposition string
	Metadata           map[string]string
	// ChunkSize is a backend hint for resumable uploads, in bytes. Nil keeps
	// the backend default; for GCS, 0 means a single request without retries.
	ChunkSize *int
}

// Bucket is a flat namespace of objects addressed by slash-separated keys
// ("avatars/2025/user-42.jpg"). Keys never start with a slash.
//
// Implementations must be safe for concurrent use.
type Bucket interface {
	// NewWriter starts writing key. The object becomes visible only after
	// Close returns nil. Cancel ctx to abort an in-flight write; Close then
	// returns an error and nothing is stored.
	NewWriter(ctx context.Context, key string, opts *WriterOptions) (io.WriteCloser, error)
	// NewReader opens key for reading. It returns ErrNotFound when missing.
	NewReader(ctx context.Context, key string) (io.ReadCloser, error)
	// Attributes returns key's metadata, or ErrNotFound.
	Attributes(ctx context.Context, key string) (*Attrs, error)
	// Delete removes key. It returns ErrNotFound when missing.
	Delete(ctx context.Context, key string) error
	// Close releases the bucket's resources.
	Close() error
}

// SignOptions configures a signed URL.
type SignOptions struct {
	// Method is the HTTP method the URL is valid for; default GET.
	Method string
	// Expires is how long the URL stays valid; zero means the signer default.
	Expires time.Duration
	// ContentType, if set, must be sent by the client on PUT.
	ContentType string
	// ResponseContentType overrides Content-Type on GET responses.
	ResponseContentType string
	// ResponseContentDisposition overrides Content-Disposition on GET
	// responses, e.g. `attachment; filename="report.pdf"`.
	ResponseContentDisposition string
}

// PostPolicyOptions configures an HTML-form (POST policy) upload.
type PostPolicyOptions struct {
	// Expires is how long the policy stays valid; zero means the signer default.
	Expires time.Duration
	// MinBytes and MaxBytes bound the upload size. MaxBytes zero means the
	// signer default.
	MinBytes, MaxBytes int64
	// ContentType, if set, is enforced by the policy.
	ContentType string
}

// PostPolicy is what a browser needs to upload with a multipart form: POST to
// URL with Fields as form fields followed by the "file" field.
type PostPolicy struct {
	URL    string            `json:"url"`
	Fields map[string]string `json:"fields"`
}

// Signer creates credentials that let a client talk to the storage backend
// directly.
type Signer interface {
	SignedURL(ctx context.Context, key string, opts SignOptions) (string, error)
	PostPolicy(ctx context.Context, key string, opts PostPolicyOptions) (*PostPolicy, error)
}

// Put writes r to key. On error the partial upload is aborted.
func Put(ctx context.Context, b Bucket, key string, r io.Reader, opts *WriterOptions) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := b.NewWriter(ctx, key, opts)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, r); err != nil {
		cancel()
		_ = w.Close()
		return fmt.Errorf("blob: write %q: %w", key, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("blob: finalize %q: %w", key, err)
	}
	return nil
}

// Exists reports whether key exists.
func Exists(ctx context.Context, b Bucket, key string) (bool, error) {
	_, err := b.Attributes(ctx, key)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, ErrNotFound):
		return false, nil
	default:
		return false, err
	}
}

// ReadAll reads the whole object at key.
func ReadAll(ctx context.Context, b Bucket, key string) ([]byte, error) {
	r, err := b.NewReader(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	return io.ReadAll(r)
}

// Package objstore defines a small, backend-neutral object storage API.
//
// [Bucket] covers reads, writes, attributes and deletes; [Signer] covers
// browser-direct uploads and downloads (signed URLs and POST policies).
// Implementations live in their own packages: objstore/memstore (in-memory, for
// tests) in this module, and objstore/gcs (Google Cloud Storage) and
// objstore/s3 (Amazon S3 and compatible stores) as submodules.
package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode"

	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
)

// Errors returned (possibly wrapped) by every backend. They are errs errors,
// so a kitchen server answers 404, 409 or 400 instead of 500.
var (
	// ErrNotFound: the object does not exist.
	ErrNotFound = errs.New(codes.NotFound, "OBJECT_NOT_FOUND", errs.Message("object not found"))
	// ErrExists: WriterOptions.IfNotExists was set and the object exists.
	ErrExists = errs.New(codes.AlreadyExists, "OBJECT_ALREADY_EXISTS", errs.Message("object already exists"))
	// ErrInvalidKey: the key is rejected by ValidKey.
	ErrInvalidKey = errs.New(codes.InvalidArgument, "INVALID_OBJECT_KEY", errs.Message("invalid object key"))
	// ErrInvalidOptions: sign or upload options are unsafe or inconsistent.
	ErrInvalidOptions = errs.New(codes.InvalidArgument, "INVALID_UPLOAD_OPTIONS", errs.Message("invalid upload options"))
)

// MaxKeyLen is the longest key accepted by ValidKey (the GCS and S3 limit).
const MaxKeyLen = 1024

// ValidKey reports whether key is a safe object key: non-empty, at most
// MaxKeyLen bytes of valid UTF-8, no leading slash, no empty, "." or ".."
// segment, and no control characters. It returns an error wrapping
// ErrInvalidKey. Every backend checks it on every operation.
func ValidKey(key string) error {
	bad := func(why string) error {
		return errs.Wrap(fmt.Errorf("objstore: key %q: %s", key, why), ErrInvalidKey)
	}
	switch {
	case key == "":
		return bad("empty")
	case len(key) > MaxKeyLen:
		return bad("too long")
	case !utf8Valid(key):
		return bad("not valid UTF-8")
	case strings.HasPrefix(key, "/"):
		return bad("leading slash")
	}
	for _, r := range key {
		if unicode.IsControl(r) {
			return bad("control character")
		}
	}
	for seg := range strings.SplitSeq(key, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return bad("empty, . or .. segment")
		}
	}
	return nil
}

func utf8Valid(s string) bool { return strings.ToValidUTF8(s, "\uFFFD") == s }

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
	// the backend default; for GCS, 0 means a single request.
	ChunkSize *int
	// IfNotExists makes the write fail with ErrExists when the object
	// already exists (a precondition, so it also lets the backend retry the
	// write safely).
	IfNotExists bool
}

// Bucket is a flat namespace of objects addressed by slash-separated keys
// ("avatars/2025/user-42.jpg"). Keys never start with a slash.
//
// Implementations must be safe for concurrent use.
type Bucket interface {
	// NewWriter starts writing key. The object is not visible before Close
	// and is visible once Close returns nil. Cancel ctx to abort an
	// in-flight write; Close then returns an error and no object is created.
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

// SignOptions configures a signed URL. See Check for the rules applied to
// upload (PUT) URLs.
type SignOptions struct {
	// Method is the HTTP method the URL is valid for; default GET.
	Method string
	// Expires is how long the URL stays valid; zero means the signer default.
	Expires time.Duration
	// ContentType must be sent by the client on PUT. Required for PUT unless
	// AllowAnyContentType is set.
	ContentType string
	// MaxBytes bounds a PUT upload where the backend supports it (GCS: the
	// client must send the signed x-goog-content-length-range header; S3
	// cannot, see AllowUnboundedSize). Zero means the signer default.
	MaxBytes int64
	// AllowAnyContentType lets a PUT URL accept any content type. Uploads
	// served from your CDN domain can then carry HTML or SVG (stored XSS):
	// only set it for private buckets.
	AllowAnyContentType bool
	// AllowOverwrite lets a PUT URL replace an existing object. By default
	// the URL only creates (GCS if-generation-match: 0, S3 If-None-Match: *).
	AllowOverwrite bool
	// AllowUnboundedSize lets a backend that cannot bound the size of a PUT
	// upload (S3) sign one anyway. Prefer PostPolicy, which can.
	AllowUnboundedSize bool
	// ResponseContentType overrides Content-Type on GET responses.
	ResponseContentType string
	// ResponseContentDisposition overrides Content-Disposition on GET
	// responses, e.g. `attachment; filename="report.pdf"`.
	ResponseContentDisposition string
}

// PostPolicyOptions configures an HTML-form (POST policy) upload. The
// policy is bound to the exact key; use a fresh key (NewObjectKey) per
// upload, since a POST policy cannot forbid overwriting.
type PostPolicyOptions struct {
	// Expires is how long the policy stays valid; zero means the signer default.
	Expires time.Duration
	// MinBytes and MaxBytes bound the upload size. MaxBytes zero means the
	// signer default.
	MinBytes, MaxBytes int64
	// ContentType is enforced exactly by the policy.
	ContentType string
	// ContentTypePrefix is enforced as a prefix ("image/"). One of
	// ContentType and ContentTypePrefix is required unless
	// AllowAnyContentType is set.
	ContentTypePrefix string
	// AllowAnyContentType accepts any content type (see
	// SignOptions.AllowAnyContentType).
	AllowAnyContentType bool
}

// Check validates key and opts for a signer: a PUT URL needs a ContentType
// unless AllowAnyContentType; MaxBytes must not be negative. Backends call
// it; it returns an error wrapping ErrInvalidKey or ErrInvalidOptions.
func (o SignOptions) Check(key string) error {
	if err := ValidKey(key); err != nil {
		return err
	}
	if o.MaxBytes < 0 {
		return invalidOpts("negative MaxBytes")
	}
	if strings.EqualFold(o.Method, http.MethodPut) && o.ContentType == "" && !o.AllowAnyContentType {
		return invalidOpts("a PUT URL needs ContentType (or AllowAnyContentType)")
	}
	return nil
}

// Check validates key and opts for a signer: a content type rule is
// required unless AllowAnyContentType, and the size range must be valid
// (maxBytes is MaxBytes or the signer default).
func (o PostPolicyOptions) Check(key string, maxBytes int64) error {
	if err := ValidKey(key); err != nil {
		return err
	}
	if o.MinBytes < 0 || maxBytes <= 0 || o.MinBytes > maxBytes {
		return invalidOpts(fmt.Sprintf("invalid size range [%d, %d]", o.MinBytes, maxBytes))
	}
	if o.ContentType == "" && o.ContentTypePrefix == "" && !o.AllowAnyContentType {
		return invalidOpts("a POST policy needs ContentType or ContentTypePrefix (or AllowAnyContentType)")
	}
	return nil
}

func invalidOpts(why string) error {
	return errs.Wrap(errors.New("objstore: "+why), ErrInvalidOptions)
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
		return fmt.Errorf("objstore: write %q: %w", key, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("objstore: finalize %q: %w", key, err)
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

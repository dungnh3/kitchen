// Package gcs implements [blob.Bucket] and [blob.Signer] on Google Cloud
// Storage.
//
//	b, err := gcs.Open(ctx, gcs.Config{Bucket: "media", CDNBaseURL: "https://cdn.example.com"})
//	defer b.Close()
//	err = blob.Put(ctx, b, key, r, &blob.WriterOptions{ContentType: "image/png"})
//	pp, err := b.PostPolicy(ctx, key, blob.PostPolicyOptions{MaxBytes: 20 << 20})
package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/dungnh3/kitchen/blob"
)

// Config configures a Bucket.
type Config struct {
	// Bucket is the GCS bucket name. Required.
	Bucket string
	// CDNBaseURL is the public base URL used by PublicURL, e.g.
	// "https://cdn.example.com". Optional.
	CDNBaseURL string
	// SignExpiry is the default validity of signed URLs and POST policies
	// (default 5m, max 7 days).
	SignExpiry time.Duration
	// MaxUploadBytes is the default POST policy size limit (default 20 MiB).
	MaxUploadBytes int64
	// GoogleAccessID and PrivateKey sign URLs locally with a service account
	// key. When empty, the client signs with the attached service account
	// (metadata server or IAM signBlob).
	GoogleAccessID string
	PrivateKey     []byte
}

// Bucket is a GCS bucket. It is safe for concurrent use.
type Bucket struct {
	cfg       Config
	client    *storage.Client
	bkt       *storage.BucketHandle
	ownClient bool
}

var (
	_ blob.Bucket = (*Bucket)(nil)
	_ blob.Signer = (*Bucket)(nil)
)

// Open creates a storage client with opts and returns a Bucket owning it.
func Open(ctx context.Context, cfg Config, opts ...option.ClientOption) (*Bucket, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	c, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("gcs: new client: %w", err)
	}
	b, err := New(c, cfg)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	b.ownClient = true
	return b, nil
}

// New wraps an existing client. Close does not close client.
func New(client *storage.Client, cfg Config) (*Bucket, error) {
	if client == nil {
		return nil, errors.New("gcs: nil client")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.SignExpiry <= 0 {
		cfg.SignExpiry = 5 * time.Minute
	}
	if cfg.MaxUploadBytes <= 0 {
		cfg.MaxUploadBytes = 20 << 20
	}
	return &Bucket{cfg: cfg, client: client, bkt: client.Bucket(cfg.Bucket)}, nil
}

func (cfg Config) validate() error {
	if cfg.Bucket == "" {
		return errors.New("gcs: Config.Bucket is required")
	}
	if cfg.CDNBaseURL != "" {
		if u, err := url.Parse(cfg.CDNBaseURL); err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("gcs: invalid CDNBaseURL %q", cfg.CDNBaseURL)
		}
	}
	if cfg.SignExpiry > 7*24*time.Hour {
		return errors.New("gcs: SignExpiry exceeds the V4 maximum of 7 days")
	}
	return nil
}

// Close closes the client if the Bucket owns it.
func (b *Bucket) Close() error {
	if b.ownClient {
		return b.client.Close()
	}
	return nil
}

// Handle returns the underlying bucket handle for features this package does
// not cover.
func (b *Bucket) Handle() *storage.BucketHandle { return b.bkt }

func mapErr(key string, err error) error {
	if errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("gcs: %q: %w", key, blob.ErrNotFound)
	}
	return fmt.Errorf("gcs: %q: %w", key, err)
}

// NewWriter implements [blob.Bucket]. Cancel ctx to abort the upload.
func (b *Bucket) NewWriter(ctx context.Context, key string, opts *blob.WriterOptions) (io.WriteCloser, error) {
	if key == "" || strings.HasPrefix(key, "/") {
		return nil, fmt.Errorf("gcs: invalid key %q", key)
	}
	w := b.bkt.Object(key).NewWriter(ctx)
	if opts != nil {
		w.ContentType = opts.ContentType
		w.CacheControl = opts.CacheControl
		w.ContentDisposition = opts.ContentDisposition
		w.Metadata = maps.Clone(opts.Metadata)
		if opts.ChunkSize != nil {
			w.ChunkSize = *opts.ChunkSize
		}
	}
	return w, nil
}

// NewReader implements [blob.Bucket].
func (b *Bucket) NewReader(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := b.bkt.Object(key).NewReader(ctx)
	if err != nil {
		return nil, mapErr(key, err)
	}
	return r, nil
}

// Attributes implements [blob.Bucket].
func (b *Bucket) Attributes(ctx context.Context, key string) (*blob.Attrs, error) {
	a, err := b.bkt.Object(key).Attrs(ctx)
	if err != nil {
		return nil, mapErr(key, err)
	}
	return &blob.Attrs{
		Size:               a.Size,
		ContentType:        a.ContentType,
		CacheControl:       a.CacheControl,
		ContentDisposition: a.ContentDisposition,
		ETag:               a.Etag,
		Updated:            a.Updated,
		Metadata:           a.Metadata,
	}, nil
}

// Delete implements [blob.Bucket].
func (b *Bucket) Delete(ctx context.Context, key string) error {
	if err := b.bkt.Object(key).Delete(ctx); err != nil {
		return mapErr(key, err)
	}
	return nil
}

// PublicURL returns CDNBaseURL joined with key (escaped). It returns an error
// when CDNBaseURL is not configured.
func (b *Bucket) PublicURL(key string) (string, error) {
	if b.cfg.CDNBaseURL == "" {
		return "", errors.New("gcs: CDNBaseURL is not configured")
	}
	return blob.PublicURL(b.cfg.CDNBaseURL, key)
}

func (b *Bucket) expiry(d time.Duration) time.Time {
	if d <= 0 {
		d = b.cfg.SignExpiry
	}
	return time.Now().Add(d)
}

// SignedURL implements [blob.Signer] with V4 signing. Response overrides are
// part of the signature, so clients cannot change them.
func (b *Bucket) SignedURL(_ context.Context, key string, opts blob.SignOptions) (string, error) {
	if key == "" {
		return "", errors.New("gcs: empty key")
	}
	method := opts.Method
	if method == "" {
		method = "GET"
	}
	q := url.Values{}
	if opts.ResponseContentType != "" {
		q.Set("response-content-type", opts.ResponseContentType)
	}
	if opts.ResponseContentDisposition != "" {
		q.Set("response-content-disposition", opts.ResponseContentDisposition)
	}
	u, err := b.bkt.SignedURL(key, &storage.SignedURLOptions{
		GoogleAccessID:  b.cfg.GoogleAccessID,
		PrivateKey:      b.cfg.PrivateKey,
		Scheme:          storage.SigningSchemeV4,
		Method:          method,
		Expires:         b.expiry(opts.Expires),
		ContentType:     opts.ContentType,
		QueryParameters: q,
	})
	if err != nil {
		return "", fmt.Errorf("gcs: sign %q: %w", key, err)
	}
	return u, nil
}

// PostPolicy implements [blob.Signer] with a V4 POST policy whose
// content-length-range is [MinBytes, MaxBytes] (MaxBytes defaults to
// Config.MaxUploadBytes).
func (b *Bucket) PostPolicy(_ context.Context, key string, opts blob.PostPolicyOptions) (*blob.PostPolicy, error) {
	if key == "" {
		return nil, errors.New("gcs: empty key")
	}
	maxBytes := opts.MaxBytes
	if maxBytes <= 0 {
		maxBytes = b.cfg.MaxUploadBytes
	}
	if opts.MinBytes < 0 || opts.MinBytes > maxBytes {
		return nil, fmt.Errorf("gcs: invalid size range [%d, %d]", opts.MinBytes, maxBytes)
	}
	po := &storage.PostPolicyV4Options{
		GoogleAccessID: b.cfg.GoogleAccessID,
		PrivateKey:     b.cfg.PrivateKey,
		Expires:        b.expiry(opts.Expires),
		Conditions: []storage.PostPolicyV4Condition{
			storage.ConditionContentLengthRange(uint64(opts.MinBytes), uint64(maxBytes)), //nolint:gosec // validated non-negative above.
		},
	}
	if opts.ContentType != "" {
		po.Fields = &storage.PolicyV4Fields{ContentType: opts.ContentType}
	}
	p, err := b.bkt.GenerateSignedPostPolicyV4(key, po)
	if err != nil {
		return nil, fmt.Errorf("gcs: post policy %q: %w", key, err)
	}
	return &blob.PostPolicy{URL: p.URL, Fields: maps.Clone(p.Fields)}, nil
}

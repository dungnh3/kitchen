// Package gcs implements [objstore.Bucket] and [objstore.Signer] on Google Cloud
// Storage.
//
//	b, err := gcs.Open(ctx, gcs.Config{Bucket: "media", CDNBaseURL: "https://cdn.example.com"})
//	defer b.Close()
//	err = objstore.Put(ctx, b, key, r, &objstore.WriterOptions{ContentType: "image/png"})
//	pp, err := b.PostPolicy(ctx, key, objstore.PostPolicyOptions{MaxBytes: 20 << 20, ContentTypePrefix: "image/"})
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
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/objstore"
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
	// MaxUploadBytes is the default size limit of PUT URLs and POST policies
	// (default 20 MiB).
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
	_ objstore.Bucket = (*Bucket)(nil)
	_ objstore.Signer = (*Bucket)(nil)
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
	wrapped := fmt.Errorf("gcs: %q: %w", key, err)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return errs.Wrap(wrapped, objstore.ErrNotFound)
	}
	if ae, ok := errors.AsType[*googleapi.Error](err); ok && ae.Code == 412 {
		return errs.Wrap(wrapped, objstore.ErrExists)
	}
	return wrapped
}

// writer maps the precondition failure reported by Close.
type writer struct {
	*storage.Writer
	key string
}

func (w writer) Close() error {
	if err := w.Writer.Close(); err != nil {
		return mapErr(w.key, err)
	}
	return nil
}

// NewWriter implements [objstore.Bucket]. Cancel ctx to abort the upload.
func (b *Bucket) NewWriter(ctx context.Context, key string, opts *objstore.WriterOptions) (io.WriteCloser, error) {
	if err := objstore.ValidKey(key); err != nil {
		return nil, err
	}
	obj := b.bkt.Object(key)
	if opts != nil && opts.IfNotExists {
		obj = obj.If(storage.Conditions{DoesNotExist: true})
	}
	w := obj.NewWriter(ctx)
	if opts != nil {
		w.ContentType = opts.ContentType
		w.CacheControl = opts.CacheControl
		w.ContentDisposition = opts.ContentDisposition
		w.Metadata = maps.Clone(opts.Metadata)
		if opts.ChunkSize != nil {
			w.ChunkSize = *opts.ChunkSize
		}
	}
	return writer{w, key}, nil
}

// NewReader implements [objstore.Bucket].
func (b *Bucket) NewReader(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := objstore.ValidKey(key); err != nil {
		return nil, err
	}
	r, err := b.bkt.Object(key).NewReader(ctx)
	if err != nil {
		return nil, mapErr(key, err)
	}
	return r, nil
}

// Attributes implements [objstore.Bucket].
func (b *Bucket) Attributes(ctx context.Context, key string) (*objstore.Attrs, error) {
	if err := objstore.ValidKey(key); err != nil {
		return nil, err
	}
	a, err := b.bkt.Object(key).Attrs(ctx)
	if err != nil {
		return nil, mapErr(key, err)
	}
	return &objstore.Attrs{
		Size:               a.Size,
		ContentType:        a.ContentType,
		CacheControl:       a.CacheControl,
		ContentDisposition: a.ContentDisposition,
		ETag:               a.Etag,
		Updated:            a.Updated,
		Metadata:           a.Metadata,
	}, nil
}

// Delete implements [objstore.Bucket].
func (b *Bucket) Delete(ctx context.Context, key string) error {
	if err := objstore.ValidKey(key); err != nil {
		return err
	}
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
	return objstore.PublicURL(b.cfg.CDNBaseURL, key)
}

func (b *Bucket) expiry(d time.Duration) time.Time {
	if d <= 0 {
		d = b.cfg.SignExpiry
	}
	return time.Now().Add(d)
}

// SignedURL implements [objstore.Signer] with V4 signing. Response overrides are
// part of the signature, so clients cannot change them. A PUT URL signs
// x-goog-content-length-range (0 to MaxBytes, default
// Config.MaxUploadBytes) and, unless AllowOverwrite, x-goog-if-generation-match: 0;
// the client must send both headers as given.
func (b *Bucket) SignedURL(_ context.Context, key string, opts objstore.SignOptions) (string, error) {
	if err := opts.Check(key); err != nil {
		return "", err
	}
	method := strings.ToUpper(opts.Method)
	if method == "" {
		method = "GET"
	}
	var headers []string
	if method == "PUT" {
		maxBytes := opts.MaxBytes
		if maxBytes == 0 {
			maxBytes = b.cfg.MaxUploadBytes
		}
		headers = append(headers, fmt.Sprintf("x-goog-content-length-range:0,%d", maxBytes))
		if !opts.AllowOverwrite {
			headers = append(headers, "x-goog-if-generation-match:0")
		}
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
		Headers:         headers,
		QueryParameters: q,
	})
	if err != nil {
		return "", fmt.Errorf("gcs: sign %q: %w", key, err)
	}
	return u, nil
}

// PostPolicy implements [objstore.Signer] with a V4 POST policy bound to key,
// whose content-length-range is [MinBytes, MaxBytes] (MaxBytes defaults to
// Config.MaxUploadBytes) and whose Content-Type is fixed or prefixed.
func (b *Bucket) PostPolicy(_ context.Context, key string, opts objstore.PostPolicyOptions) (*objstore.PostPolicy, error) {
	maxBytes := opts.MaxBytes
	if maxBytes <= 0 {
		maxBytes = b.cfg.MaxUploadBytes
	}
	if err := opts.Check(key, maxBytes); err != nil {
		return nil, err
	}
	po := &storage.PostPolicyV4Options{
		GoogleAccessID: b.cfg.GoogleAccessID,
		PrivateKey:     b.cfg.PrivateKey,
		Expires:        b.expiry(opts.Expires),
		Conditions: []storage.PostPolicyV4Condition{
			storage.ConditionContentLengthRange(uint64(opts.MinBytes), uint64(maxBytes)), //nolint:gosec // validated non-negative above.
		},
	}
	switch {
	case opts.ContentType != "":
		po.Fields = &storage.PolicyV4Fields{ContentType: opts.ContentType}
	case opts.ContentTypePrefix != "":
		po.Conditions = append(po.Conditions, storage.ConditionStartsWith("$Content-Type", opts.ContentTypePrefix))
	}
	p, err := b.bkt.GenerateSignedPostPolicyV4(key, po)
	if err != nil {
		return nil, fmt.Errorf("gcs: post policy %q: %w", key, err)
	}
	return &objstore.PostPolicy{URL: p.URL, Fields: maps.Clone(p.Fields)}, nil
}

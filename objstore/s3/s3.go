// Package s3 implements [objstore.Bucket] and [objstore.Signer] on Amazon S3 (and
// S3-compatible stores such as MinIO or Cloudflare R2).
//
//	b, err := s3.Open(ctx, s3.Config{Bucket: "media", CDNBaseURL: "https://cdn.example.com"})
//	defer b.Close()
//	tb := objstore.Instrument(b, objstore.WithSystem("s3")) // traced
//	err = objstore.Put(ctx, tb, key, r, &objstore.WriterOptions{ContentType: "image/png", IfNotExists: true})
//	pp, err := b.PostPolicy(ctx, key, objstore.PostPolicyOptions{ContentTypePrefix: "image/"})
//
// Differences from GCS: Delete of a missing key costs an extra HEAD to
// report objstore.ErrNotFound (S3 deletes are silent), and S3 cannot bound the
// size of a presigned PUT, so SignedURL refuses PUT unless
// SignOptions.AllowUnboundedSize; use PostPolicy for browser uploads.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/objstore"
)

// Defaults.
const (
	DefaultSignExpiry     = 5 * time.Minute
	DefaultMaxUploadBytes = 20 << 20
	DefaultPartSize       = 8 << 20
	minPartSize           = 5 << 20 // S3 minimum for all parts but the last
	maxSignExpiry         = 7 * 24 * time.Hour
)

// Config configures a Bucket.
type Config struct {
	// Bucket is the S3 bucket name. Required.
	Bucket string
	// CDNBaseURL is the public base URL used by PublicURL, e.g.
	// "https://cdn.example.com". Optional.
	CDNBaseURL string
	// SignExpiry is the default validity of signed URLs and POST policies
	// (default 5m, max 7 days).
	SignExpiry time.Duration
	// MaxUploadBytes is the default POST policy size limit (default 20 MiB).
	MaxUploadBytes int64
	// PartSize is the multipart part size for writes, and the size below
	// which a write is a single PutObject (default 8 MiB, min 5 MiB). A
	// writer buffers one part in memory.
	PartSize int64
}

// Bucket is an S3 bucket. It is safe for concurrent use.
type Bucket struct {
	cfg     Config
	client  *s3.Client
	presign *s3.PresignClient
}

var (
	_ objstore.Bucket = (*Bucket)(nil)
	_ objstore.Signer = (*Bucket)(nil)
)

// Open loads the default AWS configuration (environment, shared config,
// IAM role) with optFns and returns a Bucket.
func Open(ctx context.Context, cfg Config, optFns ...func(*config.LoadOptions) error) (*Bucket, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("s3: load AWS config: %w", err)
	}
	return New(s3.NewFromConfig(awsCfg), cfg)
}

// New wraps an existing client.
func New(client *s3.Client, cfg Config) (*Bucket, error) {
	if client == nil {
		return nil, errors.New("s3: nil client")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.SignExpiry <= 0 {
		cfg.SignExpiry = DefaultSignExpiry
	}
	if cfg.MaxUploadBytes <= 0 {
		cfg.MaxUploadBytes = DefaultMaxUploadBytes
	}
	if cfg.PartSize <= 0 {
		cfg.PartSize = DefaultPartSize
	}
	return &Bucket{cfg: cfg, client: client, presign: s3.NewPresignClient(client)}, nil
}

func (cfg Config) validate() error {
	if cfg.Bucket == "" {
		return errors.New("s3: Config.Bucket is required")
	}
	if cfg.CDNBaseURL != "" {
		if u, err := url.Parse(cfg.CDNBaseURL); err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("s3: invalid CDNBaseURL %q", cfg.CDNBaseURL)
		}
	}
	if cfg.SignExpiry > maxSignExpiry {
		return errors.New("s3: SignExpiry exceeds the SigV4 maximum of 7 days")
	}
	if cfg.PartSize != 0 && cfg.PartSize < minPartSize {
		return fmt.Errorf("s3: PartSize must be at least %d bytes", minPartSize)
	}
	return nil
}

// Close is a no-op: the AWS client holds no resources to release.
func (b *Bucket) Close() error { return nil }

// Client returns the underlying client for features this package does not
// cover.
func (b *Bucket) Client() *s3.Client { return b.client }

func mapErr(key string, err error) error {
	wrapped := fmt.Errorf("s3: %q: %w", key, err)
	var nsk *types.NoSuchKey
	var nf *types.NotFound
	if errors.As(err, &nsk) || errors.As(err, &nf) {
		return errs.Wrap(wrapped, objstore.ErrNotFound)
	}
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return errs.Wrap(wrapped, objstore.ErrNotFound)
		case "PreconditionFailed", "ConditionalRequestConflict":
			return errs.Wrap(wrapped, objstore.ErrExists)
		}
	}
	if re, ok := errors.AsType[*awshttp.ResponseError](err); ok {
		switch re.HTTPStatusCode() {
		case http.StatusNotFound:
			return errs.Wrap(wrapped, objstore.ErrNotFound)
		case http.StatusPreconditionFailed:
			return errs.Wrap(wrapped, objstore.ErrExists)
		}
	}
	return wrapped
}

// NewWriter implements [objstore.Bucket]. Data is buffered one part at a time:
// a write smaller than Config.PartSize is a single PutObject, a larger one a
// multipart upload that is aborted on error or when ctx is cancelled.
// IfNotExists applies in both cases (If-None-Match: *). Close must be
// called.
func (b *Bucket) NewWriter(ctx context.Context, key string, opts *objstore.WriterOptions) (io.WriteCloser, error) {
	if err := objstore.ValidKey(key); err != nil {
		return nil, err
	}
	w := &writer{ctx: ctx, b: b, key: key}
	if opts != nil {
		w.opts = *opts
		w.opts.Metadata = maps.Clone(opts.Metadata)
	}
	return w, nil
}

type writer struct {
	ctx      context.Context
	b        *Bucket
	key      string
	opts     objstore.WriterOptions
	buf      bytes.Buffer
	uploadID *string
	parts    []types.CompletedPart
	err      error
	closed   bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("s3: write after close")
	}
	if w.err != nil {
		return 0, w.err
	}
	if err := w.ctx.Err(); err != nil {
		w.fail(err)
		return 0, err
	}
	n, _ := w.buf.Write(p)
	for int64(w.buf.Len()) >= w.b.cfg.PartSize {
		if err := w.flushPart(w.b.cfg.PartSize); err != nil {
			w.fail(err)
			return n, err
		}
	}
	return n, nil
}

func (w *writer) flushPart(size int64) error {
	if w.uploadID == nil {
		out, err := w.b.client.CreateMultipartUpload(w.ctx, &s3.CreateMultipartUploadInput{
			Bucket:             &w.b.cfg.Bucket,
			Key:                &w.key,
			ContentType:        nonEmpty(w.opts.ContentType),
			CacheControl:       nonEmpty(w.opts.CacheControl),
			ContentDisposition: nonEmpty(w.opts.ContentDisposition),
			Metadata:           w.opts.Metadata,
		})
		if err != nil {
			return mapErr(w.key, err)
		}
		w.uploadID = out.UploadId
	}
	part := bytes.Clone(w.buf.Next(int(size)))
	num := int32(len(w.parts) + 1) //nolint:gosec // S3 allows at most 10000 parts.
	out, err := w.b.client.UploadPart(w.ctx, &s3.UploadPartInput{
		Bucket:     &w.b.cfg.Bucket,
		Key:        &w.key,
		UploadId:   w.uploadID,
		PartNumber: &num,
		Body:       bytes.NewReader(part),
	})
	if err != nil {
		return mapErr(w.key, err)
	}
	w.parts = append(w.parts, types.CompletedPart{ETag: out.ETag, PartNumber: &num})
	return nil
}

// fail records err and aborts a started multipart upload.
func (w *writer) fail(err error) {
	if w.err == nil {
		w.err = err
	}
	if w.uploadID != nil {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), 30*time.Second)
		defer cancel()
		_, _ = w.b.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: &w.b.cfg.Bucket, Key: &w.key, UploadId: w.uploadID,
		})
		w.uploadID = nil
	}
}

func (w *writer) Close() error {
	if w.closed {
		return w.err
	}
	w.closed = true
	if w.err != nil {
		return w.err
	}
	if err := w.ctx.Err(); err != nil {
		w.fail(err)
		return fmt.Errorf("s3: upload aborted: %w", err)
	}
	var ifNoneMatch *string
	if w.opts.IfNotExists {
		ifNoneMatch = aws.String("*")
	}
	if w.uploadID == nil {
		_, err := w.b.client.PutObject(w.ctx, &s3.PutObjectInput{
			Bucket:             &w.b.cfg.Bucket,
			Key:                &w.key,
			Body:               bytes.NewReader(w.buf.Bytes()),
			ContentLength:      aws.Int64(int64(w.buf.Len())),
			ContentType:        nonEmpty(w.opts.ContentType),
			CacheControl:       nonEmpty(w.opts.CacheControl),
			ContentDisposition: nonEmpty(w.opts.ContentDisposition),
			Metadata:           w.opts.Metadata,
			IfNoneMatch:        ifNoneMatch,
		})
		if err != nil {
			w.err = mapErr(w.key, err)
		}
		return w.err
	}
	if w.buf.Len() > 0 {
		if err := w.flushPart(int64(w.buf.Len())); err != nil {
			w.fail(err)
			return w.err
		}
	}
	_, err := w.b.client.CompleteMultipartUpload(w.ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          &w.b.cfg.Bucket,
		Key:             &w.key,
		UploadId:        w.uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: w.parts},
		IfNoneMatch:     ifNoneMatch,
	})
	if err != nil {
		w.fail(mapErr(w.key, err))
		return w.err
	}
	w.uploadID = nil
	return nil
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// NewReader implements [objstore.Bucket].
func (b *Bucket) NewReader(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := objstore.ValidKey(key); err != nil {
		return nil, err
	}
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &b.cfg.Bucket, Key: &key})
	if err != nil {
		return nil, mapErr(key, err)
	}
	return out.Body, nil
}

// Attributes implements [objstore.Bucket].
func (b *Bucket) Attributes(ctx context.Context, key string) (*objstore.Attrs, error) {
	if err := objstore.ValidKey(key); err != nil {
		return nil, err
	}
	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &b.cfg.Bucket, Key: &key})
	if err != nil {
		return nil, mapErr(key, err)
	}
	return &objstore.Attrs{
		Size:               aws.ToInt64(out.ContentLength),
		ContentType:        aws.ToString(out.ContentType),
		CacheControl:       aws.ToString(out.CacheControl),
		ContentDisposition: aws.ToString(out.ContentDisposition),
		ETag:               strings.Trim(aws.ToString(out.ETag), `"`),
		Updated:            aws.ToTime(out.LastModified),
		Metadata:           out.Metadata,
	}, nil
}

// Delete implements [objstore.Bucket]. S3 deletes silently, so a HEAD first
// reports objstore.ErrNotFound for a missing key.
func (b *Bucket) Delete(ctx context.Context, key string) error {
	if _, err := b.Attributes(ctx, key); err != nil {
		return err
	}
	if _, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &b.cfg.Bucket, Key: &key}); err != nil {
		return mapErr(key, err)
	}
	return nil
}

// PublicURL returns CDNBaseURL joined with key (escaped). It returns an error
// when CDNBaseURL is not configured.
func (b *Bucket) PublicURL(key string) (string, error) {
	if b.cfg.CDNBaseURL == "" {
		return "", errors.New("s3: CDNBaseURL is not configured")
	}
	return objstore.PublicURL(b.cfg.CDNBaseURL, key)
}

func (b *Bucket) expires(d time.Duration) func(*s3.PresignOptions) {
	if d <= 0 {
		d = b.cfg.SignExpiry
	}
	return s3.WithPresignExpires(d)
}

// SignedURL implements [objstore.Signer] with SigV4 query signing for GET and
// PUT. Response overrides are signed. A PUT URL signs Content-Type and,
// unless AllowOverwrite, If-None-Match: * (the client must send both); it
// requires AllowUnboundedSize because S3 cannot bound a PUT's size.
func (b *Bucket) SignedURL(ctx context.Context, key string, opts objstore.SignOptions) (string, error) {
	if err := opts.Check(key); err != nil {
		return "", err
	}
	switch strings.ToUpper(opts.Method) {
	case "", http.MethodGet:
		req, err := b.presign.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket:                     &b.cfg.Bucket,
			Key:                        &key,
			ResponseContentType:        nonEmpty(opts.ResponseContentType),
			ResponseContentDisposition: nonEmpty(opts.ResponseContentDisposition),
		}, b.expires(opts.Expires))
		if err != nil {
			return "", fmt.Errorf("s3: sign %q: %w", key, err)
		}
		return req.URL, nil
	case http.MethodPut:
		if !opts.AllowUnboundedSize {
			return "", errs.Wrap(errors.New("s3: a presigned PUT cannot bound the upload size; use PostPolicy or set AllowUnboundedSize"), objstore.ErrInvalidOptions)
		}
		in := &s3.PutObjectInput{Bucket: &b.cfg.Bucket, Key: &key, ContentType: nonEmpty(opts.ContentType)}
		if !opts.AllowOverwrite {
			in.IfNoneMatch = aws.String("*")
		}
		req, err := b.presign.PresignPutObject(ctx, in, b.expires(opts.Expires))
		if err != nil {
			return "", fmt.Errorf("s3: sign %q: %w", key, err)
		}
		return req.URL, nil
	default:
		return "", errs.Wrap(fmt.Errorf("s3: cannot sign method %q", opts.Method), objstore.ErrInvalidOptions)
	}
}

// PostPolicy implements [objstore.Signer] with a SigV4 POST policy bound to
// key, whose content-length-range is [MinBytes, MaxBytes] (MaxBytes
// defaults to Config.MaxUploadBytes) and whose Content-Type is fixed or
// prefixed.
func (b *Bucket) PostPolicy(ctx context.Context, key string, opts objstore.PostPolicyOptions) (*objstore.PostPolicy, error) {
	maxBytes := opts.MaxBytes
	if maxBytes <= 0 {
		maxBytes = b.cfg.MaxUploadBytes
	}
	if err := opts.Check(key, maxBytes); err != nil {
		return nil, err
	}
	conds := []any{[]any{"content-length-range", opts.MinBytes, maxBytes}}
	switch {
	case opts.ContentType != "":
		conds = append(conds, map[string]string{"Content-Type": opts.ContentType})
	case opts.ContentTypePrefix != "":
		conds = append(conds, []any{"starts-with", "$Content-Type", opts.ContentTypePrefix})
	}
	d := opts.Expires
	if d <= 0 {
		d = b.cfg.SignExpiry
	}
	req, err := b.presign.PresignPostObject(ctx, &s3.PutObjectInput{Bucket: &b.cfg.Bucket, Key: &key},
		func(o *s3.PresignPostOptions) { o.Expires = d; o.Conditions = conds })
	if err != nil {
		return nil, fmt.Errorf("s3: post policy %q: %w", key, err)
	}
	fields := maps.Clone(req.Values)
	if opts.ContentType != "" {
		fields["Content-Type"] = opts.ContentType
	}
	return &objstore.PostPolicy{URL: req.URL, Fields: fields}, nil
}

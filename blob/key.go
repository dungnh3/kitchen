package blob

import (
	"fmt"
	"mime"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/rs/xid"
)

// NewObjectKey returns "<prefix...>/<YYYYMMDD UTC>/<xid><ext>", e.g.
// NewObjectKey(".png", "image") → "image/20251120/d4fh2s3k0lb1o0q5c7u0.png".
// ext may be given with or without the leading dot; empty ext adds none.
// xid keys sort by creation time and are safe in URLs.
func NewObjectKey(ext string, prefix ...string) string {
	return newObjectKey(time.Now(), ext, prefix...)
}

func newObjectKey(now time.Time, ext string, prefix ...string) string {
	if ext != "" && !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	parts := append(prefix[:len(prefix):len(prefix)], now.UTC().Format("20060102"), xid.NewWithTime(now).String()+ext)
	return strings.TrimPrefix(path.Join(parts...), "/")
}

// PublicURL joins a public (CDN) base URL and an object key, escaping each
// key segment: PublicURL("https://cdn.example.com/media", "a b/c.png") →
// "https://cdn.example.com/media/a%20b/c.png".
func PublicURL(base, key string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("blob: parse base URL: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("blob: base URL %q must be absolute", base)
	}
	return u.JoinPath(strings.Split(strings.TrimPrefix(key, "/"), "/")...).String(), nil
}

// common holds preferred extensions where the stdlib table is ambiguous or
// platform dependent.
var common = map[string]string{
	"image/jpeg":         ".jpg",
	"image/png":          ".png",
	"image/gif":          ".gif",
	"image/webp":         ".webp",
	"image/svg+xml":      ".svg",
	"image/heic":         ".heic",
	"image/heif":         ".heif",
	"image/avif":         ".avif",
	"text/plain":         ".txt",
	"text/html":          ".html",
	"text/css":           ".css",
	"text/csv":           ".csv",
	"text/markdown":      ".md",
	"text/javascript":    ".js",
	"application/json":   ".json",
	"application/xml":    ".xml",
	"application/pdf":    ".pdf",
	"application/zip":    ".zip",
	"application/gzip":   ".gz",
	"application/msword": ".doc",
	"application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
	"application/vnd.ms-excel": ".xls",
	"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":         ".xlsx",
	"application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
	"application/octet-stream": ".bin",
	"audio/mpeg":               ".mp3",
	"audio/ogg":                ".ogg",
	"audio/wav":                ".wav",
	"audio/mp4":                ".m4a",
	"video/mp4":                ".mp4",
	"video/webm":               ".webm",
	"video/quicktime":          ".mov",
}

// ExtensionByType returns the conventional file extension (with dot) for a
// MIME type, ignoring parameters such as "; charset=utf-8". It returns ""
// when unknown.
func ExtensionByType(mimeType string) string {
	mt, _, err := mime.ParseMediaType(mimeType)
	if err != nil {
		return ""
	}
	if ext, ok := common[mt]; ok {
		return ext
	}
	if exts, err := mime.ExtensionsByType(mt); err == nil && len(exts) > 0 {
		return exts[0]
	}
	return ""
}

// Category returns the top-level type of a MIME type ("image" for
// "image/png"), or "file" when it cannot be determined.
func Category(mimeType string) string {
	mt, _, err := mime.ParseMediaType(mimeType)
	if err != nil {
		return "file"
	}
	if c, _, ok := strings.Cut(mt, "/"); ok && c != "" {
		return c
	}
	return "file"
}

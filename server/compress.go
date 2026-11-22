package server

import (
	"mime"
	"net/http"
	"strings"

	"github.com/klauspost/compress/gzhttp"
)

// streamContentTypes are never compressed: gzip buffers output, which would
// hold back events that must reach the client as soon as they are flushed.
var streamContentTypes = map[string]bool{
	contentTypeSSE:    true,
	contentTypeNDJSON: true,
}

// compressible reports whether a response with content type ct is gzipped:
// not a stream, not a raster image or font (already compressed; SVG is
// text) and not audio, video or an archive (gzhttp.DefaultContentTypeFilter).
func compressible(ct string) bool {
	if mt, _, err := mime.ParseMediaType(ct); err == nil {
		if streamContentTypes[mt] || mt == "font/woff" || mt == "font/woff2" ||
			strings.HasPrefix(mt, "image/") && mt != "image/svg+xml" {
			return false
		}
	}
	return gzhttp.DefaultContentTypeFilter(ct)
}

// compressHandler gzips responses of at least gzhttp.DefaultMinSize (1 KiB)
// for clients that send Accept-Encoding: gzip, and sets Vary:
// Accept-Encoding. Responses that already have a Content-Encoding are left
// alone.
func compressHandler(next http.Handler) http.Handler {
	wrap, err := gzhttp.NewWrapper(gzhttp.ContentTypeFilter(compressible))
	if err != nil { // only for invalid options, which are constant here
		panic("server: gzhttp: " + err.Error())
	}
	return wrap(next)
}

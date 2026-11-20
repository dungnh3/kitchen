package webmeta

import (
	"cmp"
	"errors"
	"io"
	"net/url"
	"strings"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

// Metadata is what a page says about itself.
type Metadata struct {
	// URL is the final URL after redirects.
	URL string `json:"url"`
	// CanonicalURL is <link rel="canonical"> or og:url, if present.
	CanonicalURL string `json:"canonical_url,omitempty"`
	// Title is og:title, falling back to <title>.
	Title string `json:"title,omitempty"`
	// Description is og:description, falling back to <meta name="description">.
	Description string `json:"description,omitempty"`
	// SiteName is og:site_name.
	SiteName string `json:"site_name,omitempty"`
	// Image is og:image (or og:image:secure_url), falling back to twitter:image.
	Image string `json:"image,omitempty"`
	// Icon is apple-touch-icon, falling back to rel=icon, then /favicon.ico.
	Icon string `json:"icon,omitempty"`
}

type collected struct {
	title, ogTitle, twTitle           string
	desc, ogDesc, twDesc              string
	siteName                          string
	ogImage, ogImageSecure, twImage   string
	appleIcon, icon, canonical, ogURL string
	base                              string
}

// Parse extracts metadata from an HTML document in UTF-8. Relative URLs are
// resolved against base (or the document's <base href>). Parsing stops at
// <body>, so callers can pass a truncated document.
func Parse(base *url.URL, r io.Reader) (*Metadata, error) {
	var c collected
	z := html.NewTokenizer(r)
	inTitle := false
loop:
	for {
		tt := z.Next()
		switch tt {
		case html.ErrorToken:
			if err := z.Err(); !errors.Is(err, io.EOF) {
				return nil, err
			}
			break loop
		case html.TextToken:
			if inTitle && c.title == "" {
				c.title = clean(string(z.Text()))
			}
		case html.EndTagToken:
			name, _ := z.TagName()
			switch atom.Lookup(name) {
			case atom.Title:
				inTitle = false
			case atom.Head:
				break loop
			}
		case html.StartTagToken, html.SelfClosingTagToken:
			name, hasAttr := z.TagName()
			a := atom.Lookup(name)
			if a == atom.Body {
				break loop
			}
			if a == atom.Title {
				inTitle = tt == html.StartTagToken
				continue
			}
			if !hasAttr {
				continue
			}
			attrs := map[string]string{}
			for {
				k, v, more := z.TagAttr()
				attrs[strings.ToLower(string(k))] = string(v)
				if !more {
					break
				}
			}
			switch a {
			case atom.Meta:
				c.meta(attrs)
			case atom.Link:
				c.link(attrs)
			case atom.Base:
				if c.base == "" {
					c.base = attrs["href"]
				}
			}
		}
	}

	if base == nil {
		base = &url.URL{}
	}
	if c.base != "" {
		if b, err := base.Parse(c.base); err == nil {
			base = b
		}
	}
	resolve := func(s string) string {
		if s == "" {
			return ""
		}
		u, err := base.Parse(strings.TrimSpace(s))
		if err != nil || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "") {
			return ""
		}
		return u.String()
	}
	m := &Metadata{
		URL:          base.String(),
		Title:        cmp.Or(c.ogTitle, c.title, c.twTitle),
		Description:  cmp.Or(c.ogDesc, c.desc, c.twDesc),
		SiteName:     c.siteName,
		Image:        resolve(cmp.Or(c.ogImageSecure, c.ogImage, c.twImage)),
		Icon:         resolve(cmp.Or(c.appleIcon, c.icon)),
		CanonicalURL: resolve(cmp.Or(c.canonical, c.ogURL)),
	}
	if m.Icon == "" && base.Host != "" {
		m.Icon = resolve("/favicon.ico")
	}
	return m, nil
}

func (c *collected) meta(a map[string]string) {
	key := strings.ToLower(cmp.Or(a["property"], a["name"]))
	v := clean(a["content"])
	if v == "" {
		return
	}
	set := func(dst *string) {
		if *dst == "" {
			*dst = v
		}
	}
	switch key {
	case "og:title":
		set(&c.ogTitle)
	case "twitter:title":
		set(&c.twTitle)
	case "description":
		set(&c.desc)
	case "og:description":
		set(&c.ogDesc)
	case "twitter:description":
		set(&c.twDesc)
	case "og:site_name":
		set(&c.siteName)
	case "og:image", "og:image:url":
		set(&c.ogImage)
	case "og:image:secure_url":
		set(&c.ogImageSecure)
	case "twitter:image", "twitter:image:src":
		set(&c.twImage)
	case "og:url":
		set(&c.ogURL)
	}
}

func (c *collected) link(a map[string]string) {
	href := strings.TrimSpace(a["href"])
	if href == "" {
		return
	}
	for _, rel := range strings.Fields(strings.ToLower(a["rel"])) {
		switch rel {
		case "apple-touch-icon", "apple-touch-icon-precomposed":
			if c.appleIcon == "" {
				c.appleIcon = href
			}
		case "icon":
			if c.icon == "" {
				c.icon = href
			}
		case "canonical":
			if c.canonical == "" {
				c.canonical = href
			}
		}
	}
}

func clean(s string) string { return strings.Join(strings.Fields(s), " ") }

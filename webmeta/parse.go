package webmeta

import (
	"cmp"
	"errors"
	"io"
	"net/url"
	"strings"
	"unicode/utf8"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

// Metadata is what a page says about itself.
type Metadata struct {
	// URL is the final URL after redirects, or the document's <base href>
	// when it has one.
	URL string `json:"url"`
	// CanonicalURL is <link rel="canonical"> or og:url, if present.
	CanonicalURL string `json:"canonical_url,omitempty"`
	// Title is og:title, falling back to <title>, then twitter:title.
	Title string `json:"title,omitempty"`
	// Description is og:description, falling back to <meta name="description">,
	// twitter:description, then the first body paragraph of at least 30
	// characters (cut to 300).
	Description string `json:"description,omitempty"`
	// SiteName is og:site_name.
	SiteName string `json:"site_name,omitempty"`
	// Image is og:image:secure_url or og:image, falling back to twitter:image.
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
	para                              string // first long-enough <p>
}

const (
	minParagraph = 30  // runes; shorter paragraphs are menus and captions
	maxParagraph = 300 // runes kept from the fallback paragraph
)

func (c *collected) hasDescription() bool { return c.desc != "" || c.ogDesc != "" || c.twDesc != "" }

// Parse extracts metadata from an HTML document in UTF-8. Relative URLs are
// resolved against base (or the document's <base href>). Parsing stops at
// <body>, unless the head has no description: then it reads on to the first
// paragraph. Callers can pass a truncated document.
func Parse(base *url.URL, r io.Reader) (*Metadata, error) {
	var c collected
	z := html.NewTokenizer(r)
	inTitle, inBody := false, false
	var (
		inP     bool
		rawText int // depth inside script/style/template/noscript
		para    strings.Builder
	)
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
			switch {
			case inTitle && c.title == "":
				c.title = clean(string(z.Text()))
			case inP && rawText == 0 && para.Len() < 8*maxParagraph:
				para.Write(z.Text())
				para.WriteByte(' ')
			}
		case html.EndTagToken:
			name, _ := z.TagName()
			switch atom.Lookup(name) {
			case atom.Title:
				inTitle = false
			case atom.Head:
				if c.hasDescription() {
					break loop
				}
				inBody = true
			case atom.Script, atom.Style, atom.Template, atom.Noscript:
				rawText = max(rawText-1, 0)
			case atom.P:
				if inP {
					inP = false
					if t := clean(para.String()); utf8.RuneCountInString(t) >= minParagraph {
						c.para = truncate(t, maxParagraph)
						break loop
					}
					para.Reset()
				}
			}
		case html.StartTagToken, html.SelfClosingTagToken:
			name, hasAttr := z.TagName()
			a := atom.Lookup(name)
			switch a {
			case atom.Body:
				if c.hasDescription() {
					break loop
				}
				inBody = true
			case atom.Script, atom.Style, atom.Template, atom.Noscript:
				if tt == html.StartTagToken {
					rawText++
				}
			case atom.P:
				if inBody {
					inP = true
					para.Reset()
				}
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
		Description:  cmp.Or(c.ogDesc, c.desc, c.twDesc, c.para),
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

// truncate cuts s to at most n runes, at a word boundary when possible.
func truncate(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)[:n]
	if i := strings.LastIndexByte(string(r), ' '); i > n/2 {
		return string(r)[:i] + "…"
	}
	return string(r) + "…"
}

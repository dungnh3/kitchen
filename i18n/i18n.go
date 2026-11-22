// Package i18n localizes messages (typically error reasons) using
// go-i18n catalogs and the standard Accept-Language negotiation.
//
// A Bundle is built once at startup and is immutable and safe for
// concurrent use afterwards. Message IDs are google.rpc.ErrorInfo reasons,
// so a Bundle plugs straight into errs.Config.Localizer:
//
//	b, err := i18n.New(language.English, i18n.WithDefaults(), i18n.WithFS(langs, "langs/*.json"))
//	eh := errs.NewHandler(errs.Config{Localizer: b, Domain: "agents.example.com"})
//
// WithDefaults loads English and Vietnamese text for every reason in
// errs.Catalog (plus the auth and ratelimit reasons). Messages are go-i18n
// templates; ErrorInfo metadata is the template data, so a message can say
// "Try again in {{.retry_after}} seconds." when the error sets that key.
//
// The request language is, in order: a value stored with WithAcceptLanguage,
// the Accept-Language of the request Info (package kitchen/context, filled
// by the server from the HTTP header or the "accept-language" /
// "grpcgateway-accept-language" metadata), the raw incoming metadata, and
// finally the X-Locale header captured in the Info.
package i18n

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"strings"

	xcontext "github.com/dungnh3/kitchen/context"
	goi18n "github.com/nicksnyder/go-i18n/v2/i18n"
	"golang.org/x/text/language"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/metadata"
)

//go:embed langs/*.json
var defaults embed.FS

// Bundle holds message catalogs for several languages.
type Bundle struct {
	matcher language.Matcher
	tags    []language.Tag
	// locs[i] localizes into tags[i] (falling back to the default). They
	// are built once, so a request never allocates a localizer.
	locs []*goi18n.Localizer
}

// maxAcceptLanguage bounds the header length we parse; longer values are
// ignored rather than parsed (cheap protection against oversized headers).
const maxAcceptLanguage = 256

type config struct {
	sources []func(*goi18n.Bundle) error
}

// Option configures New.
type Option func(*config)

// WithDefaults loads kitchen's built-in English and Vietnamese messages for
// the generic errs reasons (NOT_FOUND, INTERNAL, ...). Later options can
// override any of them.
func WithDefaults() Option {
	return WithFS(defaults, "langs/*.json")
}

// WithFS loads every catalog in fsys matching the fs.Glob patterns. The file
// name gives the language and format, as in go-i18n: "vi.json",
// "active.en-US.json". JSON is supported out of the box.
func WithFS(fsys fs.FS, patterns ...string) Option {
	return func(c *config) {
		c.sources = append(c.sources, func(b *goi18n.Bundle) error {
			for _, p := range patterns {
				files, err := fs.Glob(fsys, p)
				if err != nil {
					return fmt.Errorf("i18n: pattern %q: %w", p, err)
				}
				if len(files) == 0 {
					return fmt.Errorf("i18n: pattern %q matched no files", p)
				}
				for _, f := range files {
					if _, err := b.LoadMessageFileFS(fsys, f); err != nil {
						return fmt.Errorf("i18n: load %s: %w", path.Clean(f), err)
					}
				}
			}
			return nil
		})
	}
}

// WithMessages adds messages (ID → text) for one language.
func WithMessages(tag language.Tag, msgs map[string]string) Option {
	return func(c *config) {
		c.sources = append(c.sources, func(b *goi18n.Bundle) error {
			for id, text := range msgs {
				if err := b.AddMessages(tag, &goi18n.Message{ID: id, Other: text}); err != nil {
					return fmt.Errorf("i18n: add %s/%s: %w", tag, id, err)
				}
			}
			return nil
		})
	}
}

// New builds a Bundle whose fallback language is def. Options are applied in
// order; later messages override earlier ones with the same ID and language.
func New(def language.Tag, opts ...Option) (*Bundle, error) {
	var c config
	for _, o := range opts {
		o(&c)
	}
	b := goi18n.NewBundle(def)
	b.RegisterUnmarshalFunc("json", json.Unmarshal)
	for _, load := range c.sources {
		if err := load(b); err != nil {
			return nil, err
		}
	}
	tags := b.LanguageTags()
	locs := make([]*goi18n.Localizer, len(tags))
	for i, t := range tags {
		locs[i] = goi18n.NewLocalizer(b, t.String())
	}
	return &Bundle{matcher: language.NewMatcher(tags), tags: tags, locs: locs}, nil
}

// Languages returns the languages that have messages, default first.
func (b *Bundle) Languages() []language.Tag { return append([]language.Tag(nil), b.tags...) }

// Match returns the supported language that best matches the given
// Accept-Language values (or plain tags), or the default language.
func (b *Bundle) Match(acceptLanguage ...string) language.Tag {
	return b.tags[b.match(acceptLanguage)]
}

func (b *Bundle) match(acceptLanguage []string) int {
	tags := parse(acceptLanguage)
	if len(tags) == 0 {
		return 0 // default language
	}
	_, i, _ := b.matcher.Match(tags...)
	return i
}

// Message returns the text for id in the best language for acceptLanguage,
// falling back to the default language, with data as template data (nil is
// fine). ok is false when id is unknown in both; then msg is empty.
func (b *Bundle) Message(id string, data map[string]string, acceptLanguage ...string) (msg string, tag language.Tag, ok bool) {
	l := b.locs[b.match(acceptLanguage)]
	cfg := &goi18n.LocalizeConfig{MessageID: id}
	if len(data) > 0 {
		cfg.TemplateData = maps.Clone(data)
	}
	// go-i18n reports a fallback to the default language as an error while
	// still returning the message; only an empty message means "not found".
	msg, tag, _ = l.LocalizeWithTag(cfg)
	// A placeholder without data renders as "<no value>"; report "no
	// message" rather than show it to a user.
	if msg == "" || strings.Contains(msg, "<no value>") {
		return "", language.Und, false
	}
	return msg, tag, true
}

// Localize implements errs.Localizer: it localizes info.Reason for the
// language of the request in ctx (see AcceptLanguage), with info.Metadata
// as template data.
func (b *Bundle) Localize(ctx context.Context, info *errdetails.ErrorInfo) (locale, message string, ok bool) {
	msg, tag, ok := b.Message(info.GetReason(), info.GetMetadata(), AcceptLanguage(ctx)...)
	if !ok {
		return "", "", false
	}
	return tag.String(), msg, true
}

func parse(values []string) []language.Tag {
	var tags []language.Tag
	for _, v := range values {
		if len(v) > maxAcceptLanguage {
			continue
		}
		ts, _, err := language.ParseAcceptLanguage(v)
		if err != nil {
			continue
		}
		tags = append(tags, ts...)
	}
	return tags
}

type acceptLanguageKey struct{}

// WithAcceptLanguage stores an Accept-Language value in ctx, for the HTTP
// path where there is no gRPC metadata.
func WithAcceptLanguage(ctx context.Context, v string) context.Context {
	return context.WithValue(ctx, acceptLanguageKey{}, v)
}

// AcceptLanguage returns the language preferences of the request in ctx, in
// the order described in the package documentation. The first non-empty
// source wins.
func AcceptLanguage(ctx context.Context) []string {
	if v, ok := ctx.Value(acceptLanguageKey{}).(string); ok && v != "" {
		return []string{v}
	}
	info, hasInfo := xcontext.FromContext(ctx)
	if hasInfo && info.AcceptLanguage != "" {
		return []string{info.AcceptLanguage}
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for _, k := range []string{"accept-language", "grpcgateway-accept-language"} {
			if v := md.Get(k); len(v) > 0 && v[0] != "" {
				return v[:1]
			}
		}
	}
	if hasInfo {
		if v := info.Header(xcontext.HeaderLocale); v != "" {
			return []string{v}
		}
	}
	return nil
}

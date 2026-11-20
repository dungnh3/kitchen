// Package i18n localizes messages (typically error reasons) using
// go-i18n catalogs and the standard Accept-Language negotiation.
//
// A Bundle is built once at startup and is immutable and safe for
// concurrent use afterwards. Message IDs are google.rpc.ErrorInfo reasons,
// so a Bundle plugs straight into errs.ServerConfig.Localizer:
//
//	b, err := i18n.New(language.English, i18n.WithDefaults(), i18n.WithFS(langs, "langs/*.json"))
//	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(
//		errs.UnaryServerInterceptor(errs.ServerConfig{Localizer: b})))
//
// The request language comes from the Accept-Language HTTP header, or the
// gRPC metadata keys "accept-language" and "grpcgateway-accept-language".
package i18n

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"path"

	goi18n "github.com/nicksnyder/go-i18n/v2/i18n"
	"golang.org/x/text/language"
	"google.golang.org/grpc/metadata"
)

//go:embed langs/*.json
var defaults embed.FS

// Bundle holds message catalogs for several languages.
type Bundle struct {
	b       *goi18n.Bundle
	matcher language.Matcher
	tags    []language.Tag
}

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
	return &Bundle{b: b, matcher: language.NewMatcher(tags), tags: tags}, nil
}

// Languages returns the languages that have messages, default first.
func (b *Bundle) Languages() []language.Tag { return append([]language.Tag(nil), b.tags...) }

// Match returns the supported language that best matches the given
// Accept-Language values (or plain tags), or the default language.
func (b *Bundle) Match(acceptLanguage ...string) language.Tag {
	_, i, _ := b.matcher.Match(parse(acceptLanguage)...)
	return b.tags[i]
}

// Message returns the text for id in the best language for acceptLanguage,
// falling back to the default language. ok is false when id is unknown in
// both; then msg is empty.
func (b *Bundle) Message(id string, acceptLanguage ...string) (msg string, tag language.Tag, ok bool) {
	l := goi18n.NewLocalizer(b.b, acceptLanguage...)
	// go-i18n reports a fallback to the default language as an error while
	// still returning the message; only an empty message means "not found".
	msg, tag, _ = l.LocalizeWithTag(&goi18n.LocalizeConfig{MessageID: id})
	if msg == "" {
		return "", language.Und, false
	}
	return msg, tag, true
}

// Localize implements errs.Localizer: it localizes reason for the language
// of the request in ctx (see AcceptLanguage).
func (b *Bundle) Localize(ctx context.Context, reason string) (locale, message string, ok bool) {
	msg, tag, ok := b.Message(reason, AcceptLanguage(ctx)...)
	if !ok {
		return "", "", false
	}
	return tag.String(), msg, true
}

func parse(values []string) []language.Tag {
	var tags []language.Tag
	for _, v := range values {
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

// AcceptLanguage returns the request's Accept-Language values: the value
// stored by WithAcceptLanguage, else the incoming gRPC metadata
// "accept-language" and "grpcgateway-accept-language".
func AcceptLanguage(ctx context.Context) []string {
	if v, ok := ctx.Value(acceptLanguageKey{}).(string); ok && v != "" {
		return []string{v}
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	if v := md.Get("accept-language"); len(v) > 0 {
		return v
	}
	return md.Get("grpcgateway-accept-language")
}

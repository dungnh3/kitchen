package i18n_test

import (
	"context"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/dungnh3/kitchen/i18n"
	"golang.org/x/text/language"
	"google.golang.org/grpc/metadata"
)

func newBundle(t *testing.T) *i18n.Bundle {
	t.Helper()
	app := fstest.MapFS{
		"langs/en.json": {Data: []byte(`{"AGENT_NOT_FOUND": "Agent not found.", "NOT_FOUND": "Nothing here."}`)},
		"langs/vi.json": {Data: []byte(`{"AGENT_NOT_FOUND": "Không tìm thấy agent."}`)},
	}
	b, err := i18n.New(language.English, i18n.WithDefaults(), i18n.WithFS(app, "langs/*.json"),
		i18n.WithMessages(language.Vietnamese, map[string]string{"QUOTA": "Hết hạn mức."}))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestMessage(t *testing.T) {
	b := newBundle(t)
	tests := []struct {
		id, accept, want, tag string
		ok                    bool
	}{
		{"AGENT_NOT_FOUND", "vi-VN,vi;q=0.9,en;q=0.5", "Không tìm thấy agent.", "vi", true},
		{"AGENT_NOT_FOUND", "fr-FR", "Agent not found.", "en", true},
		{"NOT_FOUND", "en", "Nothing here.", "en", true},                           // app overrides default
		{"NOT_FOUND", "vi", "Không tìm thấy tài nguyên được yêu cầu.", "vi", true}, // built-in vi
		{"QUOTA", "en", "", "und", false},                                          // vi only, no en fallback
		{"QUOTA", "vi", "Hết hạn mức.", "vi", true},
		{"MISSING", "vi", "", "und", false},
		{"INTERNAL", "", "Something went wrong on our side. Please try again later.", "en", true},
		{"INTERNAL", "!!garbage", "Something went wrong on our side. Please try again later.", "en", true},
	}
	for _, tt := range tests {
		msg, tag, ok := b.Message(tt.id, tt.accept)
		if msg != tt.want || ok != tt.ok || tag.String() != tt.tag {
			t.Errorf("Message(%q, %q) = %q %s %v; want %q %s %v", tt.id, tt.accept, msg, tag, ok, tt.want, tt.tag, tt.ok)
		}
	}
	if got := b.Match("vi-VN"); got != language.Vietnamese {
		t.Errorf("Match = %v", got)
	}
}

func TestLocalizeFromContext(t *testing.T) {
	b := newBundle(t)
	ctxs := map[string]context.Context{
		"grpc":    metadata.NewIncomingContext(context.Background(), metadata.Pairs("accept-language", "vi")),
		"gateway": metadata.NewIncomingContext(context.Background(), metadata.Pairs("grpcgateway-accept-language", "vi-VN")),
		"http":    i18n.WithAcceptLanguage(context.Background(), "vi;q=1"),
	}
	for name, ctx := range ctxs {
		loc, msg, ok := b.Localize(ctx, "AGENT_NOT_FOUND")
		if !ok || loc != "vi" || msg != "Không tìm thấy agent." {
			t.Errorf("%s: %q %q %v", name, loc, msg, ok)
		}
	}
	if loc, _, ok := b.Localize(context.Background(), "AGENT_NOT_FOUND"); !ok || loc != "en" {
		t.Errorf("no language: %q %v", loc, ok)
	}
}

func TestNewErrors(t *testing.T) {
	if _, err := i18n.New(language.English, i18n.WithFS(fstest.MapFS{}, "langs/*.json")); err == nil {
		t.Error("empty glob should fail")
	}
	bad := fstest.MapFS{"langs/en.json": {Data: []byte(`{not json`)}}
	if _, err := i18n.New(language.English, i18n.WithFS(bad, "langs/*.json")); err == nil {
		t.Error("bad json should fail")
	}
}

func TestConcurrentUse(t *testing.T) {
	b := newBundle(t)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for range 200 {
				_, _, _ = b.Message("NOT_FOUND", "vi")
			}
		})
	}
	wg.Wait()
}

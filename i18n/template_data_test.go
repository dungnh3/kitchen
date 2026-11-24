package i18n_test

import (
	"testing"

	"github.com/dungnh3/kitchen/i18n"
	"golang.org/x/text/language"
)

// A placeholder without data must not render "<no value>": the message is
// reported as missing instead.
func TestMissingTemplateData(t *testing.T) {
	b, err := i18n.New(language.English, i18n.WithMessages(language.English, map[string]string{"LIMIT": "Limit of {{.limit}} reached."}))
	if err != nil {
		t.Fatal(err)
	}
	if msg, _, ok := b.Message("LIMIT", map[string]string{"limit": "10"}); !ok || msg != "Limit of 10 reached." {
		t.Fatalf("with data: %q %v", msg, ok)
	}
	if msg, _, ok := b.Message("LIMIT", nil); ok || msg != "" {
		t.Fatalf("without data: %q %v", msg, ok)
	}
}

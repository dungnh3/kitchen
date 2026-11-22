package i18n_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing/fstest"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/i18n"
	"github.com/dungnh3/kitchen/log"
	"golang.org/x/text/language"
	"google.golang.org/grpc/codes"
)

// The examples in this file are the snippets of i18n/USAGE.md.

func ExampleWithFS() {
	// In a service: //go:embed langs/*.json, then pass that embed.FS.
	langs := fstest.MapFS{
		"langs/en.json": {Data: []byte(`{"AGENT_NOT_FOUND": "Agent not found."}`)},
		"langs/vi.json": {Data: []byte(`{"AGENT_NOT_FOUND": "Không tìm thấy agent."}`)},
	}
	b, err := i18n.New(language.English, i18n.WithDefaults(), i18n.WithFS(langs, "langs/*.json"))
	if err != nil {
		panic(err)
	}
	fmt.Println(b.Languages())
	msg, tag, _ := b.Message("AGENT_NOT_FOUND", nil, "vi")
	fmt.Println(tag, msg)
	// Output:
	// [en vi]
	// vi Không tìm thấy agent.
}

func ExampleBundle_Message() {
	b, err := i18n.New(language.English, i18n.WithMessages(language.English, map[string]string{
		"QUOTA_EXCEEDED": "You used all {{.limit}} requests of this month.",
	}))
	if err != nil {
		panic(err)
	}
	msg, _, ok := b.Message("QUOTA_EXCEEDED", map[string]string{"limit": "1000"})
	fmt.Println(msg, ok)
	// A placeholder without data is reported as missing, never "<no value>".
	msg, _, ok = b.Message("QUOTA_EXCEEDED", nil)
	fmt.Printf("%q %v\n", msg, ok)
	// Output:
	// You used all 1000 requests of this month. true
	// "" false
}

func ExampleBundle_Match() {
	b, err := i18n.New(language.English, i18n.WithDefaults())
	if err != nil {
		panic(err)
	}
	fmt.Println(b.Match("vi-VN,vi;q=0.9,en;q=0.5"))
	fmt.Println(b.Match("fr-FR")) // unsupported: the default language
	// Output:
	// vi
	// en
}

var ErrAgentNotFound = errs.New(codes.NotFound, "AGENT_NOT_FOUND",
	errs.Domain("agents.example.com"), errs.Message("agent not found"))

func Example_errsHandler() {
	b, err := i18n.New(language.English, i18n.WithDefaults(),
		i18n.WithMessages(language.Vietnamese, map[string]string{"AGENT_NOT_FOUND": "Không tìm thấy agent {{.agent_id}}."}))
	if err != nil {
		panic(err)
	}
	eh := errs.NewHandler(errs.Config{Localizer: b, Domain: "agents.example.com", Logger: log.Discard()})

	// kitchen/server records Accept-Language for you; a bare handler sets it.
	r := httptest.NewRequest(http.MethodGet, "/v1/agents/a1", nil)
	r = r.WithContext(i18n.WithAcceptLanguage(context.Background(), "vi"))
	rec := httptest.NewRecorder()
	eh.WriteHTTP(rec, r, ErrAgentNotFound.With(errs.Meta("agent_id", "a1")))
	fmt.Println(rec.Body.String())
	// Output:
	// {"error":{"code":404,"message":"agent not found","status":"NOT_FOUND","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"AGENT_NOT_FOUND","domain":"agents.example.com","metadata":{"agent_id":"a1"}},{"@type":"type.googleapis.com/google.rpc.LocalizedMessage","locale":"vi","message":"Không tìm thấy agent a1."}]}}
}

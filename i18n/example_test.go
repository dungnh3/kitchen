package i18n_test

import (
	"context"
	"fmt"

	"github.com/dungnh3/kitchen/i18n"
	"golang.org/x/text/language"
)

func Example() {
	b, err := i18n.New(language.English, i18n.WithDefaults(),
		i18n.WithMessages(language.Vietnamese, map[string]string{"AGENT_NOT_FOUND": "Không tìm thấy agent."}))
	if err != nil {
		panic(err)
	}
	ctx := i18n.WithAcceptLanguage(context.Background(), "vi-VN,vi;q=0.9,en;q=0.5")
	fmt.Println(b.Localize(ctx, "AGENT_NOT_FOUND"))
	fmt.Println(b.Message("NOT_FOUND", "en-US"))
	// Output:
	// vi Không tìm thấy agent. true
	// The requested resource was not found. en true
}

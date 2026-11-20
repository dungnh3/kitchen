package gcs_test

import (
	"context"
	"fmt"
	"time"

	"github.com/dungnh3/kitchen/blob"
	"github.com/dungnh3/kitchen/blob/gcs"
)

func ExampleOpen() {
	ctx := context.Background()
	b, err := gcs.Open(ctx, gcs.Config{Bucket: "media", CDNBaseURL: "https://cdn.example.com"})
	if err != nil {
		return
	}
	defer func() { _ = b.Close() }()

	key := blob.NewObjectKey(blob.ExtensionByType("image/png"), "image")
	pp, err := b.PostPolicy(ctx, key, blob.PostPolicyOptions{MaxBytes: 20 << 20, Expires: 5 * time.Minute})
	if err != nil {
		return
	}
	cdn, _ := b.PublicURL(key)
	fmt.Println(pp.URL, cdn)
}

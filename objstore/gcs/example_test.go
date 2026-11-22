package gcs_test

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/dungnh3/kitchen/objstore"
	"github.com/dungnh3/kitchen/objstore/gcs"
)

func ExampleOpen() {
	ctx := context.Background()
	b, err := gcs.Open(ctx, gcs.Config{Bucket: "media", CDNBaseURL: "https://cdn.example.com"})
	if err != nil {
		return
	}
	defer func() { _ = b.Close() }()

	key := objstore.NewObjectKey(objstore.ExtensionByType("image/png"), "image")
	pp, err := b.PostPolicy(ctx, key, objstore.PostPolicyOptions{MaxBytes: 20 << 20, ContentTypePrefix: "image/", Expires: 5 * time.Minute})
	if err != nil {
		return
	}
	cdn, _ := b.PublicURL(key)
	fmt.Println(pp.URL, cdn)
}

func ExampleBucket_SignedURL() {
	ctx := context.Background()
	b, err := gcs.Open(ctx, gcs.Config{Bucket: "media"})
	if err != nil {
		return
	}
	defer func() { _ = b.Close() }()

	// A create-only PUT URL for one PNG of at most 5 MiB. The client must
	// send the Content-Type and the x-goog-* headers it was signed with.
	put, err := b.SignedURL(ctx, "avatars/u42.png", objstore.SignOptions{
		Method:      http.MethodPut,
		ContentType: "image/png",
		MaxBytes:    5 << 20,
	})
	if err != nil {
		return
	}
	// A download URL that forces a file name.
	get, _ := b.SignedURL(ctx, "reports/q1.pdf", objstore.SignOptions{
		ResponseContentDisposition: `attachment; filename="q1.pdf"`,
	})
	fmt.Println(put, get)
}

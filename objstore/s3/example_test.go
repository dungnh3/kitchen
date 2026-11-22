package s3_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dungnh3/kitchen/objstore"
	"github.com/dungnh3/kitchen/objstore/s3"
)

func ExampleOpen() {
	ctx := context.Background()
	// Credentials and region come from the default AWS chain (env, shared config, IAM role).
	b, err := s3.Open(ctx, s3.Config{Bucket: "media", CDNBaseURL: "https://cdn.example.com"})
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

func ExampleNew() {
	// An S3-compatible store (MinIO, Cloudflare R2) with an explicit client.
	client := awss3.New(awss3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String("https://minio.internal:9000"),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("AKID", "SECRET", ""),
	})
	b, err := s3.New(client, s3.Config{Bucket: "media"})
	if err != nil {
		panic(err)
	}

	// S3 cannot bound the size of a presigned PUT, so it is refused by default.
	_, err = b.SignedURL(context.Background(), "uploads/a.png", objstore.SignOptions{
		Method: http.MethodPut, ContentType: "image/png",
	})
	fmt.Println(errors.Is(err, objstore.ErrInvalidOptions))
	// Output: true
}

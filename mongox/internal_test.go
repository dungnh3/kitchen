package mongox

import (
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestCollectionOf(t *testing.T) {
	raw := func(d bson.D) bson.Raw {
		b, err := bson.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	for _, tt := range []struct {
		cmd  string
		body bson.Raw
		want string
	}{
		{"find", raw(bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{}}}), "users"},
		{"insert", raw(bson.D{{Key: "insert", Value: "orders"}}), "orders"},
		{"getMore", raw(bson.D{{Key: "getMore", Value: int64(42)}, {Key: "collection", Value: "users"}}), "users"},
		{"ping", raw(bson.D{{Key: "ping", Value: 1}}), ""},
		{"find", nil, ""},
	} {
		if got := collectionOf(tt.cmd, tt.body); got != tt.want {
			t.Errorf("%s: got %q, want %q", tt.cmd, got, tt.want)
		}
	}
}

func TestServerOf(t *testing.T) {
	if h, p := serverOf("db.example.com:27017[-12]"); h != "db.example.com" || p != 27017 {
		t.Fatalf("got %q %d", h, p)
	}
	if h, _ := serverOf("garbage"); h != "" {
		t.Fatalf("got %q", h)
	}
}

func TestClassify(t *testing.T) {
	if typ, code := classify(mongo.CommandError{Code: 11000, Name: "DuplicateKey"}); typ != "DuplicateKey" || code != "11000" {
		t.Fatalf("got %q %q", typ, code)
	}
	if typ, _ := classify(errors.New("x")); typ != "_OTHER" {
		t.Fatalf("got %q", typ)
	}
}

func TestTLSPolicy(t *testing.T) {
	for _, tt := range []struct {
		uri     string
		wantTLS bool
	}{
		{"mongodb://db.internal:27017", true},             // remote, unspecified → TLS
		{"mongodb://db.internal:27017/?tls=false", false}, // explicit opt-out
		{"mongodb://localhost:27017", false},
		{"mongodb://127.0.0.1:27017,[::1]:27018", false},
		{"mongodb://localhost:27017,db.internal:27017", true},
		{"mongodb://db.internal/?ssl=true", true},
	} {
		co, err := ClientOptions(Config{URI: tt.uri})
		if err != nil {
			t.Fatalf("%s: %v", tt.uri, err)
		}
		if got := co.TLSConfig != nil; got != tt.wantTLS {
			t.Errorf("%s: TLS = %v, want %v", tt.uri, got, tt.wantTLS)
		}
	}
}

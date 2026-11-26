package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/zen/internal/scaffold"
)

// serviceAdds are the add commands that the add golden and build tests run
// on a golden service case.
var serviceAdds = map[string][][]string{
	"service-events": {{"domain", "order-item"}, {"consumer", "payments"}, {"job", "cleanup", "-every", "5m"}, {"client", "ledger"}},
	"service-kafka":  {{"domain", "category"}, {"consumer", "order-events"}},
	"service-sqs":    {{"domain", "box"}, {"consumer", "order-events"}},
	"service-mysql":  {{"domain", "order-item"}, {"client", "billing"}, {"client", "user-profile"}},
}

// runAdds writes the case into dir and runs its add commands without code
// generation.
func runAdds(t *testing.T, dir, name string) {
	t.Helper()
	if err := scaffold.Write(dir, renderSvc(t, serviceGoldenCases[name])); err != nil {
		t.Fatal(err)
	}
	for _, args := range serviceAdds[name] {
		args = append(append([]string{"add"}, args...), "-dir", dir)
		if args[1] == "domain" {
			args = append(args, "-no-generate")
		}
		var out bytes.Buffer
		if code := run(t.Context(), args, nil, &out, &out); code != 0 {
			t.Fatalf("zen %s: exit %d\n%s", strings.Join(args, " "), code, out.String())
		}
	}
}

// TestAddGolden compares the files that the add commands write or change
// with testdata/golden/add-<case>.
func TestAddGolden(t *testing.T) {
	for name := range serviceAdds {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "proj")
			runAdds(t, dir, name)
			before := renderSvc(t, serviceGoldenCases[name])
			changed := scaffold.Files{}
			err := filepath.WalkDir(dir, func(fp string, d os.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return err
				}
				rel, _ := filepath.Rel(dir, fp)
				rel = filepath.ToSlash(rel)
				b, err := os.ReadFile(fp) //nolint:gosec // file in the test project
				if err != nil {
					return err
				}
				if old, ok := before[rel]; !ok || !bytes.Equal(old, b) {
					changed[rel] = b
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			checkGolden(t, filepath.Join("testdata", "golden", "add-"+strings.TrimPrefix(name, "service-")), changed)
		})
	}
}

func TestAddRefusals(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "proj")
	if err := scaffold.Write(dir, renderSvc(t, serviceGoldenCases["service-postgres"])); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		args []string
		code int
		want string
	}{
		{"existing domain", []string{"domain", "item"}, 1, "already exists"},
		{"bad name", []string{"domain", "Order"}, 1, "lower-case"},
		{"same plural", []string{"domain", "sheep", "-plural", "sheep"}, 1, "must differ"},
		{"consumer without pubsub", []string{"consumer", "orders"}, 1, "-with pubsub"},
		{"job without lease", []string{"job", "cleanup"}, 1, "-with lease"},
		{"client with a reserved name", []string{"client", "redis"}, 1, "name is taken"},
		{"client named like a Go keyword", []string{"client", "type"}, 1, "name is taken"},
		{"bad client name", []string{"client", "Billing"}, 1, "lower-case"},
		{"client without name", []string{"client"}, 2, "missing <name>"},
		{"missing name", []string{"domain"}, 2, "missing <name>"},
		{"unknown kind", []string{"model", "x"}, 2, "zen add domain"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			args := append(append([]string{"add"}, tt.args...), "-dir", dir)
			if code := run(t.Context(), args, nil, &out, &out); code != tt.code || !strings.Contains(out.String(), tt.want) {
				t.Errorf("exit %d, want %d; output %q lacks %q", code, tt.code, out.String(), tt.want)
			}
		})
	}
}

// TestAddMissingMarkerWritesNothing: when one marker is missing, no file is
// created or changed.
func TestAddMissingMarkerWritesNothing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "proj")
	files := renderSvc(t, serviceGoldenCases["service-events"])
	files["Makefile"] = bytes.ReplaceAll(files["Makefile"], []byte("# zen:local-env"), nil)
	if err := scaffold.Write(dir, files); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if code := run(t.Context(), []string{"add", "consumer", "payments", "-dir", dir}, nil, &out, &out); code != 1 {
		t.Fatalf("exit %d, want 1\n%s", code, out.String())
	}
	if !strings.Contains(out.String(), "add the change by hand") {
		t.Errorf("output %q does not explain the missing marker", out.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "internal", "consumer", "payments.go")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("consumer file written: %v", err)
	}
	cfg, err := os.ReadFile(filepath.Join(dir, "internal", "config", "config.go")) //nolint:gosec // file in the test project
	if err != nil || !bytes.Equal(cfg, files["internal/config/config.go"]) {
		t.Errorf("config.go changed (err %v)", err)
	}
}

func TestNewEntity(t *testing.T) {
	tests := []struct {
		name, plural string
		want         entity
	}{
		{"item", "", entity{Kebab: "item", Name: "Item", Names: "Items", Lower: "item", Camels: "items", Snake: "item", Snakes: "items", Words: "item", WordsPlural: "items", Article: "an"}},
		{"order-item", "", entity{Kebab: "order-item", Name: "OrderItem", Names: "OrderItems", Lower: "orderItem", Camels: "orderItems", Snake: "order_item", Snakes: "order_items", Words: "order item", WordsPlural: "order items", Article: "an"}},
		{"category", "", entity{Kebab: "category", Name: "Category", Names: "Categories", Lower: "category", Camels: "categories", Snake: "category", Snakes: "categories", Words: "category", WordsPlural: "categories", Article: "a"}},
		{"person", "people", entity{Kebab: "person", Name: "Person", Names: "People", Lower: "person", Camels: "people", Snake: "person", Snakes: "people", Words: "person", WordsPlural: "people", Article: "a"}},
	}
	for _, tt := range tests {
		got, err := newEntity(tt.name, tt.plural)
		if err != nil || got != tt.want {
			t.Errorf("newEntity(%q, %q) = %+v, %v; want %+v", tt.name, tt.plural, got, err, tt.want)
		}
	}
	for in, want := range map[string]string{"box": "boxes", "key": "keys", "batch": "batches", "bus": "buses", "dish": "dishes", "day": "days"} {
		if got := pluralize(in); got != want {
			t.Errorf("pluralize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestInsertBefore(t *testing.T) {
	src := "a\n\t// zen:x\nb\n"
	got, err := insertBefore([]byte(src), "zen:x", "one\ntwo")
	if err != nil || string(got) != "a\n\tone\n\ttwo\n\t// zen:x\nb\n" {
		t.Errorf("got %q, %v", got, err)
	}
	if _, err := insertBefore([]byte(src), "zen:y", "z"); !errors.Is(err, errMarker) {
		t.Errorf("missing marker: err %v", err)
	}
	if _, err := insertBefore([]byte(src+src), "zen:x", "z"); err == nil {
		t.Error("duplicate marker: no error")
	}
	if _, err := insertBefore([]byte("// zen:xy\n"), "zen:x", "z"); !errors.Is(err, errMarker) {
		t.Errorf("prefix of another marker matched: %v", err)
	}
}

func TestDurationCode(t *testing.T) {
	for d, want := range map[time.Duration]string{
		time.Minute:             "time.Minute",
		5 * time.Minute:         "5 * time.Minute",
		24 * time.Hour:          "24 * time.Hour",
		90 * time.Second:        "90 * time.Second",
		1500 * time.Millisecond: "1500 * time.Millisecond",
		1500:                    "time.Duration(1500)",
	} {
		if got := durationCode(d); got != want {
			t.Errorf("durationCode(%v) = %q, want %q", d, got, want)
		}
	}
}

// TestAddClientTwice: a second client adds no second grpcx import, and the
// same client cannot be added twice.
func TestAddClientTwice(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "proj")
	if err := scaffold.Write(dir, renderSvc(t, serviceGoldenCases["service-postgres"])); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		code int
		want string
	}{
		{"billing", 0, "Added client billing"},
		{"ledger", 0, "Added client ledger"},
		{"billing", 1, "already exists"},
	}
	for _, tt := range tests {
		var out bytes.Buffer
		if code := run(t.Context(), []string{"add", "client", tt.name, "-dir", dir}, nil, &out, &out); code != tt.code || !strings.Contains(out.String(), tt.want) {
			t.Fatalf("add client %s: exit %d, want %d; output %q lacks %q", tt.name, code, tt.code, out.String(), tt.want)
		}
	}
	deps, err := os.ReadFile(filepath.Join(dir, "internal", "app", "deps.go")) //nolint:gosec // file in the test project
	if err != nil {
		t.Fatal(err)
	}
	if n := bytes.Count(deps, []byte(`"github.com/dungnh3/kitchen/grpcx"`)); n != 1 {
		t.Errorf("grpcx imported %d times", n)
	}
}

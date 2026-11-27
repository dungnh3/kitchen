package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"slices"
	"strings"

	"charm.land/huh/v2"

	"github.com/dungnh3/kitchen/zen/internal/scaffold"
)

// serviceVersions are the dependency and tool versions written into
// generated services. Bump them together with the golden files
// (go test ./... -update) and keep the library versions in line with the
// kitchen modules, so "go mod tidy" does not have to move them.
var serviceVersions = struct {
	Buf, Sqlc                                string
	Protobuf, GRPC, ProtocGenGoGRPC, Gateway string
	GenprotoAPI, ProtovalidateGen            string
	Pgx, Env, UUID, Prometheus, XText        string
	Testcontainers, Goleak                   string
	Redis, Miniredis                         string
	GCPPubSub, FranzGo                       string
	AWS, AWSConfig, SQS                      string
}{
	Buf:              "v1.75.0",
	Sqlc:             "v1.31.1",
	Protobuf:         "v1.36.12",
	GRPC:             "v1.85.0",
	ProtocGenGoGRPC:  "v1.6.2",
	Gateway:          "v2.34.0",
	GenprotoAPI:      "v0.0.0-20261110000943-09e21b237f2c",
	ProtovalidateGen: "v1.36.12-20260825204119-511051f7f437.2",
	Pgx:              "v5.13.0",
	Env:              "v11.4.2",
	UUID:             "v1.6.0",
	Prometheus:       "v1.25.0",
	XText:            "v0.44.0",
	Testcontainers:   "v0.45.0",
	Goleak:           "v1.3.0",
	Redis:            "v9.23.0",
	Miniredis:        "v2.40.0",
	GCPPubSub:        "v2.9.0",
	FranzGo:          "v1.22.3",
	AWS:              "v1.48.0",
	AWSConfig:        "v1.33.21",
	SQS:              "v1.53.0",
}

// localImages are the images of the local dependencies in compose.yaml;
// Postgres is also the image of the integration test.
var localImages = struct {
	Postgres, MySQL, Redis, PubSubEmulator, Kafka, SQS string
}{
	Postgres:       "postgres:18-alpine",
	MySQL:          "mysql:8.4",
	Redis:          "redis:8.6-alpine",
	PubSubEmulator: "gcr.io/google.com/cloudsdktool/google-cloud-cli:555.0.0-emulators",
	Kafka:          "apache/kafka:4.2.0",
	SQS:            "softwaremill/elasticmq-native:1.7.0",
}

// Databases.
const (
	dbPostgres = "postgres"
	dbMySQL    = "mysql"
	dbNone     = "none"
)

// Authentication modes (-auth).
const (
	authJWKS          = "jwks"
	authIntrospection = "introspection"
)

// Optional features (-with). Each one wires one canonical kitchen package.
const (
	withRedis    = "redis"
	withCache    = "cache"
	withLease    = "lease"
	withPubSub   = "pubsub"
	withOutbox   = "outbox"
	withJobs     = "jobs"
	withWebhooks = "webhooks"
)

// features lists the -with values in the order they are shown.
var features = []struct{ name, desc string }{
	{withRedis, "Redis client (redisx), shared rate limits"},
	{withCache, "Read-through Redis cache in front of the item store (cache)"},
	{withLease, "Leader-elected periodic jobs in the worker (lease on Redis)"},
	{withPubSub, "Events: publisher, consumer in the worker (pubsub)"},
	{withOutbox, "Transactional outbox and relay (pubsub/outbox/pgx); needs postgres and pubsub"},
	{withJobs, "Background tasks stored in the database, run by the worker (jobs); needs postgres or mysql"},
	{withWebhooks, "Webhooks sent to customers' endpoints, retried for hours (webhook/delivery); needs jobs"},
}

// Message brokers (-pubsub).
const (
	brokerGCP   = "gcp"
	brokerKafka = "kafka"
	brokerSQS   = "sqs"
)

// protoPkgDir is the placeholder directory in templates/service/proto that
// becomes the proto package name.
const protoPkgDir = "_pkg_"

type serviceParams struct {
	projectParams
	DB         string
	NoGenerate bool
	DryRun     bool
	// With lists the optional features, sorted and without duplicates
	// after applyDefaults.
	With []string
	// Broker is the message broker; templates use it only with PubSub.
	Broker string
	// brokerSet records an explicit -pubsub flag.
	brokerSet bool
	// Auth is the token verification: jwks or introspection.
	Auth string
}

// Postgres reports whether the service uses PostgreSQL.
func (p serviceParams) Postgres() bool { return p.DB == dbPostgres }

// MySQL reports whether the service uses MySQL.
func (p serviceParams) MySQL() bool { return p.DB == dbMySQL }

// SQL reports whether the service has a SQL database, migrations and sqlc.
func (p serviceParams) SQL() bool { return p.Postgres() || p.MySQL() }

// DBName, StoreDir and DBHandle name the SQL database, its store package and
// the Go type of its connection.
func (p serviceParams) DBName() string {
	if p.MySQL() {
		return "MySQL"
	}
	return "PostgreSQL"
}

func (p serviceParams) StoreDir() string {
	if p.MySQL() {
		return "mysqlstore"
	}
	return "pgstore"
}

func (p serviceParams) DBHandle() string {
	if p.MySQL() {
		return "*sql.DB"
	}
	return "*pgxpool.Pool"
}

// Cache reports whether item reads go through a Redis cache.
func (p serviceParams) Cache() bool { return p.Has(withCache) }

// Introspection reports whether tokens are verified by RFC 7662
// introspection instead of JWKS.
func (p serviceParams) Introspection() bool { return p.Auth == authIntrospection }

// Has reports whether feature f is enabled.
func (p serviceParams) Has(f string) bool { return slices.Contains(p.With, f) }

// Redis reports whether the service opens a Redis client.
func (p serviceParams) Redis() bool { return p.Has(withRedis) }

// Lease reports whether the worker runs leader-elected jobs.
func (p serviceParams) Lease() bool { return p.Has(withLease) }

// PubSub reports whether the service publishes and consumes events.
func (p serviceParams) PubSub() bool { return p.Has(withPubSub) }

// Jobs reports whether the service has background tasks in its database.
func (p serviceParams) Jobs() bool { return p.Has(withJobs) }

// Webhooks reports whether the worker sends webhooks.
func (p serviceParams) Webhooks() bool { return p.Has(withWebhooks) }

// Outbox reports whether events go through the transactional outbox.
func (p serviceParams) Outbox() bool { return p.Has(withOutbox) }

// GCP, Kafka and SQS report whether the service publishes and consumes
// through that broker.
func (p serviceParams) GCP() bool   { return p.PubSub() && p.Broker == brokerGCP }
func (p serviceParams) Kafka() bool { return p.PubSub() && p.Broker == brokerKafka }
func (p serviceParams) SQS() bool   { return p.PubSub() && p.Broker == brokerSQS }

// Worker reports whether the service has a "worker" command.
func (p serviceParams) Worker() bool { return p.PubSub() || p.Lease() || p.Jobs() }

// Commands is the usage of the server binary.
func (p serviceParams) Commands() string {
	cmds := []string{"serve"}
	if p.Worker() {
		cmds = append(cmds, "worker")
	}
	if p.SQL() {
		cmds = append(cmds, "migrate up|down|status")
	}
	return strings.Join(cmds, " | ")
}

// Compose reports whether the service has local dependencies to run in
// Docker Compose.
func (p serviceParams) Compose() bool { return p.SQL() || p.Redis() || p.PubSub() }

// ProtoPackage is the proto package (without ".v1") and the directory under
// proto/ and gen/: the name with dashes turned into underscores.
func (p serviceParams) ProtoPackage() string { return strings.ReplaceAll(p.Name, "-", "_") }

type serviceTemplateData struct {
	serviceParams
	Versions      any
	GoDirective   string
	GoImage       string
	RuntimeImage  string
	PostgresImage string
	Images        any
	// CLIVersion is the zen version that CI and "make check" run.
	CLIVersion          string
	GolangciLintVersion string
	Actions             actionPins
}

func newService(ctx context.Context, args []string, stdin *os.File, stdout, stderr io.Writer) error {
	p, err := parseServiceFlags(args, stderr)
	if err != nil {
		return err
	}
	if p.Module == "" && !p.Yes && isTerminal(stdin) {
		if err := promptService(ctx, &p); err != nil {
			return err
		}
	}
	p.applyDefaults()
	if err := p.validate(); err != nil {
		return err
	}
	files, err := renderService(p)
	if err != nil {
		return err
	}
	if p.DryRun {
		_, _ = fmt.Fprintf(stdout, "Would create %s (%d files):\n", p.Dir, len(files))
		for _, rel := range files.Paths() {
			mark := ""
			if slices.Contains(serviceManaged, rel) {
				mark = "  (managed)"
			}
			_, _ = fmt.Fprintf(stdout, "  %s%s\n", rel, mark)
		}
		return nil
	}
	if err := scaffold.Write(p.Dir, files); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "Created %s (%d files).\n", p.Dir, len(files))
	manual := "  go mod tidy -e\n  make generate\n  go mod tidy\n"
	if p.NoGenerate {
		_, _ = fmt.Fprintf(stdout, "\nNext steps:\n  cd %s\n%s  make test\n  make run\n", p.Dir, manual)
		return nil
	}
	_, _ = fmt.Fprintln(stdout, "Generating code (this downloads modules and buf on the first run)...")
	if err := generateService(ctx, p.Dir, nil, stdout, stderr); err != nil {
		return fmt.Errorf("%w\nThe project is in %s; finish it by hand:\n  cd %s\n%s", err, p.Dir, p.Dir, manual)
	}
	_, _ = fmt.Fprintf(stdout, "\nNext steps:\n  cd %s\n  make test\n  make run\n", p.Dir)
	return nil
}

// generateService resolves the modules, runs buf and tidies go.mod in dir.
// env, when non-nil, replaces the environment of the commands. The sqlc
// output ships pre-generated, so sqlc (which needs cgo) is not run here.
func generateService(ctx context.Context, dir string, env []string, stdout, stderr io.Writer) error {
	steps := [][]string{
		// -e: the packages under gen/ do not exist yet.
		{"go", "mod", "tidy", "-e"},
		{"go", "run", "github.com/bufbuild/buf/cmd/buf@" + serviceVersions.Buf, "generate"},
		{"go", "mod", "tidy"},
	}
	for _, s := range steps {
		_, _ = fmt.Fprintf(stdout, "  %s\n", strings.Join(s, " "))
		cmd := exec.CommandContext(ctx, s[0], s[1:]...) //nolint:gosec // fixed commands
		cmd.Dir = dir
		cmd.Env = env
		cmd.Stdout, cmd.Stderr = stdout, stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("%s: %w", strings.Join(s, " "), err)
		}
	}
	return nil
}

func parseServiceFlags(args []string, stderr io.Writer) (serviceParams, error) {
	var p serviceParams
	fs := flag.NewFlagSet("zen new service", flag.ContinueOnError)
	fs.SetOutput(stderr)
	p.addFlags(fs, "github.com/acme/orders", "service name, used for the proto package and telemetry")
	fs.StringVar(&p.DB, "db", dbPostgres, `database: "postgres" (pgx), "mysql" (database/sql), both with sqlc and goose migrations, or "none"`)
	fs.StringVar(&p.Auth, "auth", authJWKS, `token verification: "jwks" (JWTs) or "introspection" (RFC 7662, opaque tokens)`)
	fs.BoolVar(&p.NoGenerate, "no-generate", false, "do not run go mod tidy and buf generate after writing the project")
	fs.BoolVar(&p.DryRun, "dry-run", false, "list the files that would be generated; write nothing")
	fs.Func("with", "comma-separated optional features: "+featureNames(), func(v string) error {
		for f := range strings.SplitSeq(v, ",") {
			if f = strings.TrimSpace(f); f != "" {
				p.With = append(p.With, f)
			}
		}
		return nil
	})
	fs.StringVar(&p.Broker, "pubsub", brokerGCP, `message broker with -with pubsub: "gcp", "kafka" or "sqs"`)
	if err := parseFlags(fs, args); err != nil {
		return p, err
	}
	fs.Visit(func(f *flag.Flag) { p.brokerSet = p.brokerSet || f.Name == "pubsub" })
	return p, nil
}

func featureNames() string {
	names := make([]string, len(features))
	for i, f := range features {
		names[i] = f.name
	}
	return strings.Join(names, ", ")
}

func (p *serviceParams) applyDefaults() {
	p.projectParams.applyDefaults("A gRPC and REST service.")
	// A lease and the cache live in Redis.
	if (p.Lease() || p.Cache()) && !p.Redis() {
		p.With = append(p.With, withRedis)
	}
	if p.Auth == "" {
		p.Auth = authJWKS
	}
	slices.Sort(p.With)
	p.With = slices.Compact(p.With)
	if p.Broker == "" {
		p.Broker = brokerGCP
	}
}

func (p *serviceParams) validate() error {
	errs := p.projectParams.validate()
	switch p.DB {
	case dbPostgres, dbMySQL, dbNone:
	default:
		errs = append(errs, fmt.Errorf("-db %q: want %q, %q or %q", p.DB, dbPostgres, dbMySQL, dbNone))
	}
	switch p.Auth {
	case authJWKS, authIntrospection:
	default:
		errs = append(errs, fmt.Errorf("-auth %q: want %q or %q", p.Auth, authJWKS, authIntrospection))
	}
	for _, f := range p.With {
		if !slices.ContainsFunc(features, func(x struct{ name, desc string }) bool { return x.name == f }) {
			errs = append(errs, fmt.Errorf("-with %q: want one of %s", f, featureNames()))
		}
	}
	if p.Outbox() && (!p.Postgres() || !p.PubSub()) {
		errs = append(errs, errors.New("-with outbox needs -db postgres and -with pubsub"))
	}
	if p.Jobs() && !p.SQL() {
		errs = append(errs, errors.New("-with jobs needs -db postgres or -db mysql"))
	}
	if p.Webhooks() && !p.Jobs() {
		errs = append(errs, errors.New("-with webhooks needs -with jobs"))
	}
	switch p.Broker {
	case brokerGCP, brokerKafka, brokerSQS:
	default:
		errs = append(errs, fmt.Errorf("-pubsub %q: want %q, %q or %q", p.Broker, brokerGCP, brokerKafka, brokerSQS))
	}
	if p.brokerSet && !p.PubSub() {
		errs = append(errs, errors.New("-pubsub needs -with pubsub"))
	}
	return errors.Join(errs...)
}

// fileRules lists template paths (or directory prefixes ending in "/")
// that are generated only when keep returns true.
var fileRules = []struct {
	paths []string
	keep  func(serviceParams) bool
}{
	{[]string{"migrations/", "sqlc.yaml", "internal/app/migrate.go"}, serviceParams.SQL},
	{[]string{"internal/store/pgstore/"}, serviceParams.Postgres},
	{[]string{"internal/store/mysqlstore/"}, serviceParams.MySQL},
	{[]string{"internal/store/cachestore/"}, serviceParams.Cache},
	{[]string{"migrations/00002_create_outbox.sql"}, serviceParams.Outbox},
	{[]string{"internal/app/redis.go"}, serviceParams.Redis},
	{[]string{"internal/events/", "internal/consumer/"}, serviceParams.PubSub},
	{[]string{"internal/app/broker_gcp.go"}, serviceParams.GCP},
	{[]string{"internal/app/broker_kafka.go"}, serviceParams.Kafka},
	{[]string{"internal/app/broker_sqs.go"}, serviceParams.SQS},
	{[]string{"internal/events/publish.go", "internal/events/publish_test.go"}, func(p serviceParams) bool { return p.PubSub() && !p.Outbox() }},
	{[]string{"internal/job/"}, serviceParams.Lease},
	{[]string{"migrations/00003_create_jobs.sql", "internal/app/jobs.go", "internal/task/"}, serviceParams.Jobs},
	{[]string{"internal/outbound/"}, serviceParams.Webhooks},
	{[]string{"internal/app/worker.go"}, serviceParams.Worker},
	{[]string{"compose.yaml"}, serviceParams.Compose},
}

func keepFile(p serviceParams, rel string) bool {
	for _, r := range fileRules {
		if matchesAny(rel, r.paths) && !r.keep(p) {
			return false
		}
	}
	return true
}

func renderService(p serviceParams) (scaffold.Files, error) {
	files, err := scaffold.Render(templatesFS, "templates/service", serviceTemplateData{
		serviceParams:       p,
		Versions:            serviceVersions,
		GoDirective:         goDirective,
		GoImage:             goImage,
		RuntimeImage:        runtimeImage,
		PostgresImage:       localImages.Postgres,
		Images:              localImages,
		CLIVersion:          pinnedCLIVersion(),
		GolangciLintVersion: golangciLintVersion,
		Actions:             actions,
	})
	if err != nil {
		return nil, err
	}
	out := scaffold.Files{}
	for rel, b := range files {
		if !keepFile(p, rel) {
			continue
		}
		// The jobs migration follows the outbox's; without an outbox it is
		// the second one.
		if rel == "migrations/00003_create_jobs.sql" && !p.Outbox() {
			rel = "migrations/00002_create_jobs.sql"
		}
		if dir, rest, ok := strings.Cut(rel, "proto/"+protoPkgDir+"/"); ok && dir == "" {
			rel = path.Join("proto", p.ProtoPackage(), rest)
		}
		// One broker file is kept; it is always broker.go.
		if strings.HasPrefix(rel, "internal/app/broker_") {
			rel = "internal/app/broker.go"
		}
		out[rel] = b
	}
	// The example domain.
	item, err := newEntity("item", "")
	if err != nil {
		return nil, err
	}
	dom, err := renderDomain(p, item, "00001", p.Outbox())
	if err != nil {
		return nil, err
	}
	for rel, b := range dom {
		out[rel] = b
	}
	if err := addManaged(out, p); err != nil {
		return nil, err
	}
	return out, nil
}

// pinnedCLIVersion is the zen version that generated projects run for
// "zen check": this CLI's when it is a release, else the default
// library version.
func pinnedCLIVersion() string {
	if v := cliVersion(); semverRE.MatchString(v) {
		return v
	}
	return defaultKitchenVersion
}

func matchesAny(rel string, patterns []string) bool {
	for _, pat := range patterns {
		if rel == pat || strings.HasSuffix(pat, "/") && strings.HasPrefix(rel, pat) {
			return true
		}
	}
	return false
}

func promptService(ctx context.Context, p *serviceParams) error {
	form := huh.NewForm(
		huh.NewGroup(
			huh.NewInput().Title("Go module path").Placeholder("github.com/acme/orders").
				Value(&p.Module).Validate(validateModule),
			huh.NewInput().Title("Service name").Description("Leave empty to use the last element of the module path.").
				Value(&p.Name),
			huh.NewInput().Title("Description").Value(&p.Description),
			huh.NewSelect[string]().Title("Database").Options(
				huh.NewOption("PostgreSQL (pgx, sqlc, goose migrations)", dbPostgres),
				huh.NewOption("MySQL (database/sql, sqlc, goose migrations)", dbMySQL),
				huh.NewOption("None", dbNone),
			).Value(&p.DB),
			huh.NewMultiSelect[string]().Title("Optional features").
				Description("Each one wires a kitchen package; outbox needs PostgreSQL and pubsub, jobs needs a database, webhooks needs jobs.").
				Options(featureOptions()...).Value(&p.With).
				Validate(func(with []string) error {
					if slices.Contains(with, withOutbox) && (p.DB != dbPostgres || !slices.Contains(with, withPubSub)) {
						return errors.New("outbox needs PostgreSQL and pubsub")
					}
					if slices.Contains(with, withJobs) && p.DB == dbNone {
						return errors.New("jobs needs PostgreSQL or MySQL")
					}
					if slices.Contains(with, withWebhooks) && !slices.Contains(with, withJobs) {
						return errors.New("webhooks needs jobs")
					}
					return nil
				}),
			huh.NewSelect[string]().Title("Token verification").Options(
				huh.NewOption("JWTs checked against the issuer's JWKS", authJWKS),
				huh.NewOption("Opaque tokens through RFC 7662 introspection", authIntrospection),
			).Value(&p.Auth),
			huh.NewSelect[string]().Title("CI workflow").Options(
				huh.NewOption("Self-contained GitHub Actions", ciStandalone),
				huh.NewOption("Reusable workflow pinned to a commit", ciReusable),
			).Value(&p.CI),
		),
		huh.NewGroup(
			huh.NewSelect[string]().Title("Message broker").Options(
				huh.NewOption("Google Cloud Pub/Sub", brokerGCP),
				huh.NewOption("Kafka", brokerKafka),
				huh.NewOption("Amazon SQS", brokerSQS),
			).Value(&p.Broker),
		).WithHideFunc(func() bool { return !slices.Contains(p.With, withPubSub) }),
		huh.NewGroup(
			huh.NewInput().Title("Reusable workflow").
				Placeholder("acme/devops-workflows/.github/workflows/go.yml@<sha>").
				Value(&p.CIWorkflow).Validate(validateWorkflow),
		).WithHideFunc(func() bool { return p.CI != ciReusable }),
	)
	return form.RunWithContext(ctx)
}

func featureOptions() []huh.Option[string] {
	opts := make([]huh.Option[string], len(features))
	for i, f := range features {
		opts[i] = huh.NewOption(f.name+": "+f.desc, f.name)
	}
	return opts
}

package pgx

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dungnh3/kitchen/log"
)

// TestClaimPlanUsesKeyIndex builds a backlog with a blocked hot key (the
// head backed off, 50k rows behind it) plus many small keys, and checks the
// claim statement's plan: the head-of-key check must probe the key index,
// not walk the primary key once per candidate row.
func TestClaimPlanUsesKeyIndex(t *testing.T) {
	dsn := os.Getenv("KITCHEN_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("KITCHEN_TEST_PG_DSN not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	table := fmt.Sprintf("outbox_plan_%d", time.Now().UnixNano())
	s, err := New(pool, WithTable(table), WithLogger(log.Discard()))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTable(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), "DROP TABLE "+s.ident) }()
	for _, q := range []string{
		`INSERT INTO %[1]s (topic, ordering_key, payload) SELECT 'orders', 'hot', '\x00' FROM generate_series(1, 50000)`,
		`UPDATE %[1]s SET available_at = now() + interval '1 hour' WHERE id = (SELECT min(id) FROM %[1]s)`,
		`INSERT INTO %[1]s (topic, ordering_key, payload) SELECT 'orders', '', '\x00' FROM generate_series(1, 100)`,
		`INSERT INTO %[1]s (topic, ordering_key, payload) SELECT 'orders', 'k' || (g %% 2000), '\x00' FROM generate_series(1, 20000) g`,
		`ANALYZE %[1]s`,
	} {
		if _, err := pool.Exec(ctx, fmt.Sprintf(q, s.ident)); err != nil {
			t.Fatal(err)
		}
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	start := time.Now()
	rows, err := tx.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) "+s.claimQuery(), 100, int64(60000), "", 100*ageWindowFactor)
	if err != nil {
		t.Fatal(err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(lines, "\n")
	t.Logf("claim over a 70k-row backlog took %s:\n%s", time.Since(start), plan)

	base := strings.ReplaceAll(table, ".", "_")
	if !strings.Contains(plan, base+"_key_idx") {
		t.Errorf("the head-of-key check does not use %s_key_idx", base)
	}
	// A head-of-key check that walks the primary key with a key filter
	// shows up as an index scan of _pkey aliased h.
	if regexp.MustCompile(`Index (Only )?Scan using \S+_pkey on \S+ h\b`).MatchString(plan) {
		t.Errorf("the head-of-key check walks the primary key")
	}
}

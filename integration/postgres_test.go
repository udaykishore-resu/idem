package integration

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"

	"github.com/udaykishore-resu/idem"
	"github.com/udaykishore-resu/idem/pgstore"
	"github.com/udaykishore-resu/idem/storetest"
)

// drivers are the database/sql driver names the store is tested with.
var drivers = []string{"pgx", "postgres"}

func uniqueKey(t *testing.T) string {
	t.Helper()
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return "integration-" + hex.EncodeToString(b[:])
}

func openDB(t *testing.T, driver string) *sql.DB {
	t.Helper()
	dsn := os.Getenv("IDEM_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("IDEM_POSTGRES_DSN is not set")
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(16)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("cannot reach PostgreSQL: %v", err)
	}
	return db
}

// newStore returns a store on the shared test table, plus an orders table for
// the transactional tests.
func newStore(t *testing.T, db *sql.DB) *pgstore.Store {
	t.Helper()
	ctx := context.Background()
	store := pgstore.New(db, pgstore.WithTable("idem_integration"))
	if err := store.CreateTable(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTable(ctx); err != nil {
		t.Fatalf("CreateTable is not repeatable: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS idem_integration_orders (
		id text PRIMARY KEY, request_key text NOT NULL, amount integer NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	return store
}

func eachDriver(t *testing.T, fn func(t *testing.T, db *sql.DB)) {
	for _, driver := range drivers {
		t.Run(driver, func(t *testing.T) { fn(t, openDB(t, driver)) })
	}
}

func TestPostgresStore(t *testing.T) {
	eachDriver(t, func(t *testing.T, db *sql.DB) {
		store := newStore(t, db)
		storetest.Run(t, func(*testing.T) idem.Store { return store })
	})
}

type order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

func orderCount(t *testing.T, db *sql.DB, key string) int {
	t.Helper()
	var n int
	err := db.QueryRow(`SELECT count(*) FROM idem_integration_orders WHERE request_key = $1`, key).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// placeOrder returns the function under test: it inserts one order row.
func placeOrder(key string) func(context.Context, *sql.Tx) (order, error) {
	return func(ctx context.Context, tx *sql.Tx) (order, error) {
		o := order{ID: "order-" + key + "-" + hex.EncodeToString(randomBytes(4)), Amount: 500}
		_, err := tx.ExecContext(ctx,
			`INSERT INTO idem_integration_orders (id, request_key, amount) VALUES ($1, $2, $3)`,
			o.ID, key, o.Amount)
		return o, err
	}
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

func TestDoTxCommitsWorkAndRecordTogether(t *testing.T) {
	eachDriver(t, func(t *testing.T, db *sql.DB) {
		ctx := context.Background()
		engine := idem.New(newStore(t, db))
		key := uniqueKey(t)

		first, replayed, err := pgstore.DoTx(ctx, engine, key, placeOrder(key))
		if err != nil || replayed {
			t.Fatalf("first call: replayed = %v, err = %v", replayed, err)
		}
		second, replayed, err := pgstore.DoTx(ctx, engine, key, placeOrder(key))
		if err != nil || !replayed {
			t.Fatalf("second call: replayed = %v, err = %v, want a replay", replayed, err)
		}
		if first != second {
			t.Errorf("replayed %+v, want %+v", second, first)
		}
		if n := orderCount(t, db, key); n != 1 {
			t.Errorf("%d orders in the table, want 1", n)
		}
	})
}

func TestDoTxRollsBackAndReleasesOnError(t *testing.T) {
	eachDriver(t, func(t *testing.T, db *sql.DB) {
		ctx := context.Background()
		engine := idem.New(newStore(t, db))
		key := uniqueKey(t)
		declined := errors.New("card declined")

		_, _, err := pgstore.DoTx(ctx, engine, key, func(ctx context.Context, tx *sql.Tx) (order, error) {
			if _, err := placeOrder(key)(ctx, tx); err != nil {
				return order{}, err
			}
			return order{}, declined // after the insert
		})
		if !errors.Is(err, declined) {
			t.Fatalf("err = %v, want the error from fn", err)
		}
		if n := orderCount(t, db, key); n != 0 {
			t.Fatalf("%d orders survived a failed transaction, want 0", n)
		}

		// The key is free again at once, without waiting for the lease.
		if _, replayed, err := pgstore.DoTx(ctx, engine, key, placeOrder(key)); err != nil || replayed {
			t.Fatalf("retry: replayed = %v, err = %v", replayed, err)
		}
		if n := orderCount(t, db, key); n != 1 {
			t.Errorf("%d orders after the retry, want 1", n)
		}
	})
}

func TestDoTxRollsBackOnPanic(t *testing.T) {
	eachDriver(t, func(t *testing.T, db *sql.DB) {
		ctx := context.Background()
		engine := idem.New(newStore(t, db))
		key := uniqueKey(t)

		func() {
			defer func() { _ = recover() }()
			_, _, _ = pgstore.DoTx(ctx, engine, key, func(ctx context.Context, tx *sql.Tx) (order, error) {
				if _, err := placeOrder(key)(ctx, tx); err != nil {
					return order{}, err
				}
				panic("bug in handler")
			})
		}()
		if n := orderCount(t, db, key); n != 0 {
			t.Fatalf("%d orders survived a panic, want 0", n)
		}
		if _, replayed, err := pgstore.DoTx(ctx, engine, key, placeOrder(key)); err != nil || replayed {
			t.Fatalf("retry: replayed = %v, err = %v", replayed, err)
		}
	})
}

// TestDoTxStaleHolderCannotCommit is the reason DoTx exists. A worker stalls
// for longer than its lease with its transaction still open. A second worker
// takes the key over and does the work. When the first one wakes up, its
// transaction must not commit: the order may exist once, not twice.
func TestDoTxStaleHolderCannotCommit(t *testing.T) {
	eachDriver(t, func(t *testing.T, db *sql.DB) {
		ctx := context.Background()
		// No heartbeat: this is a process that was frozen, not a slow one.
		engine := idem.New(newStore(t, db), idem.WithLease(300*time.Millisecond), idem.WithHeartbeat(0))
		key := uniqueKey(t)

		inserted := make(chan struct{})
		wake := make(chan struct{})
		staleErr := make(chan error, 1)
		go func() {
			_, _, err := pgstore.DoTx(ctx, engine, key, func(ctx context.Context, tx *sql.Tx) (order, error) {
				o, err := placeOrder(key)(ctx, tx)
				close(inserted)
				<-wake
				return o, err
			})
			staleErr <- err
		}()
		<-inserted
		time.Sleep(600 * time.Millisecond) // the lease runs out

		fresh, replayed, err := pgstore.DoTx(ctx, engine, key, placeOrder(key))
		if err != nil || replayed {
			t.Fatalf("takeover: replayed = %v, err = %v", replayed, err)
		}

		close(wake)
		if err := <-staleErr; !errors.Is(err, idem.ErrLeaseLost) {
			t.Fatalf("stale holder: err = %v, want ErrLeaseLost", err)
		}
		if n := orderCount(t, db, key); n != 1 {
			t.Fatalf("%d orders in the table, want exactly 1", n)
		}

		got, replayed, err := pgstore.DoTx(ctx, engine, key, placeOrder(key))
		if err != nil || !replayed || got != fresh {
			t.Fatalf("replay: got %+v, replayed = %v, err = %v, want %+v", got, replayed, err, fresh)
		}
	})
}

func TestDoTxConcurrentCallers(t *testing.T) {
	eachDriver(t, func(t *testing.T, db *sql.DB) {
		ctx := context.Background()
		engine := idem.New(newStore(t, db))
		key := uniqueKey(t)
		const callers = 12

		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			results = map[order]int{}
			runs    atomic.Int32
		)
		for range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				o, _, err := pgstore.DoTx(ctx, engine, key, func(ctx context.Context, tx *sql.Tx) (order, error) {
					runs.Add(1)
					time.Sleep(50 * time.Millisecond)
					return placeOrder(key)(ctx, tx)
				}, idem.WithWait(10*time.Second))
				if err != nil {
					t.Errorf("DoTx: %v", err)
					return
				}
				mu.Lock()
				results[o]++
				mu.Unlock()
			}()
		}
		wg.Wait()

		if got := runs.Load(); got != 1 {
			t.Errorf("fn ran %d times, want 1", got)
		}
		if len(results) != 1 {
			t.Errorf("callers saw %d different orders, want all of them to see the same one", len(results))
		}
		if n := orderCount(t, db, key); n != 1 {
			t.Errorf("%d orders in the table, want 1", n)
		}
	})
}

func TestDoTxHeartbeatOutlivesLease(t *testing.T) {
	eachDriver(t, func(t *testing.T, db *sql.DB) {
		ctx := context.Background()
		engine := idem.New(newStore(t, db), idem.WithLease(300*time.Millisecond))
		key := uniqueKey(t)

		// The transaction runs for three leases. The heartbeat updates the
		// record from another connection in the meantime, and the final
		// update inside the transaction must still succeed.
		_, _, err := pgstore.DoTx(ctx, engine, key, func(ctx context.Context, tx *sql.Tx) (order, error) {
			o, err := placeOrder(key)(ctx, tx)
			if err != nil {
				return o, err
			}
			select {
			case <-time.After(900 * time.Millisecond):
				return o, nil
			case <-ctx.Done():
				return o, context.Cause(ctx)
			}
		})
		if err != nil {
			t.Fatalf("DoTx: %v", err)
		}
		if n := orderCount(t, db, key); n != 1 {
			t.Errorf("%d orders in the table, want 1", n)
		}
	})
}

func TestWithTxTakesEffectOnCommit(t *testing.T) {
	eachDriver(t, func(t *testing.T, db *sql.DB) {
		ctx := context.Background()
		store := newStore(t, db)
		key := uniqueKey(t)

		// WithTx on its own: the record becomes visible only at commit.
		engine := idem.New(store)
		attempt, _, err := engine.Begin(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := attempt.Complete(pgstore.WithTx(ctx, tx), []byte(`"done"`)); err != nil {
			t.Fatalf("Complete inside the transaction: %v", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		claim, err := store.Acquire(ctx, key, "probe", "", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if claim.State != idem.StateInProgress {
			t.Fatalf("after rolling back: state = %v, want the record still in progress", claim.State)
		}

		// The same attempt can write the record again in a new transaction.
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := attempt.Complete(pgstore.WithTx(ctx, tx), []byte(`"done"`)); err != nil {
			t.Fatalf("second Complete: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		// A deferred Release after a commit must leave the record alone.
		_ = attempt.Release(ctx)
		if _, stored, err := engine.Begin(ctx, key); err != nil || string(stored) != `"done"` {
			t.Errorf("after committing: stored = %q, err = %v, want the record", stored, err)
		}
	})
}

func TestWithTxRollbackThenReleaseFreesKey(t *testing.T) {
	eachDriver(t, func(t *testing.T, db *sql.DB) {
		ctx := context.Background()
		engine := idem.New(newStore(t, db))
		key := uniqueKey(t)

		attempt, _, err := engine.Begin(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := attempt.Complete(pgstore.WithTx(ctx, tx), []byte(`"done"`)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if err := attempt.Release(ctx); err != nil {
			t.Fatalf("Release after the rollback: %v", err)
		}

		again, stored, err := engine.Begin(ctx, key)
		if err != nil || again == nil {
			t.Fatalf("Begin after the release: attempt = %v, stored = %q, err = %v, want the key to be free", again, stored, err)
		}
		_ = again.Release(ctx)
	})
}

func TestDeleteExpired(t *testing.T) {
	eachDriver(t, func(t *testing.T, db *sql.DB) {
		ctx := context.Background()
		store := newStore(t, db)
		expired, live := uniqueKey(t), uniqueKey(t)

		for key, lease := range map[string]time.Duration{expired: 50 * time.Millisecond, live: time.Minute} {
			if _, err := store.Acquire(ctx, key, "token", "", lease); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(150 * time.Millisecond)
		n, err := store.DeleteExpired(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n < 1 {
			t.Errorf("DeleteExpired removed %d rows, want at least the expired one", n)
		}
		var remaining int
		if err := db.QueryRow(`SELECT count(*) FROM idem_integration WHERE key IN ($1, $2)`, expired, live).Scan(&remaining); err != nil {
			t.Fatal(err)
		}
		if remaining != 1 {
			t.Errorf("%d of the two rows remain, want only the live one", remaining)
		}
	})
}

// TestDoTxCommitFailureFreesKey makes the commit itself fail, after the
// record has been written inside the transaction, by violating a deferred
// constraint. Nothing may remain: no rows, and no key held until its lease
// runs out.
func TestDoTxCommitFailureFreesKey(t *testing.T) {
	eachDriver(t, func(t *testing.T, db *sql.DB) {
		ctx := context.Background()
		engine := idem.New(newStore(t, db))
		key := uniqueKey(t)
		if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS idem_integration_deferred (
			id text, request_key text NOT NULL,
			CONSTRAINT idem_integration_deferred_id UNIQUE (id) DEFERRABLE INITIALLY DEFERRED)`); err != nil {
			t.Fatal(err)
		}
		insert := func(ctx context.Context, tx *sql.Tx, id string) error {
			_, err := tx.ExecContext(ctx,
				`INSERT INTO idem_integration_deferred (id, request_key) VALUES ($1, $2)`, id, key)
			return err
		}

		_, _, err := pgstore.DoTx(ctx, engine, key, func(ctx context.Context, tx *sql.Tx) (int, error) {
			if err := insert(ctx, tx, "dup-"+key); err != nil {
				return 0, err
			}
			return 1, insert(ctx, tx, "dup-"+key) // accepted now, rejected at commit
		})
		if err == nil {
			t.Fatal("DoTx succeeded although the commit must have failed")
		}

		begin := time.Now()
		v, replayed, err := pgstore.DoTx(ctx, engine, key, func(ctx context.Context, tx *sql.Tx) (int, error) {
			return 2, insert(ctx, tx, "ok-"+key)
		})
		if err != nil || replayed || v != 2 {
			t.Fatalf("retry: v = %d, replayed = %v, err = %v, want it to run", v, replayed, err)
		}
		if waited := time.Since(begin); waited > 5*time.Second {
			t.Errorf("the retry waited %v for the key", waited)
		}
		var rows int
		if err := db.QueryRow(`SELECT count(*) FROM idem_integration_deferred WHERE request_key = $1`, key).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Errorf("%d rows, want only the retry's", rows)
		}
	})
}

// TestDoTxWithStricterDefaultIsolation runs DoTx on connections whose default
// isolation level is REPEATABLE READ. The transaction outlives its first
// lease, so the heartbeat changes the record while the transaction's snapshot
// is already taken.
func TestDoTxWithStricterDefaultIsolation(t *testing.T) {
	for _, driver := range drivers {
		t.Run(driver, func(t *testing.T) {
			dsn := os.Getenv("IDEM_POSTGRES_DSN")
			if dsn == "" {
				t.Skip("IDEM_POSTGRES_DSN is not set")
			}
			u, err := url.Parse(dsn)
			if err != nil || u.Scheme == "" {
				t.Skip("IDEM_POSTGRES_DSN is not a URL; cannot add a session parameter")
			}
			q := u.Query()
			q.Set("default_transaction_isolation", "repeatable read")
			u.RawQuery = q.Encode()
			db, err := sql.Open(driver, u.String())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			ctx := context.Background()
			var level string
			if err := db.QueryRowContext(ctx, `SHOW default_transaction_isolation`).Scan(&level); err != nil {
				t.Fatal(err)
			}
			if level != "repeatable read" {
				t.Fatalf("session default is %q; the test did not set up what it means to test", level)
			}

			engine := idem.New(newStore(t, db), idem.WithLease(300*time.Millisecond))
			key := uniqueKey(t)
			_, _, err = pgstore.DoTx(ctx, engine, key, func(ctx context.Context, tx *sql.Tx) (order, error) {
				o, err := placeOrder(key)(ctx, tx)
				if err != nil {
					return o, err
				}
				select {
				case <-time.After(900 * time.Millisecond):
					return o, nil
				case <-ctx.Done():
					return o, context.Cause(ctx)
				}
			})
			if err != nil {
				t.Fatalf("DoTx: %v", err)
			}
			if n := orderCount(t, db, key); n != 1 {
				t.Errorf("%d orders in the table, want 1", n)
			}
		})
	}
}

// TestDuplicateDoesNotWaitForRowLock checks that a duplicate request is
// answered while another transaction holds the record's row lock, as DoTx
// does between writing the record and committing.
func TestDuplicateDoesNotWaitForRowLock(t *testing.T) {
	eachDriver(t, func(t *testing.T, db *sql.DB) {
		ctx := context.Background()
		store := newStore(t, db)
		key := uniqueKey(t)
		if _, err := store.Acquire(ctx, key, "holder", "fp", time.Minute); err != nil {
			t.Fatal(err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, `SELECT 1 FROM idem_integration WHERE key = $1 FOR UPDATE`, key); err != nil {
			t.Fatal(err)
		}

		probe, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		claim, err := store.Acquire(probe, key, "duplicate", "fp", time.Minute)
		if err != nil {
			t.Fatalf("Acquire behind a row lock: %v", err)
		}
		if claim.State != idem.StateInProgress {
			t.Errorf("state = %v, want in progress", claim.State)
		}
	})
}

package pgstore_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/udaykishore-resu/idem"
	"github.com/udaykishore-resu/idem/memstore"
	"github.com/udaykishore-resu/idem/pgstore"
)

// The tests that talk to PostgreSQL need a driver and therefore live in the
// integration module at the root of the repository. What is tested here needs
// no server.

// noServer is a connector for a database that cannot be reached.
type noServer struct{}

func (noServer) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("no server in unit tests")
}
func (noServer) Driver() driver.Driver { return nil }

func newDB(t *testing.T) *sql.DB {
	t.Helper()
	db := sql.OpenDB(noServer{})
	t.Cleanup(func() { db.Close() })
	return db
}

func TestSchema(t *testing.T) {
	schema := pgstore.New(newDB(t)).Schema()
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS idem_records",
		"key         text        PRIMARY KEY",
		"CREATE INDEX IF NOT EXISTS idem_records_expires_at_idx ON idem_records (expires_at)",
	} {
		if !strings.Contains(schema, want) {
			t.Errorf("schema lacks %q:\n%s", want, schema)
		}
	}

	schema = pgstore.New(newDB(t), pgstore.WithTable("billing.dedupe")).Schema()
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS billing.dedupe",
		"CREATE INDEX IF NOT EXISTS billing_dedupe_expires_at_idx ON billing.dedupe (expires_at)",
	} {
		if !strings.Contains(schema, want) {
			t.Errorf("schema lacks %q:\n%s", want, schema)
		}
	}
}

func TestTableNameIsValidated(t *testing.T) {
	for _, name := range []string{"idem_records", "billing.idem_records", "_t1", "Records"} {
		pgstore.WithTable(name) // must not panic
	}
	for _, name := range []string{
		"", "1records", "a.b.c", "records; DROP TABLE users", `"quoted"`, "with space", "a-b", "a.", ".b",
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("WithTable(%q) did not panic", name)
				}
			}()
			pgstore.WithTable(name)
		})
	}
}

func TestNewPanicsWithoutDB(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("New(nil) did not panic")
		}
	}()
	pgstore.New(nil)
}

func TestDatabaseErrorsAreWrapped(t *testing.T) {
	ctx := context.Background()
	store := pgstore.New(newDB(t))

	if _, err := store.Acquire(ctx, "k", "t", "", 1); err == nil || !strings.HasPrefix(err.Error(), "pgstore: acquire: ") {
		t.Errorf("Acquire: err = %v", err)
	}
	for op, err := range map[string]error{
		"complete": store.Complete(ctx, "k", "t", nil, 1),
		"release":  store.Release(ctx, "k", "t"),
		"extend":   store.Extend(ctx, "k", "t", 1),
	} {
		if err == nil || !strings.HasPrefix(err.Error(), "pgstore: "+op+": ") || errors.Is(err, idem.ErrLeaseLost) {
			t.Errorf("%s: err = %v, want a wrapped database error", op, err)
		}
	}
	if err := store.CreateTable(ctx); err == nil {
		t.Error("CreateTable: no error")
	}
	if _, err := store.DeleteExpired(ctx); err == nil {
		t.Error("DeleteExpired: no error")
	}
}

func TestDoTxRejectsOtherStores(t *testing.T) {
	engine := idem.New(memstore.New())
	_, _, err := pgstore.DoTx(context.Background(), engine, "key",
		func(context.Context, *sql.Tx) (int, error) {
			t.Error("fn ran without a transaction to run in")
			return 0, nil
		})
	if err == nil || !strings.Contains(err.Error(), "pgstore.Store") {
		t.Errorf("err = %v, want an explanation that the engine needs a pgstore.Store", err)
	}
}

func TestDoTxReportsAcquireFailure(t *testing.T) {
	engine := idem.New(pgstore.New(newDB(t)))
	_, _, err := pgstore.DoTx(context.Background(), engine, "key",
		func(context.Context, *sql.Tx) (int, error) {
			t.Error("fn ran although the key could not be acquired")
			return 0, nil
		})
	if err == nil || !strings.Contains(err.Error(), "acquire") {
		t.Errorf("err = %v, want the acquire failure", err)
	}
}

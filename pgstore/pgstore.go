// Package pgstore provides a PostgreSQL store for package idem, with
// transactional completion.
//
// It works through database/sql and does not import a driver, so it runs on
// whichever one your service already uses, such as pgx's stdlib adapter or
// lib/pq:
//
//	db, err := sql.Open("pgx", dsn)
//	...
//	store := pgstore.New(db)
//	if err := store.CreateTable(ctx); err != nil { ... }
//	engine := idem.New(store)
//
// # Transactional completion
//
// Keeping the records in the same database as your data makes one thing
// possible that no other store can offer: the record can commit in the same
// transaction as the changes it protects. Either both are durable or neither
// is, so a retry can never find the work done but unrecorded, nor recorded
// but undone. [DoTx] does this for you; [WithTx] is the building block.
//
// # Housekeeping
//
// Expired rows are treated as absent and are overwritten when their key is
// used again, but rows whose key never returns stay in the table. Call
// [Store.DeleteExpired] from a periodic job to remove them.
//
// All times are taken from the database server's clock, so the clocks of your
// application instances do not matter.
package pgstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/udaykishore-resu/idem"
)

// DefaultTable is the table used unless [WithTable] changes it.
const DefaultTable = "idem_records"

// Row states, as stored in the state column.
const (
	stateInProgress = 1
	stateCompleted  = 2
)

// Store is an [idem.Store] that keeps its records in a PostgreSQL table.
type Store struct {
	db    *sql.DB
	table string

	acquire, lookup, complete, release, extend, deleteExpired string
}

// Option configures a [Store].
type Option func(*Store)

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

// WithTable sets the table that holds the records, optionally qualified with
// a schema, as in "billing.idem_records". The default is [DefaultTable].
// The name is placed into SQL text, so WithTable panics unless it consists of
// plain identifiers.
func WithTable(name string) Option {
	if !identifier.MatchString(name) {
		panic(fmt.Sprintf("pgstore: invalid table name %q", name))
	}
	return func(s *Store) { s.table = name }
}

// New returns a store that keeps its records in db. It panics if db is nil.
// The table must exist; see [Store.CreateTable] and [Store.Schema].
func New(db *sql.DB, opts ...Option) *Store {
	if db == nil {
		panic("pgstore: db must not be nil")
	}
	s := &Store{db: db, table: DefaultTable}
	for _, opt := range opts {
		opt(s)
	}
	s.prepare()
	return s
}

var _ idem.Store = (*Store)(nil)

// prepare builds the statements. They use clock_timestamp() rather than
// now(): inside a transaction now() is frozen at the transaction's start,
// which would make a lease look valid for as long as the transaction runs.
// The literals 1 and 2 are stateInProgress and stateCompleted.
func (s *Store) prepare() {
	// lifetime is the expression for "this many milliseconds from now", with
	// the duration taken from parameter n.
	lifetime := func(n int) string {
		return fmt.Sprintf(`clock_timestamp() + $%d::double precision * interval '1 millisecond'`, n)
	}

	// Insert the row, or take over a row that has expired. A live row makes
	// the WHERE clause false, and then nothing is returned.
	s.acquire = `
INSERT INTO ` + s.table + ` AS r (key, token, fingerprint, state, result, expires_at)
VALUES ($1, $2, $3, 1, NULL, ` + lifetime(4) + `)
ON CONFLICT (key) DO UPDATE
   SET token = EXCLUDED.token,
       fingerprint = EXCLUDED.fingerprint,
       state = EXCLUDED.state,
       result = NULL,
       expires_at = EXCLUDED.expires_at
 WHERE r.expires_at <= clock_timestamp()
RETURNING r.token`

	s.lookup = `
SELECT state, fingerprint, result FROM ` + s.table + `
 WHERE key = $1 AND expires_at > clock_timestamp()`

	// A repeated Complete finds state = 2 and leaves the row as it is, but
	// still reports one affected row, which makes the call safe to retry.
	s.complete = `
UPDATE ` + s.table + `
   SET result = CASE WHEN state = 1 THEN $3::bytea ELSE result END,
       expires_at = CASE WHEN state = 1 THEN ` + lifetime(4) + ` ELSE expires_at END,
       state = 2
 WHERE key = $1 AND token = $2 AND expires_at > clock_timestamp()`

	s.release = `
DELETE FROM ` + s.table + `
 WHERE key = $1 AND token = $2 AND state = 1 AND expires_at > clock_timestamp()`

	s.extend = `
UPDATE ` + s.table + ` SET expires_at = ` + lifetime(3) + `
 WHERE key = $1 AND token = $2 AND state = 1 AND expires_at > clock_timestamp()`

	s.deleteExpired = `DELETE FROM ` + s.table + ` WHERE expires_at <= clock_timestamp()`
}

// Schema returns the DDL that creates the table and its index, for use in a
// migration. Both statements are idempotent.
func (s *Store) Schema() string {
	return strings.Join(s.ddl(), "\n")
}

func (s *Store) ddl() []string {
	index := strings.ReplaceAll(s.table, ".", "_") + "_expires_at_idx"
	return []string{
		`CREATE TABLE IF NOT EXISTS ` + s.table + ` (
    key         text        PRIMARY KEY,
    token       text        NOT NULL,
    fingerprint text        NOT NULL,
    state       smallint    NOT NULL, -- 1 in progress, 2 completed
    result      bytea,
    expires_at  timestamptz NOT NULL
);`,
		`CREATE INDEX IF NOT EXISTS ` + index + ` ON ` + s.table + ` (expires_at);`,
	}
}

// CreateTable creates the table and its index if they do not exist. Services
// that manage their schema with migrations can use [Store.Schema] instead.
func (s *Store) CreateTable(ctx context.Context) error {
	for _, stmt := range s.ddl() {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("pgstore: create table: %w", err)
		}
	}
	return nil
}

// DeleteExpired removes expired rows and reports how many it removed.
func (s *Store) DeleteExpired(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx, s.deleteExpired)
	if err != nil {
		return 0, fmt.Errorf("pgstore: delete expired: %w", err)
	}
	return res.RowsAffected()
}

// acquireAttempts bounds the loop in Acquire. A second round is only needed
// when the row changes between the two statements: it expired or was released
// just after the lookup missed it, or another caller claimed it first.
const acquireAttempts = 5

// Acquire implements [idem.Store].
//
// It looks before it writes. A duplicate request, which is the common case
// for a key that already exists, is answered by a plain SELECT that takes no
// lock and never waits behind another transaction. Only a key that looks
// free is claimed with the INSERT, which settles races atomically.
func (s *Store) Acquire(ctx context.Context, key, token, fingerprint string, lease time.Duration) (idem.Claim, error) {
	for range acquireAttempts {
		var (
			state  int
			stored string
			result []byte
		)
		err := s.db.QueryRowContext(ctx, s.lookup, key).Scan(&state, &stored, &result)
		switch {
		case err == nil && state == stateInProgress:
			return idem.Claim{State: idem.StateInProgress, Fingerprint: stored}, nil
		case err == nil && state == stateCompleted:
			return idem.Claim{State: idem.StateCompleted, Fingerprint: stored, Result: result}, nil
		case err == nil:
			return idem.Claim{}, fmt.Errorf("pgstore: acquire: row has unknown state %d", state)
		case !errors.Is(err, sql.ErrNoRows):
			return idem.Claim{}, fmt.Errorf("pgstore: acquire: %w", err)
		}

		var holder string
		err = s.db.QueryRowContext(ctx, s.acquire, key, token, fingerprint, millis(lease)).Scan(&holder)
		if err == nil {
			return idem.Claim{State: idem.StateAcquired}, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return idem.Claim{}, fmt.Errorf("pgstore: acquire: %w", err)
		}
		// Another caller claimed the key in between; look again.
	}
	return idem.Claim{}, errors.New("pgstore: acquire: the record kept changing; giving up")
}

// execer is the part of *sql.DB and *sql.Tx that the write paths need.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Complete implements [idem.Store]. If ctx carries a transaction, see
// [WithTx], the update runs inside it and takes effect when it commits.
func (s *Store) Complete(ctx context.Context, key, token string, result []byte, ttl time.Duration) error {
	if result == nil {
		result = []byte{}
	}
	var db execer = s.db
	if tx, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		db = tx
	}
	return s.held(ctx, db, "complete", s.complete, key, token, result, millis(ttl))
}

// Release implements [idem.Store].
func (s *Store) Release(ctx context.Context, key, token string) error {
	return s.held(ctx, s.db, "release", s.release, key, token)
}

// Extend implements [idem.Store].
func (s *Store) Extend(ctx context.Context, key, token string, lease time.Duration) error {
	return s.held(ctx, s.db, "extend", s.extend, key, token, millis(lease))
}

// held runs a statement that affects one row if token holds key and none if
// it does not.
func (s *Store) held(ctx context.Context, db execer, op, query string, args ...any) error {
	res, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("pgstore: %s: %w", op, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("pgstore: %s: %w", op, err)
	}
	if n == 0 {
		return idem.ErrLeaseLost
	}
	return nil
}

func millis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

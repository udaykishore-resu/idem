package pgstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/udaykishore-resu/idem"
)

type txKey struct{}

// WithTx returns a context that makes [Store.Complete] run inside tx instead
// of on its own connection. The record then becomes completed if and when tx
// commits, together with everything else tx changed.
//
// Pass the returned context to [idem.Attempt.Complete] before committing. If
// Complete fails, roll the transaction back: the attempt has lost its lease,
// and committing would apply the work a second time. If the commit fails or
// the transaction is rolled back afterwards, call [idem.Attempt.Release] to
// free the key; deferring it right after Begin covers every path, because it
// does nothing to a record that did commit.
//
// The transaction must run at READ COMMITTED, the PostgreSQL default.
//
// Only Complete looks at the transaction. [DoTx] wraps the whole sequence.
func WithTx(ctx context.Context, tx *sql.Tx) context.Context {
	return idem.Provisional(context.WithValue(ctx, txKey{}, tx))
}

// DoTx runs fn at most once for key, inside a database transaction that also
// records the result. The changes fn makes through tx and the idempotency
// record commit together or not at all.
//
// The engine must have been created with a [Store] from this package; DoTx
// begins the transaction on that store's database. If the operation already
// completed, DoTx does not call fn: it returns the stored result and reports
// replayed as true.
//
// If fn returns an error or panics, the transaction is rolled back and the
// key is released. If the attempt loses its lease before the record is
// written, the transaction is rolled back and DoTx returns an error wrapping
// [idem.ErrLeaseLost]; nothing fn did through tx takes effect, and whoever
// took the key over does the work instead.
//
// The transaction runs at READ COMMITTED, whatever the default of the
// database or session is. The check that the attempt still holds its lease
// has to see the row as it is when the record is written, not as it was when
// the transaction began, and a stricter level would hide that.
//
// fn should use the context it is given, which is cancelled if the lease is
// lost. The heartbeat that keeps the lease alive needs a connection of its
// own, so the pool must allow at least two connections.
//
// Only what goes through tx is covered. A call fn makes to another system is
// not rolled back with the transaction; give that system the key as well.
func DoTx[T any](ctx context.Context, e *idem.Engine, key string, fn func(ctx context.Context, tx *sql.Tx) (T, error), opts ...idem.Option) (v T, replayed bool, err error) {
	store, ok := e.Store().(*Store)
	if !ok {
		return v, false, fmt.Errorf("pgstore: DoTx needs an engine backed by *pgstore.Store, not %T", e.Store())
	}

	attempt, stored, err := e.Begin(ctx, key, opts...)
	if err != nil {
		return v, false, err
	}
	if attempt == nil {
		if err := e.Codec().Unmarshal(stored, &v); err != nil {
			return v, true, fmt.Errorf("pgstore: decode stored result: %w", err)
		}
		return v, true, nil
	}

	var tx *sql.Tx
	committed := false
	defer func() {
		if committed {
			return
		}
		// Reached on every failure and on a panic in fn. The rollback comes
		// first: once it is done, a release cannot expose half-applied work.
		// If the commit failed in a way that left it applied after all, the
		// record is completed and the release changes nothing.
		if tx != nil {
			_ = tx.Rollback()
		}
		_ = attempt.Release(ctx)
	}()

	tx, err = store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return v, false, fmt.Errorf("pgstore: begin transaction: %w", err)
	}
	if v, err = fn(attempt.Context(), tx); err != nil {
		return v, false, err
	}
	data, err := e.Codec().Marshal(v)
	if err != nil {
		var zero T
		return zero, false, fmt.Errorf("pgstore: encode result: %w", err)
	}
	if err := attempt.Complete(WithTx(ctx, tx), data); err != nil {
		var zero T
		return zero, false, err
	}
	if err := tx.Commit(); err != nil {
		var zero T
		return zero, false, fmt.Errorf("pgstore: commit: %w", err)
	}
	committed = true
	return v, false, nil
}

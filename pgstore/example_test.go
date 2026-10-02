package pgstore_test

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/udaykishore-resu/idem"
	"github.com/udaykishore-resu/idem/pgstore"
)

type Order struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

// The order row and the idempotency record are written in one transaction.
// If the process dies before the commit, the order does not exist, and once
// the lease has run out the client's retry creates it. If it dies after the
// commit, the retry gets the stored order back.
func ExampleDoTx() {
	ctx := context.Background()

	// Use the driver your service already has: "pgx" with
	// github.com/jackc/pgx/v5/stdlib, or "postgres" with github.com/lib/pq.
	db, err := sql.Open("pgx", "postgres://localhost:5432/shop")
	if err != nil {
		log.Fatal(err)
	}
	store := pgstore.New(db)
	if err := store.CreateTable(ctx); err != nil {
		log.Fatal(err)
	}
	engine := idem.New(store)

	order, replayed, err := pgstore.DoTx(ctx, engine, "checkout-7f3a",
		func(ctx context.Context, tx *sql.Tx) (Order, error) {
			o := Order{ID: "ord_1001", Total: 4200}
			_, err := tx.ExecContext(ctx,
				`INSERT INTO orders (id, total) VALUES ($1, $2)`, o.ID, o.Total)
			return o, err
		})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("order %s, replayed: %t", order.ID, replayed)
}

// Rows of keys that never come back stay in the table until they are
// deleted. A ticker in the service is enough to keep it small.
func ExampleStore_DeleteExpired() {
	ctx := context.Background()
	db, err := sql.Open("pgx", "postgres://localhost:5432/shop")
	if err != nil {
		log.Fatal(err)
	}
	store := pgstore.New(db)

	for range time.Tick(10 * time.Minute) {
		n, err := store.DeleteExpired(ctx)
		if err != nil {
			log.Printf("idem: deleting expired records: %v", err)
			continue
		}
		log.Printf("idem: deleted %d expired records", n)
	}
}

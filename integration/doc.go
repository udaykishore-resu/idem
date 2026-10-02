// Package integration holds tests that run the stores against real servers
// through the client libraries people actually use: go-redis for Redis, and
// pgx and lib/pq for PostgreSQL.
//
// It is a module of its own so that those libraries stay out of the go.mod of
// github.com/udaykishore-resu/idem. Nothing here is meant to be imported.
//
// Run the tests with
//
//	IDEM_REDIS_ADDR=localhost:6379 \
//	IDEM_POSTGRES_DSN='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable' \
//	go test -race ./...
//
// Tests whose server is not configured are skipped.
package integration

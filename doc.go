// Package idem makes operations safe to retry: it runs an operation at most
// once per idempotency key and replays the stored result to every later caller.
//
// A client that times out cannot tell whether its request was lost or whether
// only the reply was. Retrying is the only way forward, and without protection
// a retry repeats the side effect: a second charge, a second order, a second
// email. With an idempotency key the client names the operation, and the
// server uses the name to recognise a retry.
//
// # Model
//
// Every key moves through three states:
//
//	absent ──Acquire──▶ in progress ──Complete──▶ completed ──ttl──▶ absent
//	                        │
//	                        └──Release, or the lease runs out──▶ absent
//
// The caller that acquires a key holds a lease on it. While the operation
// runs, a heartbeat keeps extending the lease. If the process dies the lease
// runs out and the key can be acquired again, so a crash blocks retries for
// one lease, 30 seconds by default, rather than for the lifetime of a fixed
// lock.
//
// Every acquisition carries a random token, and a store only accepts
// Complete, Release and Extend from the token that currently holds the key.
// A worker that stalled past its lease therefore cannot overwrite the record
// of the worker that took over; it gets [ErrLeaseLost] instead.
//
// # Using it
//
// Create one [Engine] per store and share it:
//
//	engine := idem.New(memstore.New())
//
// [Do] protects any function, whatever delivered the work: an HTTP handler, a
// queue consumer, a scheduled job.
//
//	receipt, replayed, err := idem.Do(ctx, engine, msg.ID,
//		func(ctx context.Context) (Receipt, error) {
//			return charge(ctx, msg)
//		})
//
// For request handlers, package idemhttp wraps an [net/http.Handler] and
// module idemgrpc provides a unary server interceptor. Both are built on
// [Engine.Begin], which is exported for flows that Do does not fit.
//
// # Stores
//
// A [Store] keeps the records. Three ship with this module, and none of them
// adds a dependency to your build:
//
//   - memstore keeps records in process memory, for tests and single
//     instances.
//   - redisstore runs on Redis through whichever client you already use.
//   - pgstore runs on PostgreSQL through database/sql, and can write the
//     record in the same transaction as your own changes.
//
// Package storetest checks any Store against the contract.
//
// # What is and is not guaranteed
//
// While the lease is held, at most one caller runs the operation for a key.
// After it succeeds, every caller with that key receives the stored result
// until the record expires.
//
// A lease is not a transaction. A holder that cannot renew its lease, because
// its process is frozen or because it cannot reach the store, loses the key
// when the lease runs out, and a second caller may then start the same work.
// The first holder's context is cancelled as soon as it can tell, and the
// store refuses its result once another caller holds the key, but whatever it
// already did outside the store has happened. Only a store that commits the
// record together with the side effects closes that gap; see pgstore.DoTx.
// For side effects in other systems, pass the key on to them so that they
// deduplicate too.
//
// Do keeps a result only when the function returns a nil error. A function
// that fails releases its key, and a retry runs it again. The transport
// packages document which responses they keep.
package idem

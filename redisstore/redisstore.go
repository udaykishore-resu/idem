// Package redisstore provides a Redis store for package idem. It works with
// any server that speaks the Redis protocol and runs Lua scripts, such as
// Valkey.
//
// The package does not import a Redis client. It needs exactly one thing from
// yours, the ability to run a script, and asks for it through the one-method
// [Evaler] interface. With go-redis the adapter is:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//
//	store := redisstore.New(redisstore.EvalFunc(
//		func(ctx context.Context, script string, keys []string, args ...any) (any, error) {
//			return rdb.Eval(ctx, script, keys, args...).Result()
//		}))
//
// Each record is one hash, and each store operation is one script that
// touches one key, so the store is atomic and works unchanged on Redis
// Cluster. Leases and result lifetimes are enforced by Redis key expiry on
// the server's clock; the clocks of your application instances do not matter.
package redisstore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/udaykishore-resu/idem"
)

// Evaler runs a Lua script on Redis, as the EVAL command does, and returns
// its reply. Integers in the reply may be int64 or int, bulk strings may be
// string or []byte, and arrays must be []any.
type Evaler interface {
	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)
}

// EvalFunc adapts a function to the [Evaler] interface.
type EvalFunc func(ctx context.Context, script string, keys []string, args ...any) (any, error)

// Eval calls f.
func (f EvalFunc) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	return f(ctx, script, keys, args...)
}

// DefaultPrefix is prepended to every key unless [WithPrefix] changes it.
const DefaultPrefix = "idem:"

// Store is an [idem.Store] that keeps its records in Redis.
type Store struct {
	client Evaler
	prefix string
}

// Option configures a [Store].
type Option func(*Store)

// WithPrefix sets the string prepended to every Redis key. The default is
// [DefaultPrefix].
func WithPrefix(prefix string) Option {
	return func(s *Store) { s.prefix = prefix }
}

// New returns a store that runs its scripts through client. It panics if
// client is nil.
func New(client Evaler, opts ...Option) *Store {
	if client == nil {
		panic("redisstore: client must not be nil")
	}
	s := &Store{client: client, prefix: DefaultPrefix}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

var _ idem.Store = (*Store)(nil)

// A record is a hash with the fields
//
//	s  state: "p" in progress, "c" completed
//	t  token of the attempt that holds or held the key
//	f  fingerprint
//	r  result, once completed
//
// and a time to live equal to the lease or, once completed, the result ttl.
// An expired record is simply gone, which is what the contract asks for.
const (
	// KEYS[1] key. ARGV: token, fingerprint, lease in ms.
	acquireScript = `
local rec = redis.call('HMGET', KEYS[1], 's', 'f', 'r')
if not rec[1] then
  redis.call('HSET', KEYS[1], 's', 'p', 't', ARGV[1], 'f', ARGV[2])
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
  return {1}
end
if rec[1] == 'c' then
  return {3, rec[2], rec[3]}
end
return {2, rec[2]}`

	// KEYS[1] key. ARGV: token, result, ttl in ms.
	completeScript = `
local rec = redis.call('HMGET', KEYS[1], 's', 't')
if rec[2] ~= ARGV[1] then
  return 0
end
if rec[1] == 'p' then
  redis.call('HSET', KEYS[1], 's', 'c', 'r', ARGV[2])
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
end
return 1`

	// KEYS[1] key. ARGV: token.
	releaseScript = `
local rec = redis.call('HMGET', KEYS[1], 's', 't')
if rec[1] ~= 'p' or rec[2] ~= ARGV[1] then
  return 0
end
redis.call('DEL', KEYS[1])
return 1`

	// KEYS[1] key. ARGV: token, lease in ms.
	extendScript = `
local rec = redis.call('HMGET', KEYS[1], 's', 't')
if rec[1] ~= 'p' or rec[2] ~= ARGV[1] then
  return 0
end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1`
)

// Acquire implements [idem.Store].
func (s *Store) Acquire(ctx context.Context, key, token, fingerprint string, lease time.Duration) (idem.Claim, error) {
	reply, err := s.client.Eval(ctx, acquireScript, []string{s.prefix + key}, token, fingerprint, millis(lease))
	if err != nil {
		return idem.Claim{}, fmt.Errorf("redisstore: acquire: %w", err)
	}
	fields, ok := reply.([]any)
	if !ok || len(fields) == 0 {
		return idem.Claim{}, fmt.Errorf("redisstore: acquire: unexpected reply %T", reply)
	}
	state, err := toInt(fields[0])
	if err != nil {
		return idem.Claim{}, fmt.Errorf("redisstore: acquire: %w", err)
	}

	claim := idem.Claim{State: idem.State(state)}
	switch {
	case claim.State == idem.StateAcquired && len(fields) == 1:
	case claim.State == idem.StateInProgress && len(fields) == 2:
		claim.Fingerprint = string(toBytes(fields[1]))
	case claim.State == idem.StateCompleted && len(fields) == 3:
		claim.Fingerprint = string(toBytes(fields[1]))
		claim.Result = toBytes(fields[2])
	default:
		return idem.Claim{}, fmt.Errorf("redisstore: acquire: unexpected reply with state %d and %d fields", state, len(fields))
	}
	return claim, nil
}

// Complete implements [idem.Store].
func (s *Store) Complete(ctx context.Context, key, token string, result []byte, ttl time.Duration) error {
	if result == nil {
		result = []byte{}
	}
	return s.held(ctx, "complete", completeScript, key, token, result, millis(ttl))
}

// Release implements [idem.Store].
func (s *Store) Release(ctx context.Context, key, token string) error {
	return s.held(ctx, "release", releaseScript, key, token)
}

// Extend implements [idem.Store].
func (s *Store) Extend(ctx context.Context, key, token string, lease time.Duration) error {
	return s.held(ctx, "extend", extendScript, key, token, millis(lease))
}

// held runs a script that replies 1 if the token held the key and 0 if not.
func (s *Store) held(ctx context.Context, op, script, key string, args ...any) error {
	reply, err := s.client.Eval(ctx, script, []string{s.prefix + key}, args...)
	if err != nil {
		return fmt.Errorf("redisstore: %s: %w", op, err)
	}
	ok, err := toInt(reply)
	if err != nil {
		return fmt.Errorf("redisstore: %s: %w", op, err)
	}
	if ok != 1 {
		return idem.ErrLeaseLost
	}
	return nil
}

// millis converts d to whole milliseconds for PEXPIRE, which rejects zero.
func millis(d time.Duration) int64 {
	return max(d.Milliseconds(), 1)
}

func toInt(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case string:
		return strconv.ParseInt(n, 10, 64)
	case []byte:
		return strconv.ParseInt(string(n), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected reply %T, want an integer", v)
	}
}

// toBytes returns a bulk string reply as bytes. A missing field (nil) and an
// empty one both come back empty.
func toBytes(v any) []byte {
	switch b := v.(type) {
	case []byte:
		return b
	case string:
		return []byte(b)
	default:
		return nil
	}
}

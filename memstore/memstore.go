// Package memstore provides an in-memory store for package idem.
//
// Records live in the process, so the store only protects against duplicates
// that reach the same instance, and it forgets everything on restart. That
// makes it the right store for tests, examples and single-instance tools, and
// the wrong one for a service that runs more than one replica.
package memstore

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/udaykishore-resu/idem"
)

// sweepInterval is how often expired records are removed. Sweeping happens on
// the calling goroutine during Acquire, so the store needs no background
// goroutine and no Close method.
const sweepInterval = time.Minute

type record struct {
	token       string
	fingerprint string
	completed   bool
	result      []byte
	expires     time.Time
}

// Store is an in-memory [idem.Store]. It is safe for concurrent use.
type Store struct {
	mu        sync.Mutex
	now       func() time.Time
	records   map[string]*record
	lastSweep time.Time
}

// Option configures a [Store].
type Option func(*Store)

// WithClock makes the store read the time from now instead of [time.Now].
// Tests use it to expire leases without sleeping.
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// New returns an empty store.
func New(opts ...Option) *Store {
	s := &Store{now: time.Now, records: make(map[string]*record)}
	for _, opt := range opts {
		opt(s)
	}
	s.lastSweep = s.now()
	return s
}

var _ idem.Store = (*Store)(nil)

// live returns the unexpired record for key, or nil. The caller holds s.mu.
func (s *Store) live(key string, now time.Time) *record {
	r, ok := s.records[key]
	if !ok {
		return nil
	}
	if !now.Before(r.expires) {
		delete(s.records, key)
		return nil
	}
	return r
}

// sweep drops every expired record. The caller holds s.mu.
func (s *Store) sweep(now time.Time) {
	if now.Sub(s.lastSweep) < sweepInterval {
		return
	}
	s.lastSweep = now
	for key, r := range s.records {
		if !now.Before(r.expires) {
			delete(s.records, key)
		}
	}
}

// Acquire implements [idem.Store].
func (s *Store) Acquire(_ context.Context, key, token, fingerprint string, lease time.Duration) (idem.Claim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.sweep(now)

	if r := s.live(key, now); r != nil {
		if r.completed {
			return idem.Claim{
				State:       idem.StateCompleted,
				Fingerprint: r.fingerprint,
				Result:      bytes.Clone(r.result),
			}, nil
		}
		return idem.Claim{State: idem.StateInProgress, Fingerprint: r.fingerprint}, nil
	}
	s.records[key] = &record{token: token, fingerprint: fingerprint, expires: now.Add(lease)}
	return idem.Claim{State: idem.StateAcquired}, nil
}

// Complete implements [idem.Store].
func (s *Store) Complete(_ context.Context, key, token string, result []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()

	r := s.live(key, now)
	if r == nil || r.token != token {
		return idem.ErrLeaseLost
	}
	if r.completed {
		return nil
	}
	r.completed = true
	r.result = bytes.Clone(result)
	r.expires = now.Add(ttl)
	return nil
}

// Release implements [idem.Store].
func (s *Store) Release(_ context.Context, key, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := s.live(key, s.now())
	if r == nil || r.token != token || r.completed {
		return idem.ErrLeaseLost
	}
	delete(s.records, key)
	return nil
}

// Extend implements [idem.Store].
func (s *Store) Extend(_ context.Context, key, token string, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()

	r := s.live(key, now)
	if r == nil || r.token != token || r.completed {
		return idem.ErrLeaseLost
	}
	r.expires = now.Add(lease)
	return nil
}

// Len reports how many records the store holds, including expired records
// that have not been swept yet.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

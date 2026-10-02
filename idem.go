package idem

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Defaults used by [New] unless an [Option] overrides them.
const (
	// DefaultLease is how long a key stays claimed without a heartbeat.
	DefaultLease = 30 * time.Second
	// DefaultTTL is how long a completed result is kept and replayed.
	DefaultTTL = 24 * time.Hour
)

// Engine runs operations at most once per key against a [Store]. An Engine is
// safe for concurrent use; create one per store and share it.
type Engine struct {
	store Store
	cfg   config
}

type config struct {
	lease        time.Duration
	ttl          time.Duration
	heartbeat    time.Duration
	heartbeatSet bool
	wait         time.Duration
	fingerprint  string
	namespace    string
	codec        Codec
}

// Option configures an [Engine]. Options passed to [New] set the engine's
// defaults; unless noted otherwise, the same options can be passed to
// [Engine.Begin] and [Do] to override a default for a single call.
type Option func(*config)

// WithLease sets how long a key stays claimed when its holder stops sending
// heartbeats. It bounds how long a crashed process can block retries.
// The default is [DefaultLease]. WithLease panics if d is not positive.
func WithLease(d time.Duration) Option {
	if d <= 0 {
		panic("idem: lease must be positive")
	}
	return func(c *config) { c.lease = d }
}

// WithTTL sets how long a completed result is kept and replayed. After it
// expires the key can be used for new work. The default is [DefaultTTL].
// WithTTL panics if d is not positive.
func WithTTL(d time.Duration) Option {
	if d <= 0 {
		panic("idem: ttl must be positive")
	}
	return func(c *config) { c.ttl = d }
}

// WithHeartbeat sets how often the lease is extended while an operation runs.
// The default is a third of the lease. A value of zero or less turns the
// heartbeat off; the operation must then finish within one lease. An interval
// that is not shorter than the lease could never keep it alive and is
// replaced by the default.
func WithHeartbeat(d time.Duration) Option {
	return func(c *config) { c.heartbeat, c.heartbeatSet = d, true }
}

// WithWait makes a call wait up to d for an operation that is in progress
// under the same key, instead of failing with [ErrInProgress] at once. If the
// other operation completes in time, its result is replayed; if it fails and
// releases the key, this call runs the operation itself. The default is not
// to wait.
func WithWait(d time.Duration) Option {
	return func(c *config) { c.wait = d }
}

// WithFingerprint attaches a digest of the request to the key. A later call
// that presents the same key with a different fingerprint fails with
// [ErrFingerprintMismatch] instead of receiving the result of other work.
// An empty fingerprint is never compared. See [Fingerprint].
func WithFingerprint(fp string) Option {
	return func(c *config) { c.fingerprint = fp }
}

// WithNamespace prefixes every key with ns and a colon, so that several
// engines can share one store. It only has an effect when passed to [New].
func WithNamespace(ns string) Option {
	return func(c *config) { c.namespace = ns }
}

// WithCodec sets how [Do] encodes results for storage. The default is JSON.
// It only has an effect when passed to [New].
func WithCodec(codec Codec) Option {
	if codec == nil {
		panic("idem: codec must not be nil")
	}
	return func(c *config) { c.codec = codec }
}

// New returns an Engine that keeps its records in store. It panics if store
// is nil.
func New(store Store, opts ...Option) *Engine {
	if store == nil {
		panic("idem: store must not be nil")
	}
	cfg := config{lease: DefaultLease, ttl: DefaultTTL, codec: jsonCodec{}}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Engine{store: store, cfg: cfg}
}

// Store returns the store the engine was created with.
func (e *Engine) Store() Store { return e.store }

// Codec returns the codec [Do] uses to encode results.
func (e *Engine) Codec() Codec { return e.cfg.codec }

// resolve applies per-call options on top of the engine's defaults.
func (e *Engine) resolve(opts []Option) config {
	cfg := e.cfg
	for _, opt := range opts {
		opt(&cfg)
	}
	// These two belong to the engine, not to a call.
	cfg.namespace, cfg.codec = e.cfg.namespace, e.cfg.codec
	if !cfg.heartbeatSet || cfg.heartbeat >= cfg.lease {
		cfg.heartbeat = cfg.lease / 3
	}
	return cfg
}

// Polling bounds used while waiting for an in-progress operation.
const (
	minPoll = 20 * time.Millisecond
	maxPoll = time.Second
)

// Begin claims key. It is the building block under [Do] and the transport
// packages; use it directly when the result is not a single Go value.
//
// Exactly one of three things happens:
//
//   - The key was free. Begin returns an [Attempt]; the caller runs the
//     operation and then calls [Attempt.Complete] or [Attempt.Release].
//   - The operation already completed. Begin returns a nil Attempt and the
//     stored result.
//   - Begin returns an error: [ErrInProgress] if another caller holds the
//     key, [ErrFingerprintMismatch] if the key belongs to a different
//     request, [ErrInvalidKey] if key is empty, or the store's error.
func (e *Engine) Begin(ctx context.Context, key string, opts ...Option) (*Attempt, []byte, error) {
	if key == "" {
		return nil, nil, ErrInvalidKey
	}
	cfg := e.resolve(opts)
	if cfg.namespace != "" {
		key = cfg.namespace + ":" + key
	}
	token := newToken()

	var deadline time.Time
	if cfg.wait > 0 {
		deadline = time.Now().Add(cfg.wait)
	}
	poll := minPoll

	for {
		// The lease is counted from before the call, so that the local view
		// of when it ends is never later than the store's.
		sent := time.Now()
		claim, err := e.store.Acquire(ctx, key, token, cfg.fingerprint, cfg.lease)
		if err != nil {
			return nil, nil, fmt.Errorf("idem: acquire: %w", err)
		}
		if claim.State == StateAcquired {
			return e.newAttempt(ctx, key, token, cfg, sent), nil, nil
		}
		if cfg.fingerprint != "" && claim.Fingerprint != "" && cfg.fingerprint != claim.Fingerprint {
			return nil, nil, ErrFingerprintMismatch
		}
		switch claim.State {
		case StateCompleted:
			return nil, claim.Result, nil
		case StateInProgress:
			remaining := time.Until(deadline)
			if cfg.wait <= 0 || remaining <= 0 {
				return nil, nil, ErrInProgress
			}
			if err := sleep(ctx, min(poll, remaining)); err != nil {
				return nil, nil, err
			}
			poll = min(poll*2, maxPoll)
		default:
			return nil, nil, fmt.Errorf("idem: store returned unknown state %d", claim.State)
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func newToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("idem: reading random bytes: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// Attempt is one caller's hold on a key. The holder runs the operation and
// then calls [Attempt.Complete] or [Attempt.Release]; until it does, a
// background goroutine keeps the lease alive, so an Attempt must always be
// ended. The simplest way is to defer Release, which does nothing once
// Complete has succeeded:
//
//	attempt, stored, err := engine.Begin(ctx, key)
//	...
//	defer attempt.Release(ctx)
//	...
//	return attempt.Complete(ctx, result)
type Attempt struct {
	store  Store
	key    string
	token  string
	lease  time.Duration
	ttl    time.Duration
	ctx    context.Context
	cancel context.CancelCauseFunc

	stopWatch context.CancelFunc // aborts an Extend that is in flight
	stop      chan struct{}      // closed to stop the watch goroutine
	watchDone chan struct{}      // closed when the watch goroutine has exited
	once      sync.Once

	mu    sync.Mutex
	state attemptState
}

type attemptState int

const (
	attemptActive attemptState = iota
	attemptCompleted
	attemptReleased
)

func (e *Engine) newAttempt(ctx context.Context, key, token string, cfg config, held time.Time) *Attempt {
	a := &Attempt{
		store:     e.store,
		key:       key,
		token:     token,
		lease:     cfg.lease,
		ttl:       cfg.ttl,
		stop:      make(chan struct{}),
		watchDone: make(chan struct{}),
	}
	a.ctx, a.cancel = context.WithCancelCause(ctx)
	watchCtx, stopWatch := context.WithCancel(context.WithoutCancel(ctx))
	a.stopWatch = stopWatch
	go a.watch(watchCtx, cfg.heartbeat, held)
	return a
}

// watch keeps the lease alive and notices when it is gone. It extends the
// lease every interval, if there is one, and cancels the attempt's context
// with ErrLeaseLost when the store says the key is no longer held or when no
// extension has succeeded for a whole lease, which means the store has let
// the lease run out even though nobody told us. held is when the lease was
// last known to start.
func (a *Attempt) watch(ctx context.Context, interval time.Duration, held time.Time) {
	defer close(a.watchDone)

	var tick <-chan time.Time
	if interval > 0 {
		t := time.NewTicker(interval)
		defer t.Stop()
		tick = t.C
	}
	expiry := time.NewTimer(time.Until(held.Add(a.lease)))
	defer expiry.Stop()

	for {
		select {
		case <-a.stop:
			return
		case <-expiry.C:
			a.cancel(ErrLeaseLost)
			return
		case <-tick:
			sent := time.Now()
			callCtx, cancel := context.WithTimeout(ctx, interval)
			err := a.store.Extend(callCtx, a.key, a.token, a.lease)
			cancel()
			switch {
			case err == nil:
				if !expiry.Stop() {
					select {
					case <-expiry.C:
					default:
					}
				}
				expiry.Reset(time.Until(sent.Add(a.lease)))
			case errors.Is(err, ErrLeaseLost):
				a.cancel(ErrLeaseLost)
				return
			}
			// Any other error is a store that could not be reached. The next
			// tick tries again, and the expiry timer bounds how long for.
		}
	}
}

// halt stops the watch goroutine and waits for it, so that no Extend can race
// with the Complete or Release that follows.
func (a *Attempt) halt() {
	a.once.Do(func() {
		close(a.stop)
		a.stopWatch()
	})
	<-a.watchDone
}

// Context returns a context derived from the one passed to [Engine.Begin].
// It is cancelled, with [ErrLeaseLost] as its cause, as soon as the lease is
// known to be gone: the store reports that the key is no longer held, or no
// extension has succeeded for a whole lease. Run the operation with this
// context so that it stops when it has lost the right to finish. The context
// is also cancelled when the attempt ends.
func (a *Attempt) Context() context.Context { return a.ctx }

// Key returns the key as stored, including the engine's namespace.
func (a *Attempt) Key() string { return a.key }

// Token returns the token that identifies this attempt to the store.
func (a *Attempt) Token() string { return a.token }

// Complete stores result as the outcome of the operation and ends the
// attempt. Later calls to [Engine.Begin] with the same key return result.
//
// Complete returns [ErrLeaseLost] if the attempt no longer holds the key; the
// result is then not stored. If Complete fails, the attempt has not ended and
// a deferred [Attempt.Release] still gives the key up. Complete stops the
// heartbeat in every case.
//
// The values of ctx are passed to the store but its cancellation is not: a
// client that has gone away must not prevent the outcome from being recorded.
// The call is bounded by the lease instead, and by five seconds at most.
func (a *Attempt) Complete(ctx context.Context, result []byte) error {
	a.halt()
	defer a.cancel(nil)
	a.mu.Lock()
	defer a.mu.Unlock()
	switch a.state {
	case attemptCompleted:
		return nil
	case attemptReleased:
		return fmt.Errorf("idem: complete: %w", ErrLeaseLost)
	}
	ctx, cancel := a.detach(ctx)
	defer cancel()
	if err := a.store.Complete(ctx, a.key, a.token, result, a.ttl); err != nil {
		return fmt.Errorf("idem: complete: %w", err)
	}
	if ctx.Value(provisionalKey{}) == nil {
		a.state = attemptCompleted
	}
	return nil
}

// Release gives the key up without storing a result and ends the attempt, so
// that a retry can run the operation again straight away. Call it when the
// operation failed. Like [Attempt.Complete], it ignores the cancellation of
// ctx.
//
// Release does nothing and returns nil if the attempt has already ended, so
// it is safe to defer. Releasing a key that is no longer held returns
// [ErrLeaseLost], which callers can usually ignore.
func (a *Attempt) Release(ctx context.Context) error {
	a.halt()
	defer a.cancel(nil)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state != attemptActive {
		return nil
	}
	a.state = attemptReleased
	ctx, cancel := a.detach(ctx)
	defer cancel()
	if err := a.store.Release(ctx, a.key, a.token); err != nil {
		return fmt.Errorf("idem: release: %w", err)
	}
	return nil
}

type provisionalKey struct{}

// Provisional returns a copy of ctx that marks a call to [Attempt.Complete]
// as not final yet: the store will only make the record permanent later, or
// not at all, as when the write is part of a database transaction that has
// still to commit. Stores that can do this wrap the context themselves; see
// pgstore.WithTx.
//
// After a provisional Complete the attempt stays open. A later
// [Attempt.Release] still reaches the store, which frees the key if the
// record did not become permanent and does nothing if it did.
func Provisional(ctx context.Context) context.Context {
	return context.WithValue(ctx, provisionalKey{}, true)
}

// maxStoreCall bounds the store calls that end an attempt.
const maxStoreCall = 5 * time.Second

func (a *Attempt) detach(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), min(a.lease, maxStoreCall))
}

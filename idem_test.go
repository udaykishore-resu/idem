package idem_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/udaykishore-resu/idem"
	"github.com/udaykishore-resu/idem/memstore"
)

type receipt struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

func newEngine(opts ...idem.Option) *idem.Engine {
	return idem.New(memstore.New(), opts...)
}

func TestDoRunsOnceAndReplays(t *testing.T) {
	ctx := context.Background()
	engine := newEngine()
	var calls int
	charge := func(context.Context) (receipt, error) {
		calls++
		return receipt{ID: "r-1", Amount: 500}, nil
	}

	first, replayed, err := idem.Do(ctx, engine, "key", charge)
	if err != nil || replayed {
		t.Fatalf("first call: replayed = %v, err = %v", replayed, err)
	}
	second, replayed, err := idem.Do(ctx, engine, "key", charge)
	if err != nil || !replayed {
		t.Fatalf("second call: replayed = %v, err = %v, want a replay", replayed, err)
	}
	if calls != 1 {
		t.Errorf("fn ran %d times, want 1", calls)
	}
	if first != second {
		t.Errorf("replayed %+v, want %+v", second, first)
	}
}

func TestDoDifferentKeysRunSeparately(t *testing.T) {
	ctx := context.Background()
	engine := newEngine()
	var calls int
	fn := func(context.Context) (int, error) { calls++; return calls, nil }

	a, _, _ := idem.Do(ctx, engine, "a", fn)
	b, _, _ := idem.Do(ctx, engine, "b", fn)
	if a != 1 || b != 2 {
		t.Errorf("results = %d, %d, want 1, 2", a, b)
	}
}

func TestDoErrorReleasesKey(t *testing.T) {
	ctx := context.Background()
	engine := newEngine()
	boom := errors.New("boom")
	var calls int

	_, _, err := idem.Do(ctx, engine, "key", func(context.Context) (int, error) {
		calls++
		return 0, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the error from fn", err)
	}

	v, replayed, err := idem.Do(ctx, engine, "key", func(context.Context) (int, error) {
		calls++
		return 42, nil
	})
	if err != nil || replayed || v != 42 {
		t.Fatalf("retry after failure: v = %d, replayed = %v, err = %v", v, replayed, err)
	}
	if calls != 2 {
		t.Errorf("fn ran %d times, want 2: a failed run must not be replayed", calls)
	}
}

func TestDoPanicReleasesKey(t *testing.T) {
	ctx := context.Background()
	engine := newEngine()

	func() {
		defer func() {
			if r := recover(); r != "kaboom" {
				t.Fatalf("recovered %v, want the original panic value", r)
			}
		}()
		_, _, _ = idem.Do(ctx, engine, "key", func(context.Context) (int, error) { panic("kaboom") })
	}()

	v, replayed, err := idem.Do(ctx, engine, "key", func(context.Context) (int, error) { return 7, nil })
	if err != nil || replayed || v != 7 {
		t.Fatalf("retry after panic: v = %d, replayed = %v, err = %v", v, replayed, err)
	}
}

func TestDoConcurrentCallersRunOnce(t *testing.T) {
	ctx := context.Background()
	engine := newEngine()
	const callers = 50

	var runs, inProgress, replays atomic.Int32
	release := make(chan struct{})
	started := make(chan struct{})
	var startedOnce sync.Once

	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, replayed, err := idem.Do(ctx, engine, "key", func(context.Context) (string, error) {
				runs.Add(1)
				startedOnce.Do(func() { close(started) })
				<-release
				return "done", nil
			})
			switch {
			case errors.Is(err, idem.ErrInProgress):
				inProgress.Add(1)
			case err != nil:
				t.Errorf("unexpected error: %v", err)
			case replayed:
				replays.Add(1)
			}
		}()
	}
	<-started
	// Give the others time to hit the held key before the holder finishes.
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := runs.Load(); got != 1 {
		t.Errorf("fn ran %d times, want 1", got)
	}
	if got := inProgress.Load() + replays.Load(); got != callers-1 {
		t.Errorf("%d callers were turned away or replayed, want %d", got, callers-1)
	}
}

func TestWaitReplaysResultOfRunningCall(t *testing.T) {
	ctx := context.Background()
	engine := newEngine()
	started := make(chan struct{})
	finish := make(chan struct{})

	go func() {
		_, _, _ = idem.Do(ctx, engine, "key", func(context.Context) (string, error) {
			close(started)
			<-finish
			return "from the first caller", nil
		})
	}()
	<-started
	time.AfterFunc(80*time.Millisecond, func() { close(finish) })

	v, replayed, err := idem.Do(ctx, engine, "key", func(context.Context) (string, error) {
		t.Error("the waiting caller ran fn")
		return "", nil
	}, idem.WithWait(5*time.Second))
	if err != nil || !replayed || v != "from the first caller" {
		t.Fatalf("v = %q, replayed = %v, err = %v", v, replayed, err)
	}
}

func TestWaitRunsWhenRunningCallFails(t *testing.T) {
	ctx := context.Background()
	engine := newEngine()
	started := make(chan struct{})
	fail := make(chan struct{})

	go func() {
		_, _, _ = idem.Do(ctx, engine, "key", func(context.Context) (string, error) {
			close(started)
			<-fail
			return "", errors.New("first caller failed")
		})
	}()
	<-started
	time.AfterFunc(80*time.Millisecond, func() { close(fail) })

	v, replayed, err := idem.Do(ctx, engine, "key", func(context.Context) (string, error) {
		return "from the second caller", nil
	}, idem.WithWait(5*time.Second))
	if err != nil || replayed || v != "from the second caller" {
		t.Fatalf("v = %q, replayed = %v, err = %v", v, replayed, err)
	}
}

func TestWaitGivesUp(t *testing.T) {
	ctx := context.Background()
	engine := newEngine()
	started := make(chan struct{})
	finish := make(chan struct{})
	defer close(finish)

	go func() {
		_, _, _ = idem.Do(ctx, engine, "key", func(context.Context) (int, error) {
			close(started)
			<-finish
			return 0, nil
		})
	}()
	<-started

	begin := time.Now()
	_, _, err := idem.Do(ctx, engine, "key", func(context.Context) (int, error) { return 0, nil },
		idem.WithWait(150*time.Millisecond))
	if !errors.Is(err, idem.ErrInProgress) {
		t.Fatalf("err = %v, want ErrInProgress", err)
	}
	if waited := time.Since(begin); waited < 150*time.Millisecond || waited > 2*time.Second {
		t.Errorf("waited %v, want about 150ms", waited)
	}
}

func TestWaitStopsWhenContextEnds(t *testing.T) {
	engine := newEngine()
	attempt, _, err := engine.Begin(context.Background(), "key")
	if err != nil {
		t.Fatal(err)
	}
	defer attempt.Release(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_, _, err = engine.Begin(ctx, "key", idem.WithWait(time.Minute))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestFingerprintMismatch(t *testing.T) {
	ctx := context.Background()
	engine := newEngine()
	fn := func(context.Context) (int, error) { return 1, nil }
	fpA := idem.WithFingerprint(idem.Fingerprint([]byte("POST /charges"), []byte(`{"amount":500}`)))
	fpB := idem.WithFingerprint(idem.Fingerprint([]byte("POST /charges"), []byte(`{"amount":900}`)))

	if _, _, err := idem.Do(ctx, engine, "key", fn, fpA); err != nil {
		t.Fatal(err)
	}
	if _, replayed, err := idem.Do(ctx, engine, "key", fn, fpA); err != nil || !replayed {
		t.Fatalf("same fingerprint: replayed = %v, err = %v", replayed, err)
	}
	if _, _, err := idem.Do(ctx, engine, "key", fn, fpB); !errors.Is(err, idem.ErrFingerprintMismatch) {
		t.Fatalf("different fingerprint: err = %v, want ErrFingerprintMismatch", err)
	}
	if _, replayed, err := idem.Do(ctx, engine, "key", fn); err != nil || !replayed {
		t.Fatalf("no fingerprint: replayed = %v, err = %v, want a replay without comparison", replayed, err)
	}
}

func TestFingerprintMismatchWhileInProgress(t *testing.T) {
	ctx := context.Background()
	engine := newEngine()
	attempt, _, err := engine.Begin(ctx, "key", idem.WithFingerprint("a"))
	if err != nil {
		t.Fatal(err)
	}
	defer attempt.Release(ctx)

	// A mismatch is a client bug; waiting for the other request cannot fix it.
	_, _, err = engine.Begin(ctx, "key", idem.WithFingerprint("b"), idem.WithWait(time.Minute))
	if !errors.Is(err, idem.ErrFingerprintMismatch) {
		t.Fatalf("err = %v, want ErrFingerprintMismatch", err)
	}
}

func TestFingerprintSeparatesParts(t *testing.T) {
	if idem.Fingerprint([]byte("ab"), []byte("c")) == idem.Fingerprint([]byte("a"), []byte("bc")) {
		t.Error("part boundaries do not affect the fingerprint")
	}
	if idem.Fingerprint([]byte("x")) != idem.Fingerprint([]byte("x")) {
		t.Error("fingerprint is not deterministic")
	}
	if got := len(idem.Fingerprint()); got != 64 {
		t.Errorf("fingerprint has %d characters, want 64 hex digits", got)
	}
}

func TestEmptyKey(t *testing.T) {
	_, _, err := idem.Do(context.Background(), newEngine(), "", func(context.Context) (int, error) { return 0, nil })
	if !errors.Is(err, idem.ErrInvalidKey) {
		t.Fatalf("err = %v, want ErrInvalidKey", err)
	}
}

func TestNamespaceSeparatesEngines(t *testing.T) {
	ctx := context.Background()
	store := memstore.New()
	billing := idem.New(store, idem.WithNamespace("billing"))
	mail := idem.New(store, idem.WithNamespace("mail"))

	if _, _, err := idem.Do(ctx, billing, "42", func(context.Context) (string, error) { return "charged", nil }); err != nil {
		t.Fatal(err)
	}
	v, replayed, err := idem.Do(ctx, mail, "42", func(context.Context) (string, error) { return "sent", nil })
	if err != nil || replayed || v != "sent" {
		t.Fatalf("v = %q, replayed = %v, err = %v: namespaces share a key", v, replayed, err)
	}

	attempt, _, err := billing.Begin(ctx, "43")
	if err != nil {
		t.Fatal(err)
	}
	defer attempt.Release(ctx)
	if got := attempt.Key(); got != "billing:43" {
		t.Errorf("stored key = %q, want %q", got, "billing:43")
	}
}

func TestHeartbeatKeepsLeaseAlive(t *testing.T) {
	ctx := context.Background()
	engine := newEngine(idem.WithLease(120 * time.Millisecond))
	started := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		_, _, err := idem.Do(ctx, engine, "key", func(ctx context.Context) (int, error) {
			close(started)
			select {
			case <-time.After(600 * time.Millisecond): // five leases long
				return 1, nil
			case <-ctx.Done():
				return 0, context.Cause(ctx)
			}
		})
		done <- err
	}()
	<-started
	time.Sleep(400 * time.Millisecond)

	_, _, err := idem.Do(ctx, engine, "key", func(context.Context) (int, error) {
		t.Error("a second caller took over a lease that was being kept alive")
		return 0, nil
	})
	if !errors.Is(err, idem.ErrInProgress) {
		t.Fatalf("second caller: err = %v, want ErrInProgress", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("first caller: %v", err)
	}
}

func TestWithoutHeartbeatLeaseRunsOut(t *testing.T) {
	ctx := context.Background()
	engine := newEngine(idem.WithLease(100*time.Millisecond), idem.WithHeartbeat(0))
	started := make(chan struct{})
	finish := make(chan struct{})
	type outcome struct {
		v   string
		err error
	}
	slow := make(chan outcome, 1)

	go func() {
		v, _, err := idem.Do(ctx, engine, "key", func(context.Context) (string, error) {
			close(started)
			<-finish
			return "slow", nil
		})
		slow <- outcome{v, err}
	}()
	<-started
	time.Sleep(250 * time.Millisecond) // the lease is gone and nothing renewed it

	v, replayed, err := idem.Do(ctx, engine, "key", func(context.Context) (string, error) { return "fast", nil })
	if err != nil || replayed || v != "fast" {
		t.Fatalf("takeover: v = %q, replayed = %v, err = %v", v, replayed, err)
	}

	close(finish)
	got := <-slow
	if !errors.Is(got.err, idem.ErrNotRecorded) || !errors.Is(got.err, idem.ErrLeaseLost) {
		t.Fatalf("stale caller: err = %v, want ErrNotRecorded wrapping ErrLeaseLost", got.err)
	}
	if got.v != "slow" {
		t.Errorf("stale caller: v = %q, want its own result alongside the error", got.v)
	}

	v, replayed, err = idem.Do(ctx, engine, "key", func(context.Context) (string, error) { return "third", nil })
	if err != nil || !replayed || v != "fast" {
		t.Fatalf("after both: v = %q, replayed = %v, err = %v: the stale caller overwrote the record", v, replayed, err)
	}
}

// faultyStore wraps a store and lets a test inject failures.
type faultyStore struct {
	idem.Store
	acquireErr  error
	completeErr error
	extendErr   atomic.Pointer[error]
	extends     atomic.Int32
}

func (s *faultyStore) Acquire(ctx context.Context, key, token, fp string, lease time.Duration) (idem.Claim, error) {
	if s.acquireErr != nil {
		return idem.Claim{}, s.acquireErr
	}
	return s.Store.Acquire(ctx, key, token, fp, lease)
}

func (s *faultyStore) Complete(ctx context.Context, key, token string, result []byte, ttl time.Duration) error {
	if s.completeErr != nil {
		return s.completeErr
	}
	return s.Store.Complete(ctx, key, token, result, ttl)
}

func (s *faultyStore) Extend(ctx context.Context, key, token string, lease time.Duration) error {
	s.extends.Add(1)
	if err := s.extendErr.Load(); err != nil {
		return *err
	}
	return s.Store.Extend(ctx, key, token, lease)
}

func TestLostLeaseCancelsContext(t *testing.T) {
	store := &faultyStore{Store: memstore.New()}
	store.extendErr.Store(&idem.ErrLeaseLost)
	engine := idem.New(store, idem.WithLease(60*time.Millisecond))

	_, _, err := idem.Do(context.Background(), engine, "key", func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, context.Cause(ctx)
		case <-time.After(5 * time.Second):
			return 0, errors.New("context was not cancelled")
		}
	})
	if !errors.Is(err, idem.ErrLeaseLost) {
		t.Fatalf("err = %v, want the cancellation cause ErrLeaseLost", err)
	}
}

func TestHeartbeatSurvivesStoreOutage(t *testing.T) {
	store := &faultyStore{Store: memstore.New()}
	outage := errors.New("connection refused")
	store.extendErr.Store(&outage)
	engine := idem.New(store, idem.WithLease(time.Minute), idem.WithHeartbeat(20*time.Millisecond))

	v, _, err := idem.Do(context.Background(), engine, "key", func(ctx context.Context) (int, error) {
		time.Sleep(150 * time.Millisecond)
		return 9, ctx.Err()
	})
	if err != nil || v != 9 {
		t.Fatalf("v = %d, err = %v: a transient heartbeat failure must not abort the operation", v, err)
	}
	if got := store.extends.Load(); got < 2 {
		t.Errorf("heartbeat ran %d times, want it to keep retrying", got)
	}
}

func TestHeartbeatStopsWhenAttemptEnds(t *testing.T) {
	store := &faultyStore{Store: memstore.New()}
	engine := idem.New(store, idem.WithLease(time.Minute), idem.WithHeartbeat(10*time.Millisecond))

	if _, _, err := idem.Do(context.Background(), engine, "key", func(context.Context) (int, error) {
		time.Sleep(60 * time.Millisecond)
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	after := store.extends.Load()
	time.Sleep(80 * time.Millisecond)
	if got := store.extends.Load(); got != after {
		t.Errorf("heartbeat sent %d more extensions after the attempt ended", got-after)
	}
}

func TestResultIsRecordedAfterCallerGoesAway(t *testing.T) {
	engine := newEngine()
	ctx, cancel := context.WithCancel(context.Background())

	// The client disconnects while the operation is finishing. The work is
	// done, so it has to be recorded regardless.
	_, _, err := idem.Do(ctx, engine, "key", func(context.Context) (string, error) {
		cancel()
		return "charged", nil
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}

	v, replayed, err := idem.Do(context.Background(), engine, "key", func(context.Context) (string, error) {
		return "charged twice", nil
	})
	if err != nil || !replayed || v != "charged" {
		t.Fatalf("v = %q, replayed = %v, err = %v", v, replayed, err)
	}
}

func TestKeyIsReleasedAfterCallerGoesAway(t *testing.T) {
	engine := newEngine()
	ctx, cancel := context.WithCancel(context.Background())

	_, _, err := idem.Do(ctx, engine, "key", func(ctx context.Context) (int, error) {
		cancel()
		<-ctx.Done()
		return 0, ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	_, replayed, err := idem.Do(context.Background(), engine, "key", func(context.Context) (int, error) { return 1, nil })
	if err != nil || replayed {
		t.Fatalf("retry: replayed = %v, err = %v, want the key to be free at once", replayed, err)
	}
}

func TestStoreErrors(t *testing.T) {
	ctx := context.Background()
	down := errors.New("store is down")

	t.Run("Acquire", func(t *testing.T) {
		engine := idem.New(&faultyStore{Store: memstore.New(), acquireErr: down})
		_, _, err := idem.Do(ctx, engine, "key", func(context.Context) (int, error) {
			t.Error("fn ran although the key could not be acquired")
			return 0, nil
		})
		if !errors.Is(err, down) {
			t.Fatalf("err = %v, want the store's error", err)
		}
	})

	t.Run("Complete", func(t *testing.T) {
		engine := idem.New(&faultyStore{Store: memstore.New(), completeErr: down})
		v, _, err := idem.Do(ctx, engine, "key", func(context.Context) (int, error) { return 5, nil })
		if !errors.Is(err, idem.ErrNotRecorded) || !errors.Is(err, down) {
			t.Fatalf("err = %v, want ErrNotRecorded wrapping the store's error", err)
		}
		if v != 5 {
			t.Errorf("v = %d, want the result of the operation that did run", v)
		}
	})
}

type upperCodec struct{}

func (upperCodec) Marshal(v any) ([]byte, error) { return []byte(strings.ToUpper(v.(string))), nil }
func (upperCodec) Unmarshal(data []byte, v any) error {
	*(v.(*string)) = string(data)
	return nil
}

func TestCustomCodec(t *testing.T) {
	ctx := context.Background()
	engine := newEngine(idem.WithCodec(upperCodec{}))
	fn := func(context.Context) (string, error) { return "hello", nil }

	if _, _, err := idem.Do(ctx, engine, "key", fn); err != nil {
		t.Fatal(err)
	}
	v, replayed, err := idem.Do(ctx, engine, "key", fn)
	if err != nil || !replayed || v != "HELLO" {
		t.Fatalf("v = %q, replayed = %v, err = %v, want the codec's encoding", v, replayed, err)
	}
}

func TestUnencodableResult(t *testing.T) {
	ctx := context.Background()
	engine := newEngine()
	fn := func(context.Context) (chan int, error) { return make(chan int), nil }

	v, _, err := idem.Do(ctx, engine, "key", fn)
	if !errors.Is(err, idem.ErrNotRecorded) || v == nil {
		t.Fatalf("v = %v, err = %v, want the value and ErrNotRecorded", v, err)
	}
	if _, replayed, _ := idem.Do(ctx, engine, "key", fn); replayed {
		t.Error("a result that could not be encoded was replayed")
	}
}

func TestBeginAndAttempt(t *testing.T) {
	ctx := context.Background()
	engine := newEngine()

	attempt, stored, err := engine.Begin(ctx, "key")
	if err != nil || attempt == nil || stored != nil {
		t.Fatalf("Begin on a free key: attempt = %v, stored = %q, err = %v", attempt, stored, err)
	}
	if attempt.Token() == "" || attempt.Key() != "key" {
		t.Errorf("attempt has key %q and token %q", attempt.Key(), attempt.Token())
	}
	if _, _, err := engine.Begin(ctx, "key"); !errors.Is(err, idem.ErrInProgress) {
		t.Fatalf("Begin on a held key: err = %v, want ErrInProgress", err)
	}
	if err := attempt.Complete(ctx, []byte("payload")); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if attempt.Context().Err() == nil {
		t.Error("the attempt's context is still live after Complete")
	}

	attempt, stored, err = engine.Begin(ctx, "key")
	if err != nil || attempt != nil || string(stored) != "payload" {
		t.Fatalf("Begin on a completed key: attempt = %v, stored = %q, err = %v", attempt, stored, err)
	}
}

func TestPerCallOptionsOverrideDefaults(t *testing.T) {
	ctx := context.Background()
	engine := newEngine()

	if _, _, err := idem.Do(ctx, engine, "key", func(context.Context) (int, error) { return 1, nil },
		idem.WithTTL(80*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(160 * time.Millisecond)
	_, replayed, err := idem.Do(ctx, engine, "key", func(context.Context) (int, error) { return 2, nil })
	if err != nil || replayed {
		t.Fatalf("replayed = %v, err = %v, want the short per-call ttl to have expired", replayed, err)
	}
}

func TestInvalidOptionsPanic(t *testing.T) {
	for name, fn := range map[string]func(){
		"nil store":      func() { idem.New(nil) },
		"zero lease":     func() { idem.WithLease(0) },
		"negative ttl":   func() { idem.WithTTL(-time.Second) },
		"nil codec":      func() { idem.WithCodec(nil) },
		"negative lease": func() { idem.WithLease(-1) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("no panic")
				}
			}()
			fn()
		})
	}
}

func TestStoreOutageLongerThanLeaseCancelsContext(t *testing.T) {
	// The store stops answering heartbeats. Nothing says "lease lost", but
	// after a full lease without a successful extension the store has let the
	// lease run out, and the holder must stop believing it holds the key.
	store := &faultyStore{Store: memstore.New()}
	outage := errors.New("i/o timeout")
	store.extendErr.Store(&outage)
	engine := idem.New(store, idem.WithLease(120*time.Millisecond))

	begin := time.Now()
	_, _, err := idem.Do(context.Background(), engine, "key", func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, context.Cause(ctx)
		case <-time.After(5 * time.Second):
			return 0, errors.New("context was not cancelled")
		}
	})
	if !errors.Is(err, idem.ErrLeaseLost) {
		t.Fatalf("err = %v, want ErrLeaseLost", err)
	}
	if took := time.Since(begin); took < 100*time.Millisecond || took > 2*time.Second {
		t.Errorf("cancelled after %v, want about one lease", took)
	}
}

func TestWithoutHeartbeatContextEndsWithLease(t *testing.T) {
	engine := newEngine(idem.WithLease(80*time.Millisecond), idem.WithHeartbeat(0))
	_, _, err := idem.Do(context.Background(), engine, "key", func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, context.Cause(ctx)
		case <-time.After(5 * time.Second):
			return 0, errors.New("context was not cancelled")
		}
	})
	if !errors.Is(err, idem.ErrLeaseLost) {
		t.Fatalf("err = %v, want ErrLeaseLost once the lease has run out", err)
	}
}

func TestShortPerCallLeaseIsStillKeptAlive(t *testing.T) {
	ctx := context.Background()
	// The engine's heartbeat is longer than the lease this call asks for.
	engine := newEngine(idem.WithLease(3*time.Second), idem.WithHeartbeat(time.Second))
	started := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		_, _, err := idem.Do(ctx, engine, "key", func(ctx context.Context) (int, error) {
			close(started)
			select {
			case <-time.After(500 * time.Millisecond):
				return 1, nil
			case <-ctx.Done():
				return 0, context.Cause(ctx)
			}
		}, idem.WithLease(100*time.Millisecond))
		done <- err
	}()
	<-started
	time.Sleep(300 * time.Millisecond)

	_, _, err := idem.Do(ctx, engine, "key", func(context.Context) (int, error) {
		t.Error("a second caller ran the operation")
		return 0, nil
	})
	if !errors.Is(err, idem.ErrInProgress) {
		t.Errorf("second caller: err = %v, want ErrInProgress", err)
	}
	if err := <-done; err != nil {
		t.Errorf("first caller: %v", err)
	}
}

type panicCodec struct{}

func (panicCodec) Marshal(any) ([]byte, error) { panic("codec bug") }
func (panicCodec) Unmarshal([]byte, any) error { return nil }

func TestPanicAfterOperationReleasesKey(t *testing.T) {
	ctx := context.Background()
	store := &faultyStore{Store: memstore.New()}
	engine := idem.New(store, idem.WithCodec(panicCodec{}), idem.WithHeartbeat(10*time.Millisecond))

	func() {
		defer func() {
			if recover() == nil {
				t.Error("the codec's panic did not propagate")
			}
		}()
		_, _, _ = idem.Do(ctx, engine, "key", func(context.Context) (int, error) { return 1, nil })
	}()

	attempt, _, err := engine.Begin(ctx, "key")
	if err != nil {
		t.Fatalf("Begin after the panic: err = %v, want the key to be free", err)
	}
	_ = attempt.Release(ctx)

	after := store.extends.Load()
	time.Sleep(60 * time.Millisecond)
	if got := store.extends.Load(); got != after {
		t.Errorf("%d heartbeats after every attempt ended: a goroutine leaked", got-after)
	}
}

// countingStore counts Release calls.
type countingStore struct {
	idem.Store
	releases atomic.Int32
}

func (s *countingStore) Release(ctx context.Context, key, token string) error {
	s.releases.Add(1)
	return s.Store.Release(ctx, key, token)
}

func TestReleaseAfterEndIsNoOp(t *testing.T) {
	ctx := context.Background()
	store := &countingStore{Store: memstore.New()}
	engine := idem.New(store)

	attempt, _, err := engine.Begin(ctx, "completed")
	if err != nil {
		t.Fatal(err)
	}
	if err := attempt.Complete(ctx, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := attempt.Complete(ctx, []byte("y")); err != nil {
		t.Errorf("second Complete: err = %v, want nil", err)
	}
	if err := attempt.Release(ctx); err != nil {
		t.Errorf("Release after Complete: err = %v, want nil", err)
	}
	if got := store.releases.Load(); got != 0 {
		t.Errorf("Release after Complete reached the store %d times, want 0", got)
	}
	if _, stored, _ := engine.Begin(ctx, "completed"); string(stored) != "x" {
		t.Errorf("stored result = %q, want %q", stored, "x")
	}

	attempt, _, err = engine.Begin(ctx, "released")
	if err != nil {
		t.Fatal(err)
	}
	if err := attempt.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := attempt.Release(ctx); err != nil {
		t.Errorf("second Release: err = %v, want nil", err)
	}
	if got := store.releases.Load(); got != 1 {
		t.Errorf("the store saw %d releases, want 1", got)
	}
	if err := attempt.Complete(ctx, []byte("late")); !errors.Is(err, idem.ErrLeaseLost) {
		t.Errorf("Complete after Release: err = %v, want ErrLeaseLost", err)
	}
}

func TestFailedCompleteCanStillRelease(t *testing.T) {
	ctx := context.Background()
	down := errors.New("store is down")
	store := &faultyStore{Store: memstore.New(), completeErr: down}
	engine := idem.New(store)

	attempt, _, err := engine.Begin(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}
	if err := attempt.Complete(ctx, []byte("x")); !errors.Is(err, down) {
		t.Fatalf("Complete: err = %v, want the store's error", err)
	}
	if err := attempt.Release(ctx); err != nil {
		t.Fatalf("Release after a failed Complete: %v", err)
	}
	again, _, err := engine.Begin(ctx, "key")
	if err != nil {
		t.Fatalf("Begin after Release: err = %v, want the key to be free", err)
	}
	_ = again.Release(ctx)
}

func TestProvisionalCompleteLeavesAttemptOpen(t *testing.T) {
	ctx := context.Background()
	store := &countingStore{Store: memstore.New()}
	engine := idem.New(store)

	attempt, _, err := engine.Begin(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}
	if err := attempt.Complete(idem.Provisional(ctx), []byte("x")); err != nil {
		t.Fatal(err)
	}
	// The store decides what a release after a provisional write means. The
	// attempt must ask it rather than assume the write became permanent.
	_ = attempt.Release(ctx)
	if got := store.releases.Load(); got != 1 {
		t.Errorf("Release after a provisional Complete reached the store %d times, want 1", got)
	}
}

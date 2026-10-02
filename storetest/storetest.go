// Package storetest is a conformance suite for implementations of idem.Store.
//
// It checks a store against the contract documented on [idem.Store]. Call
// [Run] from a test in the package that implements the store:
//
//	func TestStore(t *testing.T) {
//		storetest.Run(t, func(t *testing.T) idem.Store {
//			return mystore.New(connect(t))
//		})
//	}
//
// The suite works with real time, because that is the only clock a remote
// store shares with the test. It uses leases of a few hundred milliseconds
// and takes about three seconds to run.
package storetest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/udaykishore-resu/idem"
)

const (
	long  = time.Minute            // a lease or ttl that does not expire during a test
	short = 250 * time.Millisecond // a lease or ttl that the test waits out
	// past is how long to sleep until a short lease has certainly expired.
	past = short + 250*time.Millisecond
)

// Run runs the conformance suite. It calls newStore once per subtest; the
// stores may share a backend, since every subtest uses its own random keys.
func Run(t *testing.T, newStore func(t *testing.T) idem.Store) {
	t.Helper()
	for _, tc := range []struct {
		name string
		fn   func(*testing.T, idem.Store)
	}{
		{"AcquireFreeKey", testAcquireFreeKey},
		{"AcquireInProgress", testAcquireInProgress},
		{"CompleteStoresResult", testCompleteStoresResult},
		{"CompleteIsBinarySafe", testCompleteIsBinarySafe},
		{"CompleteEmptyResult", testCompleteEmptyResult},
		{"CompleteTwiceIsNoOp", testCompleteTwiceIsNoOp},
		{"CompleteWithWrongToken", testCompleteWithWrongToken},
		{"CompleteUnknownKey", testCompleteUnknownKey},
		{"ReleaseFreesKey", testReleaseFreesKey},
		{"ReleaseWithWrongToken", testReleaseWithWrongToken},
		{"ReleaseAfterComplete", testReleaseAfterComplete},
		{"ExtendKeepsLease", testExtendKeepsLease},
		{"ExtendWithWrongToken", testExtendWithWrongToken},
		{"ExtendAfterComplete", testExtendAfterComplete},
		{"ExpiredLeaseCanBeTakenOver", testExpiredLeaseCanBeTakenOver},
		{"ExpiredLeaseCannotComplete", testExpiredLeaseCannotComplete},
		{"CompletedRecordExpires", testCompletedRecordExpires},
		{"KeysAreIndependent", testKeysAreIndependent},
		{"ConcurrentAcquire", testConcurrentAcquire},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newStore(t))
		})
	}
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

func random(t *testing.T, prefix string) string {
	t.Helper()
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return prefix + hex.EncodeToString(b[:])
}

func newKey(t *testing.T) string   { return random(t, "storetest-") }
func newToken(t *testing.T) string { return random(t, "") }

func acquire(t *testing.T, s idem.Store, key, token, fp string, lease time.Duration) idem.Claim {
	t.Helper()
	claim, err := s.Acquire(ctx(t), key, token, fp, lease)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	return claim
}

func mustAcquire(t *testing.T, s idem.Store, key, token, fp string, lease time.Duration) {
	t.Helper()
	if claim := acquire(t, s, key, token, fp, lease); claim.State != idem.StateAcquired {
		t.Fatalf("Acquire: state = %v, want %v", claim.State, idem.StateAcquired)
	}
}

func wantState(t *testing.T, claim idem.Claim, want idem.State) {
	t.Helper()
	if claim.State != want {
		t.Fatalf("state = %v, want %v", claim.State, want)
	}
}

func wantLeaseLost(t *testing.T, op string, err error) {
	t.Helper()
	if !errors.Is(err, idem.ErrLeaseLost) {
		t.Fatalf("%s: err = %v, want idem.ErrLeaseLost", op, err)
	}
}

func testAcquireFreeKey(t *testing.T, s idem.Store) {
	claim := acquire(t, s, newKey(t), newToken(t), "fp", long)
	wantState(t, claim, idem.StateAcquired)
}

func testAcquireInProgress(t *testing.T, s idem.Store) {
	key := newKey(t)
	mustAcquire(t, s, key, newToken(t), "fp-first", long)

	claim := acquire(t, s, key, newToken(t), "fp-second", long)
	wantState(t, claim, idem.StateInProgress)
	if claim.Fingerprint != "fp-first" {
		t.Errorf("fingerprint = %q, want the one stored by the holder", claim.Fingerprint)
	}
}

func testCompleteStoresResult(t *testing.T, s idem.Store) {
	key, token := newKey(t), newToken(t)
	mustAcquire(t, s, key, token, "fp", long)
	if err := s.Complete(ctx(t), key, token, []byte("result"), long); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	claim := acquire(t, s, key, newToken(t), "other", long)
	wantState(t, claim, idem.StateCompleted)
	if claim.Fingerprint != "fp" {
		t.Errorf("fingerprint = %q, want %q", claim.Fingerprint, "fp")
	}
	if string(claim.Result) != "result" {
		t.Errorf("result = %q, want %q", claim.Result, "result")
	}
}

func testCompleteIsBinarySafe(t *testing.T, s idem.Store) {
	key, token := newKey(t), newToken(t)
	want := make([]byte, 0, 70_000)
	for i := 0; len(want) < 70_000; i++ {
		want = append(want, byte(i)) // every byte value, including 0x00 and invalid UTF-8
	}
	mustAcquire(t, s, key, token, "", long)
	if err := s.Complete(ctx(t), key, token, want, long); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	claim := acquire(t, s, key, newToken(t), "", long)
	wantState(t, claim, idem.StateCompleted)
	if !bytes.Equal(claim.Result, want) {
		t.Errorf("result differs from what was stored (got %d bytes, want %d)", len(claim.Result), len(want))
	}
}

func testCompleteEmptyResult(t *testing.T, s idem.Store) {
	key, token := newKey(t), newToken(t)
	mustAcquire(t, s, key, token, "", long)
	if err := s.Complete(ctx(t), key, token, nil, long); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	claim := acquire(t, s, key, newToken(t), "", long)
	wantState(t, claim, idem.StateCompleted)
	if len(claim.Result) != 0 {
		t.Errorf("result = %q, want empty", claim.Result)
	}
}

func testCompleteTwiceIsNoOp(t *testing.T, s idem.Store) {
	key, token := newKey(t), newToken(t)
	mustAcquire(t, s, key, token, "", long)
	if err := s.Complete(ctx(t), key, token, []byte("first"), long); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if err := s.Complete(ctx(t), key, token, []byte("second"), long); err != nil {
		t.Fatalf("second Complete with the same token: err = %v, want nil", err)
	}

	claim := acquire(t, s, key, newToken(t), "", long)
	wantState(t, claim, idem.StateCompleted)
	if string(claim.Result) != "first" {
		t.Errorf("result = %q, want %q: a repeated Complete must not overwrite", claim.Result, "first")
	}
}

func testCompleteWithWrongToken(t *testing.T, s idem.Store) {
	key, token := newKey(t), newToken(t)
	mustAcquire(t, s, key, token, "", long)
	wantLeaseLost(t, "Complete with a foreign token", s.Complete(ctx(t), key, newToken(t), []byte("x"), long))

	wantState(t, acquire(t, s, key, newToken(t), "", long), idem.StateInProgress)
	if err := s.Complete(ctx(t), key, token, []byte("mine"), long); err != nil {
		t.Fatalf("Complete by the holder after a foreign attempt: %v", err)
	}
}

func testCompleteUnknownKey(t *testing.T, s idem.Store) {
	wantLeaseLost(t, "Complete on a key that was never acquired",
		s.Complete(ctx(t), newKey(t), newToken(t), []byte("x"), long))
}

func testReleaseFreesKey(t *testing.T, s idem.Store) {
	key, token := newKey(t), newToken(t)
	mustAcquire(t, s, key, token, "", long)
	if err := s.Release(ctx(t), key, token); err != nil {
		t.Fatalf("Release: %v", err)
	}
	mustAcquire(t, s, key, newToken(t), "", long)
}

func testReleaseWithWrongToken(t *testing.T, s idem.Store) {
	key := newKey(t)
	mustAcquire(t, s, key, newToken(t), "", long)
	wantLeaseLost(t, "Release with a foreign token", s.Release(ctx(t), key, newToken(t)))
	wantState(t, acquire(t, s, key, newToken(t), "", long), idem.StateInProgress)
}

func testReleaseAfterComplete(t *testing.T, s idem.Store) {
	key, token := newKey(t), newToken(t)
	mustAcquire(t, s, key, token, "", long)
	if err := s.Complete(ctx(t), key, token, []byte("done"), long); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	wantLeaseLost(t, "Release after Complete", s.Release(ctx(t), key, token))
	wantState(t, acquire(t, s, key, newToken(t), "", long), idem.StateCompleted)
}

func testExtendKeepsLease(t *testing.T, s idem.Store) {
	key, token := newKey(t), newToken(t)
	mustAcquire(t, s, key, token, "", short)
	if err := s.Extend(ctx(t), key, token, long); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	time.Sleep(past)

	wantState(t, acquire(t, s, key, newToken(t), "", long), idem.StateInProgress)
	if err := s.Complete(ctx(t), key, token, []byte("done"), long); err != nil {
		t.Fatalf("Complete after Extend: %v", err)
	}
}

func testExtendWithWrongToken(t *testing.T, s idem.Store) {
	key := newKey(t)
	mustAcquire(t, s, key, newToken(t), "", short)
	wantLeaseLost(t, "Extend with a foreign token", s.Extend(ctx(t), key, newToken(t), long))
	time.Sleep(past)
	mustAcquire(t, s, key, newToken(t), "", long) // the foreign Extend did not prolong the lease
}

func testExtendAfterComplete(t *testing.T, s idem.Store) {
	key, token := newKey(t), newToken(t)
	mustAcquire(t, s, key, token, "", long)
	if err := s.Complete(ctx(t), key, token, []byte("done"), long); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	wantLeaseLost(t, "Extend after Complete", s.Extend(ctx(t), key, token, short))
	time.Sleep(past)
	wantState(t, acquire(t, s, key, newToken(t), "", long), idem.StateCompleted) // ttl was not shortened
}

func testExpiredLeaseCanBeTakenOver(t *testing.T, s idem.Store) {
	key, stale, fresh := newKey(t), newToken(t), newToken(t)
	mustAcquire(t, s, key, stale, "fp-stale", short)
	time.Sleep(past)
	mustAcquire(t, s, key, fresh, "fp-fresh", long)

	wantLeaseLost(t, "Complete by the stale holder", s.Complete(ctx(t), key, stale, []byte("stale"), long))
	wantLeaseLost(t, "Extend by the stale holder", s.Extend(ctx(t), key, stale, long))
	wantLeaseLost(t, "Release by the stale holder", s.Release(ctx(t), key, stale))

	if err := s.Complete(ctx(t), key, fresh, []byte("fresh"), long); err != nil {
		t.Fatalf("Complete by the new holder: %v", err)
	}
	claim := acquire(t, s, key, newToken(t), "", long)
	wantState(t, claim, idem.StateCompleted)
	if string(claim.Result) != "fresh" || claim.Fingerprint != "fp-fresh" {
		t.Errorf("record = (%q, %q), want the new holder's (%q, %q)",
			claim.Result, claim.Fingerprint, "fresh", "fp-fresh")
	}
}

func testExpiredLeaseCannotComplete(t *testing.T, s idem.Store) {
	key, token := newKey(t), newToken(t)
	mustAcquire(t, s, key, token, "", short)
	time.Sleep(past)

	wantLeaseLost(t, "Complete after the lease expired", s.Complete(ctx(t), key, token, []byte("late"), long))
	wantLeaseLost(t, "Extend after the lease expired", s.Extend(ctx(t), key, token, long))
	wantLeaseLost(t, "Release after the lease expired", s.Release(ctx(t), key, token))
	mustAcquire(t, s, key, newToken(t), "", long)
}

func testCompletedRecordExpires(t *testing.T, s idem.Store) {
	key, token := newKey(t), newToken(t)
	mustAcquire(t, s, key, token, "", long)
	if err := s.Complete(ctx(t), key, token, []byte("done"), short); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	wantState(t, acquire(t, s, key, newToken(t), "", long), idem.StateCompleted)
	time.Sleep(past)
	mustAcquire(t, s, key, newToken(t), "", long)
}

func testKeysAreIndependent(t *testing.T, s idem.Store) {
	a, b, token := newKey(t), newKey(t), newToken(t)
	mustAcquire(t, s, a, token, "", long)
	mustAcquire(t, s, b, token, "", long) // the same token on another key is unrelated
	if err := s.Release(ctx(t), a, token); err != nil {
		t.Fatalf("Release: %v", err)
	}
	wantState(t, acquire(t, s, b, newToken(t), "", long), idem.StateInProgress)
}

func testConcurrentAcquire(t *testing.T, s idem.Store) {
	const callers = 32
	key := newKey(t)
	c := ctx(t)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		acquired int
		errs     []error
		start    = make(chan struct{})
	)
	for range callers {
		token := newToken(t)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			claim, err := s.Acquire(c, key, token, "", long)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				errs = append(errs, err)
			case claim.State == idem.StateAcquired:
				acquired++
			case claim.State != idem.StateInProgress:
				errs = append(errs, errors.New("unexpected state "+claim.State.String()))
			}
		}()
	}
	close(start)
	wg.Wait()

	for _, err := range errs {
		t.Errorf("Acquire: %v", err)
	}
	if acquired != 1 {
		t.Errorf("%d of %d concurrent callers acquired the key, want exactly 1", acquired, callers)
	}
}

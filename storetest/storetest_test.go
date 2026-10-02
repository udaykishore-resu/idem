package storetest_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/udaykishore-resu/idem"
	"github.com/udaykishore-resu/idem/storetest"
)

// A conformance suite is only worth something if it fails for a store that
// breaks the contract. TestSuiteCatchesBrokenStores runs the suite against a
// store with one defect switched on at a time and expects it to fail every
// time; TestSuitePassesCorrectStore runs it with no defect.
//
// A failing suite cannot run inside a passing test, so each defect is tested
// in a child process: the test binary runs itself with -test.run=TestChild
// and reads the exit status.

const defectEnv = "IDEM_STORETEST_DEFECT"

var defects = []string{
	"never-expires",
	"completed-never-expires",
	"racy-acquire",
	"result-cut-at-nul",
	"fingerprint-not-returned",
	"acquire-overwrites-fingerprint",
	"complete-after-expiry",
	"complete-ignores-token",
	"second-complete-overwrites",
	"second-complete-fails",
	"complete-keeps-lease-expiry",
	"release-ignores-token",
	"release-deletes-completed",
	"release-does-nothing",
	"extend-ignores-token",
	"extend-touches-completed",
	"extend-does-nothing",
}

func TestSuitePassesCorrectStore(t *testing.T) {
	storetest.Run(t, func(*testing.T) idem.Store { return newDefective("") })
}

func TestSuiteCatchesBrokenStores(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the suite once per defect")
	}
	for _, defect := range defects {
		t.Run(defect, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command(os.Args[0], "-test.run=^TestChild$")
			cmd.Env = append(os.Environ(), defectEnv+"="+defect)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("the suite passed a store with the defect %q", defect)
			}
			if !bytes.Contains(out, []byte("--- FAIL: TestChild/")) {
				t.Fatalf("the child did not fail in the suite: %v\n%s", err, out)
			}
		})
	}
}

// TestChild is the body of the child process. Run directly, it does nothing.
func TestChild(t *testing.T) {
	defect := os.Getenv(defectEnv)
	if defect == "" {
		t.Skip("only runs as a child of TestSuiteCatchesBrokenStores")
	}
	storetest.Run(t, func(*testing.T) idem.Store { return newDefective(defect) })
}

type record struct {
	token, fingerprint string
	completed          bool
	result             []byte
	expires            time.Time
}

// defective is a correct in-memory store with one switchable defect.
type defective struct {
	mu      sync.Mutex
	records map[string]*record
	defect  string
}

func newDefective(defect string) *defective {
	return &defective{records: map[string]*record{}, defect: defect}
}

func (s *defective) is(defect string) bool { return s.defect == defect }

func (s *defective) live(key string) *record {
	r := s.records[key]
	if r == nil {
		return nil
	}
	if s.is("never-expires") || (s.is("completed-never-expires") && r.completed) {
		return r
	}
	if !time.Now().Before(r.expires) {
		delete(s.records, key)
		return nil
	}
	return r
}

func (s *defective) Acquire(_ context.Context, key, token, fingerprint string, lease time.Duration) (idem.Claim, error) {
	if s.is("racy-acquire") {
		s.mu.Lock()
		free := s.live(key) == nil
		s.mu.Unlock()
		if free {
			time.Sleep(5 * time.Millisecond) // check, then act
			s.mu.Lock()
			s.records[key] = &record{token: token, fingerprint: fingerprint, expires: time.Now().Add(lease)}
			s.mu.Unlock()
			return idem.Claim{State: idem.StateAcquired}, nil
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.live(key)
	if r == nil {
		s.records[key] = &record{token: token, fingerprint: fingerprint, expires: time.Now().Add(lease)}
		return idem.Claim{State: idem.StateAcquired}, nil
	}
	if s.is("acquire-overwrites-fingerprint") {
		r.fingerprint = fingerprint
	}
	claim := idem.Claim{State: idem.StateInProgress, Fingerprint: r.fingerprint}
	if s.is("fingerprint-not-returned") {
		claim.Fingerprint = ""
	}
	if r.completed {
		claim.State = idem.StateCompleted
		claim.Result = bytes.Clone(r.result)
		if i := bytes.IndexByte(claim.Result, 0); i >= 0 && s.is("result-cut-at-nul") {
			claim.Result = claim.Result[:i]
		}
	}
	return claim, nil
}

func (s *defective) Complete(_ context.Context, key, token string, result []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.live(key)
	if r == nil {
		if s.is("complete-after-expiry") {
			s.records[key] = &record{token: token, completed: true, result: bytes.Clone(result), expires: time.Now().Add(ttl)}
			return nil
		}
		return idem.ErrLeaseLost
	}
	if r.token != token && !s.is("complete-ignores-token") {
		return idem.ErrLeaseLost
	}
	if r.completed {
		if s.is("second-complete-fails") {
			return idem.ErrLeaseLost
		}
		if s.is("second-complete-overwrites") {
			r.result = bytes.Clone(result)
		}
		return nil
	}
	r.completed, r.result = true, bytes.Clone(result)
	if !s.is("complete-keeps-lease-expiry") {
		r.expires = time.Now().Add(ttl)
	} else {
		r.expires = time.Now().Add(100 * time.Millisecond)
	}
	return nil
}

func (s *defective) Release(_ context.Context, key, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.live(key)
	switch {
	case r == nil:
		return idem.ErrLeaseLost
	case r.token != token && !s.is("release-ignores-token"):
		return idem.ErrLeaseLost
	case r.completed && !s.is("release-deletes-completed"):
		return idem.ErrLeaseLost
	}
	if !s.is("release-does-nothing") {
		delete(s.records, key)
	}
	return nil
}

func (s *defective) Extend(_ context.Context, key, token string, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.live(key)
	switch {
	case r == nil:
		return idem.ErrLeaseLost
	case r.token != token && !s.is("extend-ignores-token"):
		return idem.ErrLeaseLost
	case r.completed && !s.is("extend-touches-completed"):
		return idem.ErrLeaseLost
	}
	if !s.is("extend-does-nothing") {
		r.expires = time.Now().Add(lease)
	}
	return nil
}

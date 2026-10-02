package memstore_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/udaykishore-resu/idem"
	"github.com/udaykishore-resu/idem/memstore"
	"github.com/udaykishore-resu/idem/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(*testing.T) idem.Store { return memstore.New() })
}

// clock is a manually advanced time source.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestExpiredRecordsAreSwept(t *testing.T) {
	ctx := context.Background()
	clk := &clock{now: time.Unix(1_700_000_000, 0)}
	s := memstore.New(memstore.WithClock(clk.Now))

	for _, key := range []string{"a", "b", "c"} {
		if _, err := s.Acquire(ctx, key, "token", "", time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Complete(ctx, "c", "token", []byte("kept"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if got := s.Len(); got != 3 {
		t.Fatalf("Len = %d, want 3", got)
	}

	// Nobody touches a or b again. The next Acquire after the sweep interval
	// must still get rid of them.
	clk.Advance(2 * time.Minute)
	if _, err := s.Acquire(ctx, "d", "token", "", time.Second); err != nil {
		t.Fatal(err)
	}
	if got := s.Len(); got != 2 {
		t.Fatalf("Len = %d after the sweep, want 2 (c and d)", got)
	}
}

func TestResultIsCopied(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	result := []byte("abc")

	if _, err := s.Acquire(ctx, "k", "token", "", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, "k", "token", result, time.Minute); err != nil {
		t.Fatal(err)
	}
	result[0] = 'X' // the caller reuses its buffer

	first, _ := s.Acquire(ctx, "k", "other", "", time.Minute)
	first.Result[1] = 'Y' // a reader scribbles on what it was given
	second, _ := s.Acquire(ctx, "k", "other", "", time.Minute)
	if string(second.Result) != "abc" {
		t.Fatalf("stored result = %q, want %q", second.Result, "abc")
	}
}

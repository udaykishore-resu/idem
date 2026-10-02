package redisstore_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/udaykishore-resu/idem"
	"github.com/udaykishore-resu/idem/redisstore"
	"github.com/udaykishore-resu/idem/storetest"
)

// TestConformance runs the store contract against a real Redis server. Point
// IDEM_REDIS_ADDR at one to run it:
//
//	IDEM_REDIS_ADDR=localhost:6379 go test ./redisstore
func TestConformance(t *testing.T) {
	addr := os.Getenv("IDEM_REDIS_ADDR")
	if addr == "" {
		t.Skip("IDEM_REDIS_ADDR is not set")
	}
	client := respClient{addr: addr}
	if _, err := client.Eval(context.Background(), "return 1", nil); err != nil {
		t.Fatalf("cannot reach Redis at %s: %v", addr, err)
	}
	storetest.Run(t, func(*testing.T) idem.Store {
		return redisstore.New(client, redisstore.WithPrefix("idem-test:"))
	})
}

func TestRecordLayout(t *testing.T) {
	addr := os.Getenv("IDEM_REDIS_ADDR")
	if addr == "" {
		t.Skip("IDEM_REDIS_ADDR is not set")
	}
	ctx := context.Background()
	client := respClient{addr: addr}
	store := redisstore.New(client, redisstore.WithPrefix("idem-layout:"))
	key := fmt.Sprintf("k-%d", time.Now().UnixNano())

	if _, err := store.Acquire(ctx, key, "tok", "fp", time.Minute); err != nil {
		t.Fatal(err)
	}
	ttl, err := client.do(ctx, "PTTL", "idem-layout:"+key)
	if err != nil {
		t.Fatal(err)
	}
	if ms := ttl.(int64); ms <= 0 || ms > time.Minute.Milliseconds() {
		t.Errorf("PTTL of an in-progress record = %dms, want within the lease", ms)
	}

	if err := store.Complete(ctx, key, "tok", []byte("res"), time.Hour); err != nil {
		t.Fatal(err)
	}
	ttl, err = client.do(ctx, "PTTL", "idem-layout:"+key)
	if err != nil {
		t.Fatal(err)
	}
	if ms := ttl.(int64); ms <= time.Minute.Milliseconds() || ms > time.Hour.Milliseconds() {
		t.Errorf("PTTL of a completed record = %dms, want the result ttl", ms)
	}
	if _, err := client.do(ctx, "DEL", "idem-layout:"+key); err != nil {
		t.Fatal(err)
	}
}

// Client libraries disagree about reply types: bulk strings arrive as string
// from some and as []byte from others. The store has to accept both.
func TestReplyTypes(t *testing.T) {
	ctx := context.Background()
	for name, reply := range map[string]any{
		"strings": []any{int64(3), "fp", "result"},
		"bytes":   []any{int64(3), []byte("fp"), []byte("result")},
		"int":     []any{3, "fp", "result"},
	} {
		t.Run(name, func(t *testing.T) {
			store := redisstore.New(fixed{reply: reply})
			claim, err := store.Acquire(ctx, "k", "t", "fp", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if claim.State != idem.StateCompleted || claim.Fingerprint != "fp" || string(claim.Result) != "result" {
				t.Errorf("claim = %+v", claim)
			}
		})
	}
}

func TestMalformedReplies(t *testing.T) {
	ctx := context.Background()
	for name, reply := range map[string]any{
		"nil":            nil,
		"not an array":   "OK",
		"empty array":    []any{},
		"unknown state":  []any{int64(9)},
		"missing fields": []any{int64(3), "fp"},
		"bad state type": []any{3.5},
	} {
		t.Run(name, func(t *testing.T) {
			store := redisstore.New(fixed{reply: reply})
			if _, err := store.Acquire(ctx, "k", "t", "", time.Second); err == nil {
				t.Error("Acquire accepted a malformed reply")
			}
		})
	}
	store := redisstore.New(fixed{reply: "nonsense"})
	if err := store.Complete(ctx, "k", "t", nil, time.Second); err == nil || errors.Is(err, idem.ErrLeaseLost) {
		t.Errorf("Complete: err = %v, want a decoding error", err)
	}
}

func TestClientErrorsAreWrapped(t *testing.T) {
	ctx := context.Background()
	down := errors.New("dial tcp: connection refused")
	store := redisstore.New(fixed{err: down})

	if _, err := store.Acquire(ctx, "k", "t", "", time.Second); !errors.Is(err, down) {
		t.Errorf("Acquire: err = %v", err)
	}
	if err := store.Complete(ctx, "k", "t", nil, time.Second); !errors.Is(err, down) {
		t.Errorf("Complete: err = %v", err)
	}
	if err := store.Release(ctx, "k", "t"); !errors.Is(err, down) {
		t.Errorf("Release: err = %v", err)
	}
	if err := store.Extend(ctx, "k", "t", time.Second); !errors.Is(err, down) {
		t.Errorf("Extend: err = %v", err)
	}
}

func TestKeyPrefixAndArguments(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{reply: int64(1)}

	store := redisstore.New(rec)
	if err := store.Extend(ctx, "order-1", "tok", 1500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got := rec.keys; len(got) != 1 || got[0] != "idem:order-1" {
		t.Errorf("keys = %q, want the default prefix", got)
	}
	if len(rec.args) != 2 || rec.args[0] != "tok" || rec.args[1] != int64(1500) {
		t.Errorf("args = %v, want the token and the lease in milliseconds", rec.args)
	}

	store = redisstore.New(rec, redisstore.WithPrefix("svc:{idem}:"))
	if err := store.Release(ctx, "order-1", "tok"); err != nil {
		t.Fatal(err)
	}
	if got := rec.keys; len(got) != 1 || got[0] != "svc:{idem}:order-1" {
		t.Errorf("keys = %q, want the custom prefix", got)
	}

	// PEXPIRE rejects 0, so a sub-millisecond lease is rounded up.
	if err := store.Extend(ctx, "order-1", "tok", time.Microsecond); err != nil {
		t.Fatal(err)
	}
	if rec.args[1] != int64(1) {
		t.Errorf("lease argument = %v, want 1", rec.args[1])
	}
}

type fixed struct {
	reply any
	err   error
}

func (f fixed) Eval(context.Context, string, []string, ...any) (any, error) { return f.reply, f.err }

type recorder struct {
	reply any
	keys  []string
	args  []any
}

func (r *recorder) Eval(_ context.Context, _ string, keys []string, args ...any) (any, error) {
	r.keys, r.args = keys, args
	return r.reply, nil
}

// respClient is a minimal Redis client: one connection per command, RESP2
// only. It exists so that this module can test against a real server without
// depending on a client library, and it doubles as proof that the Evaler
// interface is all the store needs.
type respClient struct{ addr string }

func (c respClient) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	cmd := make([]any, 0, 3+len(keys)+len(args))
	cmd = append(cmd, "EVAL", script, len(keys))
	for _, k := range keys {
		cmd = append(cmd, k)
	}
	return c.do(ctx, append(cmd, args...)...)
}

func (c respClient) do(ctx context.Context, cmd ...any) (any, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(cmd))
	for _, arg := range cmd {
		var s string
		switch v := arg.(type) {
		case string:
			s = v
		case []byte:
			s = string(v)
		case int:
			s = strconv.Itoa(v)
		case int64:
			s = strconv.FormatInt(v, 10)
		default:
			return nil, fmt.Errorf("respClient: unsupported argument type %T", arg)
		}
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(s), s)
	}
	if _, err := io.WriteString(conn, b.String()); err != nil {
		return nil, err
	}
	return readReply(bufio.NewReader(conn))
}

func readReply(r *bufio.Reader) (any, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		return nil, errors.New("respClient: empty reply line")
	}
	switch kind, rest := line[0], line[1:]; kind {
	case '+':
		return rest, nil
	case '-':
		return nil, errors.New(rest)
	case ':':
		return strconv.ParseInt(rest, 10, 64)
	case '$':
		n, err := strconv.Atoi(rest)
		if err != nil || n < 0 {
			return nil, err
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(rest)
		if err != nil || n < 0 {
			return nil, err
		}
		items := make([]any, n)
		for i := range items {
			if items[i], err = readReply(r); err != nil {
				return nil, err
			}
		}
		return items, nil
	default:
		return nil, fmt.Errorf("respClient: unexpected reply type %q", kind)
	}
}

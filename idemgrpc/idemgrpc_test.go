package idemgrpc_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/udaykishore-resu/idem"
	"github.com/udaykishore-resu/idem/idemgrpc"
	"github.com/udaykishore-resu/idem/memstore"
)

// The tests use the standard health service because its generated code ships
// with gRPC. The server answers call n with status n, so a replay can be told
// from a second run.
type service struct {
	healthpb.UnimplementedHealthServer
	calls atomic.Int32
	hook  func(ctx context.Context, call int32) error
}

func (s *service) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	n := s.calls.Add(1)
	if s.hook != nil {
		if err := s.hook(ctx, n); err != nil {
			return nil, err
		}
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_ServingStatus(n)}, nil
}

// serve starts a server with the interceptor and returns a client for it.
func serve(t *testing.T, svc *service, engine *idem.Engine, opts ...idemgrpc.Option) healthpb.HealthClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(idemgrpc.UnaryServerInterceptor(engine, opts...)))
	healthpb.RegisterHealthServer(server, svc)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return healthpb.NewHealthClient(conn)
}

func newEngine() *idem.Engine { return idem.New(memstore.New()) }

// call makes one Check call. kv are metadata pairs.
func call(t *testing.T, client healthpb.HealthClient, service string, kv ...string) (*healthpb.HealthCheckResponse, metadata.MD, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if len(kv) > 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, kv...)
	}
	var header metadata.MD
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: service}, grpc.Header(&header))
	return resp, header, err
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("code = %v (%v), want %v", got, err, want)
	}
}

func TestFirstCallRunsAndRepeatIsReplayed(t *testing.T) {
	svc := &service{}
	client := serve(t, svc, newEngine())

	first, firstHeader, err := call(t, client, "billing", "idempotency-key", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	second, secondHeader, err := call(t, client, "billing", "idempotency-key", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if got := svc.calls.Load(); got != 1 {
		t.Fatalf("method ran %d times, want 1", got)
	}
	if first.Status != second.Status {
		t.Errorf("replayed status %v, want %v", second.Status, first.Status)
	}
	if got := firstHeader.Get(idemgrpc.ReplayedMetadataKey); len(got) != 0 {
		t.Errorf("first response has %s: %v", idemgrpc.ReplayedMetadataKey, got)
	}
	if got := secondHeader.Get(idemgrpc.ReplayedMetadataKey); len(got) != 1 || got[0] != "true" {
		t.Errorf("replay has %s: %v, want true", idemgrpc.ReplayedMetadataKey, got)
	}

	if _, _, err := call(t, client, "billing", "idempotency-key", "key-2"); err != nil {
		t.Fatal(err)
	}
	if got := svc.calls.Load(); got != 2 {
		t.Errorf("method ran %d times, want a new key to run it again", got)
	}
}

func TestCallWithoutKey(t *testing.T) {
	t.Run("passes through by default", func(t *testing.T) {
		svc := &service{}
		client := serve(t, svc, newEngine())
		for range 2 {
			if _, _, err := call(t, client, "billing"); err != nil {
				t.Fatal(err)
			}
		}
		if got := svc.calls.Load(); got != 2 {
			t.Errorf("method ran %d times, want 2", got)
		}
	})
	t.Run("fails with RequireKey", func(t *testing.T) {
		svc := &service{}
		client := serve(t, svc, newEngine(), idemgrpc.RequireKey())
		_, _, err := call(t, client, "billing")
		wantCode(t, err, codes.InvalidArgument)
		if got := svc.calls.Load(); got != 0 {
			t.Errorf("method ran %d times, want 0", got)
		}
	})
}

func TestKeyReusedForDifferentRequest(t *testing.T) {
	svc := &service{}
	client := serve(t, svc, newEngine())

	if _, _, err := call(t, client, "billing", "idempotency-key", "key"); err != nil {
		t.Fatal(err)
	}
	_, _, err := call(t, client, "shipping", "idempotency-key", "key")
	wantCode(t, err, codes.InvalidArgument)
	if got := svc.calls.Load(); got != 1 {
		t.Errorf("method ran %d times, want 1", got)
	}
}

func TestRepeatWhileFirstIsRunning(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	svc := &service{hook: func(context.Context, int32) error {
		close(started)
		<-finish
		return nil
	}}
	client := serve(t, svc, newEngine())

	done := make(chan error, 1)
	go func() {
		_, _, err := call(t, client, "billing", "idempotency-key", "key")
		done <- err
	}()
	<-started
	_, _, err := call(t, client, "billing", "idempotency-key", "key")
	wantCode(t, err, codes.Aborted)

	close(finish)
	if err := <-done; err != nil {
		t.Fatalf("first call: %v", err)
	}
}

func TestWaitingForRunningCall(t *testing.T) {
	svc := &service{hook: func(context.Context, int32) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}}
	client := serve(t, svc, newEngine(), idemgrpc.EngineOptions(idem.WithWait(5*time.Second)))

	const clients = 16
	statuses := make(chan healthpb.HealthCheckResponse_ServingStatus, clients)
	var wg sync.WaitGroup
	for range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, _, err := call(t, client, "billing", "idempotency-key", "shared")
			if err != nil {
				t.Error(err)
				return
			}
			statuses <- resp.Status
		}()
	}
	wg.Wait()
	close(statuses)

	for s := range statuses {
		if s != 1 {
			t.Errorf("a client received status %v, want the first call's result", s)
		}
	}
	if got := svc.calls.Load(); got != 1 {
		t.Errorf("method ran %d times for %d concurrent clients, want 1", got, clients)
	}
}

func TestErrorsAreNotStored(t *testing.T) {
	svc := &service{hook: func(_ context.Context, call int32) error {
		if call == 1 {
			return status.Error(codes.ResourceExhausted, "try later")
		}
		return nil
	}}
	client := serve(t, svc, newEngine())

	_, _, err := call(t, client, "billing", "idempotency-key", "key")
	wantCode(t, err, codes.ResourceExhausted)

	resp, header, err := call(t, client, "billing", "idempotency-key", "key")
	if err != nil {
		t.Fatalf("retry after a failed call: %v", err)
	}
	if resp.Status != 2 || len(header.Get(idemgrpc.ReplayedMetadataKey)) != 0 {
		t.Errorf("retry: status %v, header %v, want the method to run again", resp.Status, header)
	}
}

func TestScopeSeparatesCallers(t *testing.T) {
	svc := &service{}
	tenant := idemgrpc.Scope(func(ctx context.Context) string {
		if v := metadata.ValueFromIncomingContext(ctx, "tenant"); len(v) > 0 {
			return v[0]
		}
		return ""
	})
	client := serve(t, svc, newEngine(), tenant)

	acme, _, err := call(t, client, "billing", "idempotency-key", "1", "tenant", "acme")
	if err != nil {
		t.Fatal(err)
	}
	globex, _, err := call(t, client, "billing", "idempotency-key", "1", "tenant", "globex")
	if err != nil {
		t.Fatal(err)
	}
	if acme.Status == globex.Status {
		t.Error("one tenant received the other's response")
	}
	again, header, err := call(t, client, "billing", "idempotency-key", "1", "tenant", "acme")
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != acme.Status || len(header.Get(idemgrpc.ReplayedMetadataKey)) != 1 {
		t.Error("a repeat within the tenant was not replayed")
	}
}

func TestSkipAndMetadataKey(t *testing.T) {
	svc := &service{}
	client := serve(t, svc, newEngine(),
		idemgrpc.Skip(func(method string) bool { return method == healthpb.Health_Check_FullMethodName }))
	for range 2 {
		if _, _, err := call(t, client, "billing", "idempotency-key", "key"); err != nil {
			t.Fatal(err)
		}
	}
	if got := svc.calls.Load(); got != 2 {
		t.Errorf("method ran %d times, want a skipped method to run every time", got)
	}

	svc = &service{}
	client = serve(t, svc, newEngine(), idemgrpc.MetadataKey("X-Request-Token"))
	for range 2 {
		if _, _, err := call(t, client, "billing", "x-request-token", "abc"); err != nil {
			t.Fatal(err)
		}
	}
	if got := svc.calls.Load(); got != 1 {
		t.Errorf("method ran %d times with a custom metadata key, want 1", got)
	}
}

func TestMalformedKeys(t *testing.T) {
	svc := &service{}
	client := serve(t, svc, newEngine())

	for name, kv := range map[string][]string{
		"sent twice":  {"idempotency-key", "a", "idempotency-key", "b"},
		"empty":       {"idempotency-key", ""},
		"inner space": {"idempotency-key", "a b"},
		"too long":    {"idempotency-key", strings.Repeat("k", idemgrpc.MaxKeyLength+1)},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := call(t, client, "billing", kv...)
			wantCode(t, err, codes.InvalidArgument)
		})
	}
	if got := svc.calls.Load(); got != 0 {
		t.Errorf("method ran %d times, want 0", got)
	}
}

// TestReplayInAnotherProcess sends the repeat to a second server that shares
// the store but has never run the method, as happens behind a load balancer
// or after a restart. That server has to find the response type in the
// protobuf registry.
func TestReplayInAnotherProcess(t *testing.T) {
	store := memstore.New()
	first, second := &service{}, &service{}
	clientA := serve(t, first, idem.New(store))
	clientB := serve(t, second, idem.New(store))

	original, _, err := call(t, clientA, "billing", "idempotency-key", "key")
	if err != nil {
		t.Fatal(err)
	}
	replayed, header, err := call(t, clientB, "billing", "idempotency-key", "key")
	if err != nil {
		t.Fatalf("replay on the second server: %v", err)
	}
	if replayed.Status != original.Status || len(header.Get(idemgrpc.ReplayedMetadataKey)) != 1 {
		t.Errorf("second server answered %v with header %v, want a replay of %v", replayed.Status, header, original.Status)
	}
	if got := second.calls.Load(); got != 0 {
		t.Errorf("the second server ran the method %d times, want 0", got)
	}
}

// brokenStore fails the operations it is told to fail.
type brokenStore struct {
	idem.Store
	acquire, complete error
}

func (s brokenStore) Acquire(ctx context.Context, key, token, fp string, lease time.Duration) (idem.Claim, error) {
	if s.acquire != nil {
		return idem.Claim{}, s.acquire
	}
	return s.Store.Acquire(ctx, key, token, fp, lease)
}

func (s brokenStore) Complete(ctx context.Context, key, token string, result []byte, ttl time.Duration) error {
	if s.complete != nil {
		return s.complete
	}
	return s.Store.Complete(ctx, key, token, result, ttl)
}

func TestStoreUnavailable(t *testing.T) {
	down := errors.New("dial tcp 10.0.0.7:6379: connection refused")
	report := func(got *atomic.Pointer[error]) idemgrpc.Option {
		return idemgrpc.OnStoreError(func(_ context.Context, _ string, err error) { got.Store(&err) })
	}

	t.Run("fails closed by default", func(t *testing.T) {
		var reported atomic.Pointer[error]
		svc := &service{}
		client := serve(t, svc, idem.New(brokenStore{Store: memstore.New(), acquire: down}), report(&reported))
		_, _, err := call(t, client, "billing", "idempotency-key", "key")
		wantCode(t, err, codes.Unavailable)
		if msg := status.Convert(err).Message(); msg == "" || strings.Contains(msg, "10.0.0.7") {
			t.Errorf("status message %q leaks the store's error or is empty", msg)
		}
		if got := svc.calls.Load(); got != 0 {
			t.Errorf("method ran %d times without protection, want 0", got)
		}
		if e := reported.Load(); e == nil || !errors.Is(*e, down) {
			t.Error("OnStoreError was not told")
		}
	})

	t.Run("fails open on request", func(t *testing.T) {
		svc := &service{}
		client := serve(t, svc, idem.New(brokenStore{Store: memstore.New(), acquire: down}), idemgrpc.FailOpen())
		if _, _, err := call(t, client, "billing", "idempotency-key", "key"); err != nil {
			t.Fatalf("err = %v, want the method's response", err)
		}
	})

	t.Run("response is returned when recording fails", func(t *testing.T) {
		var reported atomic.Pointer[error]
		svc := &service{}
		client := serve(t, svc, idem.New(brokenStore{Store: memstore.New(), complete: down}), report(&reported))
		if _, _, err := call(t, client, "billing", "idempotency-key", "key"); err != nil {
			t.Fatalf("err = %v, want the method's response: the work was done", err)
		}
		if e := reported.Load(); e == nil || !errors.Is(*e, down) {
			t.Error("OnStoreError was not told")
		}
	})
}

func TestPanicReleasesKey(t *testing.T) {
	intercept := idemgrpc.UnaryServerInterceptor(newEngine())
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("idempotency-key", "key"))
	info := &grpc.UnaryServerInfo{FullMethod: healthpb.Health_Check_FullMethodName}
	req := &healthpb.HealthCheckRequest{Service: "billing"}

	func() {
		defer func() {
			if recover() != "bug" {
				t.Error("the panic did not propagate")
			}
		}()
		_, _ = intercept(ctx, req, info, func(context.Context, any) (any, error) { panic("bug") })
	}()

	resp, err := intercept(ctx, req, info, func(context.Context, any) (any, error) {
		return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
	})
	if err != nil {
		t.Fatalf("retry after a panic: %v", err)
	}
	if resp.(*healthpb.HealthCheckResponse).Status != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("resp = %v", resp)
	}
}

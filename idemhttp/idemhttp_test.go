package idemhttp_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/udaykishore-resu/idem"
	"github.com/udaykishore-resu/idem/idemhttp"
	"github.com/udaykishore-resu/idem/memstore"
)

// orders is a handler that creates an order per call and counts its calls.
type orders struct {
	calls atomic.Int32
}

func (o *orders) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := o.calls.Add(1)
	body, _ := io.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", fmt.Sprintf("/orders/%d", n))
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, `{"id":%d,"request":%q}`, n, body)
}

func newEngine() *idem.Engine { return idem.New(memstore.New()) }

func do(t *testing.T, h http.Handler, method, target, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func post(t *testing.T, h http.Handler, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, h, http.MethodPost, "/orders", key, body)
}

func wantProblem(t *testing.T, rec *httptest.ResponseRecorder, status int) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, status, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("Content-Type = %q, want application/problem+json", ct)
	}
	var p struct {
		Title  string `json:"title"`
		Status int    `json:"status"`
		Detail string `json:"detail"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("body is not a problem document: %v (%s)", err, rec.Body)
	}
	if p.Title == "" || p.Detail == "" || p.Status != status {
		t.Errorf("problem = %+v, want a title, a detail and status %d", p, status)
	}
}

func TestFirstRequestRunsAndRepeatIsReplayed(t *testing.T) {
	app := &orders{}
	h := idemhttp.Middleware(newEngine())(app)

	first := post(t, h, "key-1", `{"sku":"a"}`)
	second := post(t, h, "key-1", `{"sku":"a"}`)

	if got := app.calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want 1", got)
	}
	if first.Code != http.StatusCreated || second.Code != http.StatusCreated {
		t.Errorf("statuses = %d, %d, want 201 twice", first.Code, second.Code)
	}
	if first.Body.String() != second.Body.String() {
		t.Errorf("replayed body %s, want %s", second.Body, first.Body)
	}
	if !strings.Contains(first.Body.String(), `sku`) {
		t.Errorf("handler did not see the request body: %s", first.Body)
	}
	for _, name := range []string{"Content-Type", "Location"} {
		if first.Header().Get(name) != second.Header().Get(name) {
			t.Errorf("replayed %s = %q, want %q", name, second.Header().Get(name), first.Header().Get(name))
		}
	}
	if got := first.Header().Get(idemhttp.ReplayedHeader); got != "" {
		t.Errorf("first response has %s: %q", idemhttp.ReplayedHeader, got)
	}
	if got := second.Header().Get(idemhttp.ReplayedHeader); got != "true" {
		t.Errorf("replay has %s: %q, want true", idemhttp.ReplayedHeader, got)
	}
}

func TestDifferentKeysRunSeparately(t *testing.T) {
	app := &orders{}
	h := idemhttp.Middleware(newEngine())(app)
	post(t, h, "key-1", `{}`)
	post(t, h, "key-2", `{}`)
	if got := app.calls.Load(); got != 2 {
		t.Errorf("handler ran %d times, want 2", got)
	}
}

func TestRequestWithoutKey(t *testing.T) {
	t.Run("passes through by default", func(t *testing.T) {
		app := &orders{}
		h := idemhttp.Middleware(newEngine())(app)
		post(t, h, "", `{}`)
		post(t, h, "", `{}`)
		if got := app.calls.Load(); got != 2 {
			t.Errorf("handler ran %d times, want 2", got)
		}
	})
	t.Run("is rejected with RequireKey", func(t *testing.T) {
		app := &orders{}
		h := idemhttp.Middleware(newEngine(), idemhttp.RequireKey())(app)
		wantProblem(t, post(t, h, "", `{}`), http.StatusBadRequest)
		if got := app.calls.Load(); got != 0 {
			t.Errorf("handler ran %d times, want 0", got)
		}
	})
}

func TestMethods(t *testing.T) {
	app := &orders{}
	h := idemhttp.Middleware(newEngine())(app)

	// GET, PUT and DELETE are idempotent by definition and pass through.
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		do(t, h, method, "/orders/1", "same-key", "")
		do(t, h, method, "/orders/1", "same-key", "")
	}
	if got := app.calls.Load(); got != 6 {
		t.Errorf("handler ran %d times for idempotent methods, want 6", got)
	}
	do(t, h, http.MethodPatch, "/orders/1", "patch-key", `{}`)
	do(t, h, http.MethodPatch, "/orders/1", "patch-key", `{}`)
	if got := app.calls.Load(); got != 7 {
		t.Errorf("handler ran %d times, want PATCH to be protected", got)
	}

	app = &orders{}
	h = idemhttp.Middleware(newEngine(), idemhttp.Methods(http.MethodPut))(app)
	do(t, h, http.MethodPut, "/orders/1", "put-key", `{}`)
	do(t, h, http.MethodPut, "/orders/1", "put-key", `{}`)
	post(t, h, "post-key", `{}`)
	post(t, h, "post-key", `{}`)
	if got := app.calls.Load(); got != 3 {
		t.Errorf("handler ran %d times, want 1 PUT and 2 POSTs", got)
	}
}

func TestKeyReusedForDifferentRequest(t *testing.T) {
	for name, repeat := range map[string]func(*testing.T, http.Handler) *httptest.ResponseRecorder{
		"body": func(t *testing.T, h http.Handler) *httptest.ResponseRecorder {
			return post(t, h, "key", `{"amount":900}`)
		},
		"path": func(t *testing.T, h http.Handler) *httptest.ResponseRecorder {
			return do(t, h, "POST", "/refunds", "key", `{"amount":500}`)
		},
		"query": func(t *testing.T, h http.Handler) *httptest.ResponseRecorder {
			return do(t, h, "POST", "/orders?dry_run=1", "key", `{"amount":500}`)
		},
		"method": func(t *testing.T, h http.Handler) *httptest.ResponseRecorder {
			return do(t, h, "PATCH", "/orders", "key", `{"amount":500}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			app := &orders{}
			h := idemhttp.Middleware(newEngine())(app)
			post(t, h, "key", `{"amount":500}`)
			wantProblem(t, repeat(t, h), http.StatusUnprocessableEntity)
			if got := app.calls.Load(); got != 1 {
				t.Errorf("handler ran %d times, want 1", got)
			}
		})
	}
}

func TestRepeatWhileFirstIsRunning(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	h := idemhttp.Middleware(newEngine())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		close(started)
		<-finish
		w.WriteHeader(http.StatusCreated)
	}))

	done := make(chan *httptest.ResponseRecorder)
	go func() { done <- post(t, h, "key", `{}`) }()
	<-started

	wantProblem(t, post(t, h, "key", `{}`), http.StatusConflict)
	close(finish)
	if first := <-done; first.Code != http.StatusCreated {
		t.Errorf("first request: status = %d, want 201", first.Code)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1", got)
	}
}

func TestWaitingForRunningRequest(t *testing.T) {
	app := &orders{}
	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		app.ServeHTTP(w, r)
	})
	h := idemhttp.Middleware(newEngine(), idemhttp.EngineOptions(idem.WithWait(5*time.Second)))(slow)
	srv := httptest.NewServer(h)
	defer srv.Close()

	const clients = 20
	bodies := make(chan string, clients)
	var wg sync.WaitGroup
	for range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPost, srv.URL+"/orders", strings.NewReader(`{"sku":"a"}`))
			req.Header.Set("Idempotency-Key", "shared")
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusCreated {
				t.Errorf("status = %d, body %s", resp.StatusCode, body)
			}
			bodies <- string(body)
		}()
	}
	wg.Wait()
	close(bodies)

	seen := map[string]int{}
	for body := range bodies {
		seen[body]++
	}
	if len(seen) != 1 {
		t.Errorf("clients received %d different bodies, want 1: %v", len(seen), seen)
	}
	if got := app.calls.Load(); got != 1 {
		t.Errorf("handler ran %d times for %d concurrent clients, want 1", got, clients)
	}
}

// flaky answers with the statuses it was given, one per call.
func flaky(calls *atomic.Int32, statuses ...int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(calls.Add(1))
		status := statuses[min(n, len(statuses))-1]
		w.WriteHeader(status)
		fmt.Fprintf(w, "call %d", n)
	})
}

func TestServerErrorsAreNotStored(t *testing.T) {
	var calls atomic.Int32
	h := idemhttp.Middleware(newEngine())(flaky(&calls, http.StatusBadGateway, http.StatusCreated))

	if rec := post(t, h, "key", `{}`); rec.Code != http.StatusBadGateway || rec.Body.String() != "call 1" {
		t.Fatalf("first: %d %s", rec.Code, rec.Body)
	}
	if rec := post(t, h, "key", `{}`); rec.Code != http.StatusCreated || rec.Body.String() != "call 2" {
		t.Fatalf("retry after a 502: %d %s, want the handler to run again", rec.Code, rec.Body)
	}
	if rec := post(t, h, "key", `{}`); rec.Code != http.StatusCreated || rec.Body.String() != "call 2" {
		t.Fatalf("repeat after success: %d %s, want a replay", rec.Code, rec.Body)
	}
}

func TestClientErrorsAreStored(t *testing.T) {
	var calls atomic.Int32
	h := idemhttp.Middleware(newEngine())(flaky(&calls, http.StatusPaymentRequired, http.StatusCreated))

	post(t, h, "key", `{}`)
	rec := post(t, h, "key", `{}`)
	if rec.Code != http.StatusPaymentRequired || rec.Body.String() != "call 1" {
		t.Fatalf("repeat: %d %s, want the stored 402", rec.Code, rec.Body)
	}
}

func TestRetryStatusesAreNotStored(t *testing.T) {
	for _, status := range []int{http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests} {
		var calls atomic.Int32
		h := idemhttp.Middleware(newEngine())(flaky(&calls, status, http.StatusCreated))
		post(t, h, "key", `{}`)
		if rec := post(t, h, "key", `{}`); rec.Code != http.StatusCreated {
			t.Errorf("after a %d: status = %d, want the handler to run again", status, rec.Code)
		}
	}
}

func TestStoreStatus(t *testing.T) {
	var calls atomic.Int32
	onlySuccess := idemhttp.StoreStatus(func(status int) bool { return status < 300 })
	h := idemhttp.Middleware(newEngine(), onlySuccess)(flaky(&calls, http.StatusConflict, http.StatusCreated))

	post(t, h, "key", `{}`)
	if rec := post(t, h, "key", `{}`); rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want the handler to run again after an unstored 409", rec.Code)
	}
}

func TestPanicReleasesKey(t *testing.T) {
	var calls atomic.Int32
	h := idemhttp.Middleware(newEngine())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			panic("bug")
		}
		w.WriteHeader(http.StatusCreated)
	}))

	func() {
		defer func() {
			if recover() != "bug" {
				t.Error("the panic did not reach the server")
			}
		}()
		post(t, h, "key", `{}`)
	}()
	if rec := post(t, h, "key", `{}`); rec.Code != http.StatusCreated {
		t.Fatalf("status = %d after a panic, want the retry to run", rec.Code)
	}
}

func TestRequestBodyLimit(t *testing.T) {
	app := &orders{}
	h := idemhttp.Middleware(newEngine(), idemhttp.MaxRequestBytes(16))(app)

	wantProblem(t, post(t, h, "key-1", strings.Repeat("x", 17)), http.StatusRequestEntityTooLarge)

	// The same body without a declared length, as a chunked upload sends it.
	req := httptest.NewRequest(http.MethodPost, "/orders", io.MultiReader(strings.NewReader(strings.Repeat("x", 17))))
	req.ContentLength = -1
	req.Header.Set("Idempotency-Key", "key-2")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	wantProblem(t, rec, http.StatusRequestEntityTooLarge)

	if got := app.calls.Load(); got != 0 {
		t.Errorf("handler ran %d times, want 0", got)
	}
	if rec := post(t, h, "key-3", strings.Repeat("x", 16)); rec.Code != http.StatusCreated {
		t.Errorf("a body at the limit: status = %d, want 201", rec.Code)
	}
	if rec := post(t, h, "", strings.Repeat("x", 100)); rec.Code != http.StatusCreated {
		t.Errorf("a large body without a key: status = %d, want it to pass through", rec.Code)
	}
}

func TestResponseTooLargeToStore(t *testing.T) {
	var calls atomic.Int32
	h := idemhttp.Middleware(newEngine(), idemhttp.MaxResponseBytes(10))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("X-Export", "yes")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "12345678")
		io.WriteString(w, "90abcdef")
		io.WriteString(w, "ghij")
	}))

	first := post(t, h, "key", `{}`)
	if first.Code != http.StatusOK || first.Body.String() != "1234567890abcdefghij" || first.Header().Get("X-Export") != "yes" {
		t.Fatalf("first: %d %q %v, want the whole response delivered", first.Code, first.Body, first.Header())
	}
	wantProblem(t, post(t, h, "key", `{}`), http.StatusConflict)
	if got := calls.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1: the work was done and must not be repeated", got)
	}
}

func TestScopeSeparatesCallers(t *testing.T) {
	app := &orders{}
	tenant := idemhttp.Scope(func(r *http.Request) string { return r.Header.Get("X-Tenant") })
	h := idemhttp.Middleware(newEngine(), tenant)(app)

	send := func(tenant string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{}`))
		req.Header.Set("Idempotency-Key", "1")
		req.Header.Set("X-Tenant", tenant)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	acme := send("acme")
	globex := send("globex")
	if got := app.calls.Load(); got != 2 {
		t.Fatalf("handler ran %d times, want 2: tenants share a key", got)
	}
	if acme.Body.String() == globex.Body.String() {
		t.Error("one tenant received the other's response")
	}
	if again := send("acme"); again.Body.String() != acme.Body.String() || again.Header().Get(idemhttp.ReplayedHeader) != "true" {
		t.Error("a repeat within the tenant was not replayed")
	}

	// A scope that contains the separator must not be able to forge another
	// scope's key.
	send("a/b")
	send("a")
	if got := app.calls.Load(); got != 4 {
		t.Errorf("handler ran %d times, want 4", got)
	}
}

func TestKeySyntax(t *testing.T) {
	t.Run("quoted and bare forms name the same key", func(t *testing.T) {
		app := &orders{}
		h := idemhttp.Middleware(newEngine())(app)
		post(t, h, `"8e03978e-40d5-43e8-bc93-6894a57f9324"`, `{}`)
		post(t, h, `8e03978e-40d5-43e8-bc93-6894a57f9324`, `{}`)
		if got := app.calls.Load(); got != 1 {
			t.Errorf("handler ran %d times, want 1", got)
		}
	})

	for name, key := range map[string]string{
		"too long":      strings.Repeat("k", idemhttp.MaxKeyLength+1),
		"inner space":   "a b",
		"control char":  "a\tb",
		"empty quotes":  `""`,
		"inner quote":   `a"b`,
		"non-ASCII":     "clé",
		"only a quote":  `"`,
		"with a backsl": `a\b`,
	} {
		t.Run(name, func(t *testing.T) {
			app := &orders{}
			h := idemhttp.Middleware(newEngine())(app)
			wantProblem(t, post(t, h, key, `{}`), http.StatusBadRequest)
			if got := app.calls.Load(); got != 0 {
				t.Errorf("handler ran %d times, want 0", got)
			}
		})
	}

	t.Run("longest allowed key", func(t *testing.T) {
		h := idemhttp.Middleware(newEngine())(&orders{})
		if rec := post(t, h, strings.Repeat("k", idemhttp.MaxKeyLength), `{}`); rec.Code != http.StatusCreated {
			t.Errorf("status = %d, want 201", rec.Code)
		}
	})

	t.Run("header sent twice", func(t *testing.T) {
		h := idemhttp.Middleware(newEngine())(&orders{})
		req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{}`))
		req.Header.Add("Idempotency-Key", "a")
		req.Header.Add("Idempotency-Key", "b")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		wantProblem(t, rec, http.StatusBadRequest)
	})

	t.Run("custom header", func(t *testing.T) {
		app := &orders{}
		h := idemhttp.Middleware(newEngine(), idemhttp.Header("X-Request-Token"))(app)
		for range 2 {
			req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{}`))
			req.Header.Set("X-Request-Token", "abc")
			h.ServeHTTP(httptest.NewRecorder(), req)
		}
		if got := app.calls.Load(); got != 1 {
			t.Errorf("handler ran %d times, want 1", got)
		}
	})
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

	t.Run("fails closed by default", func(t *testing.T) {
		app := &orders{}
		var reported error
		h := idemhttp.Middleware(
			idem.New(brokenStore{Store: memstore.New(), acquire: down}),
			idemhttp.OnStoreError(func(_ *http.Request, err error) { reported = err }),
		)(app)
		rec := post(t, h, "key", `{}`)
		if !errors.Is(reported, down) {
			t.Errorf("OnStoreError got %v, want the store's error", reported)
		}
		wantProblem(t, rec, http.StatusServiceUnavailable)
		if rec.Header().Get("Retry-After") == "" {
			t.Error("no Retry-After header")
		}
		if strings.Contains(rec.Body.String(), "10.0.0.7") {
			t.Errorf("the response leaks the store's error: %s", rec.Body)
		}
		if got := app.calls.Load(); got != 0 {
			t.Errorf("handler ran %d times without protection, want 0", got)
		}
	})

	t.Run("fails open on request", func(t *testing.T) {
		app := &orders{}
		var reported error
		h := idemhttp.Middleware(
			idem.New(brokenStore{Store: memstore.New(), acquire: down}),
			idemhttp.FailOpen(),
			idemhttp.OnStoreError(func(_ *http.Request, err error) { reported = err }),
		)(app)
		if rec := post(t, h, "key", `{"sku":"a"}`); rec.Code != http.StatusCreated || !strings.Contains(rec.Body.String(), "sku") {
			t.Fatalf("status = %d, body %s, want the handler's response", rec.Code, rec.Body)
		}
		if !errors.Is(reported, down) {
			t.Errorf("OnStoreError got %v, want the store's error", reported)
		}
	})

	t.Run("response is delivered when recording fails", func(t *testing.T) {
		app := &orders{}
		var reported error
		h := idemhttp.Middleware(
			idem.New(brokenStore{Store: memstore.New(), complete: down}),
			idemhttp.OnStoreError(func(_ *http.Request, err error) { reported = err }),
		)(app)
		if rec := post(t, h, "key", `{}`); rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want the handler's 201: the order exists", rec.Code)
		}
		if !errors.Is(reported, down) {
			t.Errorf("OnStoreError got %v, want the store's error", reported)
		}
	})
}

func TestOnError(t *testing.T) {
	custom := idemhttp.OnError(func(w http.ResponseWriter, r *http.Request, err error) {
		http.Error(w, "custom: "+err.Error(), idemhttp.Status(err))
	})
	h := idemhttp.Middleware(newEngine(), custom, idemhttp.RequireKey())(&orders{})

	rec := post(t, h, "", `{}`)
	if rec.Code != http.StatusBadRequest || !strings.HasPrefix(rec.Body.String(), "custom: ") {
		t.Errorf("got %d %q, want the custom handler's response", rec.Code, rec.Body)
	}
}

func TestStatus(t *testing.T) {
	for want, err := range map[int]error{
		http.StatusBadRequest:            idemhttp.ErrMissingKey,
		http.StatusConflict:              idem.ErrInProgress,
		http.StatusUnprocessableEntity:   idem.ErrFingerprintMismatch,
		http.StatusRequestEntityTooLarge: idemhttp.ErrRequestTooLarge,
		http.StatusInternalServerError:   idemhttp.ErrUnusableRecord,
		http.StatusServiceUnavailable:    errors.New("anything else"),
	} {
		if got := idemhttp.Status(fmt.Errorf("wrapped: %w", err)); got != want {
			t.Errorf("Status(%v) = %d, want %d", err, got, want)
		}
	}
}

func TestOuterMiddlewareHeadersAreNotReplayed(t *testing.T) {
	var requests atomic.Int32
	requestID := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Request-Id", fmt.Sprintf("req-%d", requests.Add(1)))
			next.ServeHTTP(w, r)
		})
	}
	h := requestID(idemhttp.Middleware(newEngine())(&orders{}))

	first := post(t, h, "key", `{}`)
	second := post(t, h, "key", `{}`)
	if first.Header().Get("X-Request-Id") != "req-1" || second.Header().Get("X-Request-Id") != "req-2" {
		t.Errorf("request ids = %q, %q, want each response to carry its own",
			first.Header().Get("X-Request-Id"), second.Header().Get("X-Request-Id"))
	}
	if second.Header().Get("Location") != first.Header().Get("Location") {
		t.Error("the handler's own headers were not replayed")
	}
}

func TestHeadersAreFrozenByWriteHeader(t *testing.T) {
	h := idemhttp.Middleware(newEngine())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Before", "1")
		w.WriteHeader(http.StatusAccepted)
		w.Header().Set("X-After", "1") // too late, as with a plain ResponseWriter
		w.WriteHeader(http.StatusTeapot)
	}))

	for i, rec := range []*httptest.ResponseRecorder{post(t, h, "key", `{}`), post(t, h, "key", `{}`)} {
		if rec.Code != http.StatusAccepted || rec.Header().Get("X-Before") != "1" || rec.Header().Get("X-After") != "" {
			t.Errorf("response %d: %d %v", i+1, rec.Code, rec.Header())
		}
	}
}

func TestHandlerThatWritesNothing(t *testing.T) {
	var calls atomic.Int32
	h := idemhttp.Middleware(newEngine())(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))

	for i := range 2 {
		if rec := post(t, h, "key", `{}`); rec.Code != http.StatusOK || rec.Body.Len() != 0 {
			t.Errorf("response %d: %d %q, want an empty 200", i+1, rec.Code, rec.Body)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1", got)
	}
}

func TestHandlerOutlivesLease(t *testing.T) {
	engine := idem.New(memstore.New(), idem.WithLease(60*time.Millisecond))
	var alive atomic.Bool
	h := idemhttp.Middleware(engine)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(250 * time.Millisecond) // four leases; the heartbeat keeps renewing
		alive.Store(r.Context().Err() == nil)
		w.WriteHeader(http.StatusCreated)
	}))

	if rec := post(t, h, "key", `{}`); rec.Code != http.StatusCreated {
		t.Fatalf("status = %d", rec.Code)
	}
	if !alive.Load() {
		t.Error("the handler's context ended although the lease was being renewed")
	}
	if rec := post(t, h, "key", `{}`); rec.Header().Get(idemhttp.ReplayedHeader) != "true" {
		t.Error("the response of a handler that outlived its lease was not recorded")
	}
}

// lostLease is a store on which every lease extension fails.
type lostLease struct{ idem.Store }

func (lostLease) Extend(context.Context, string, string, time.Duration) error {
	return idem.ErrLeaseLost
}

func TestHandlerContextEndsWhenLeaseIsLost(t *testing.T) {
	engine := idem.New(lostLease{memstore.New()}, idem.WithLease(60*time.Millisecond))
	cause := make(chan error, 1)
	h := idemhttp.Middleware(engine)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			cause <- context.Cause(r.Context())
		case <-time.After(5 * time.Second):
			cause <- errors.New("the context was never cancelled")
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))

	post(t, h, "key", `{}`)
	if err := <-cause; !errors.Is(err, idem.ErrLeaseLost) {
		t.Errorf("handler context cause = %v, want ErrLeaseLost", err)
	}
}

func TestAbortedRequestIsNotStored(t *testing.T) {
	var calls atomic.Int32
	h := idemhttp.Middleware(newEngine())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			// The client hangs up; the handler's database call fails with
			// context.Canceled and the handler returns without an answer.
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{}`)).WithContext(ctx)
	req.Header.Set("Idempotency-Key", "key")
	time.AfterFunc(30*time.Millisecond, cancel)
	h.ServeHTTP(httptest.NewRecorder(), req)

	rec := post(t, h, "key", `{}`)
	if rec.Code != http.StatusCreated || rec.Header().Get(idemhttp.ReplayedHeader) != "" {
		t.Fatalf("retry: %d, replayed %q, want the handler to run and answer 201",
			rec.Code, rec.Header().Get(idemhttp.ReplayedHeader))
	}
}

func TestResponseFinishedAfterClientLeftIsStored(t *testing.T) {
	app := &orders{}
	ctx, cancel := context.WithCancel(context.Background())
	h := idemhttp.Middleware(newEngine())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cancel() // the client is gone, but the order is created all the same
		app.ServeHTTP(w, r)
	}))

	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{}`)).WithContext(ctx)
	req.Header.Set("Idempotency-Key", "key")
	h.ServeHTTP(httptest.NewRecorder(), req)

	rec := post(t, h, "key", `{}`)
	if rec.Code != http.StatusCreated || rec.Header().Get(idemhttp.ReplayedHeader) != "true" {
		t.Fatalf("retry: %d, replayed %q, want the first order replayed", rec.Code, rec.Header().Get(idemhttp.ReplayedHeader))
	}
	if got := app.calls.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1", got)
	}
}

// failingWriter is a ResponseWriter whose connection breaks after the
// headers have gone out.
type failingWriter struct{ header http.Header }

func (w failingWriter) Header() http.Header     { return w.header }
func (failingWriter) WriteHeader(int)           {}
func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("stream reset") }

func TestBrokenConnectionDuringLargeResponse(t *testing.T) {
	var calls atomic.Int32
	h := idemhttp.Middleware(newEngine(), idemhttp.MaxResponseBytes(10))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "12345678")
		_, _ = io.WriteString(w, "90abcdef") // crosses the limit; the write to the client fails
		_, _ = io.WriteString(w, "ghijkl")
	}))

	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{}`))
	req.Header.Set("Idempotency-Key", "key")
	h.ServeHTTP(failingWriter{header: http.Header{}}, req)

	// Whatever the retry gets, it must not be the first eight bytes passed
	// off as the whole response.
	rec := post(t, h, "key", `{}`)
	if rec.Body.String() == "12345678" {
		t.Fatalf("the retry received a truncated body as a complete response: %d %q", rec.Code, rec.Body)
	}
	wantProblem(t, rec, http.StatusConflict)
	if got := calls.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1", got)
	}
}

func TestDeadlineWhileWaitingIsAnswered(t *testing.T) {
	engine := newEngine()
	// "/key" is how the middleware stores the key "key" when no Scope is set.
	held, _, err := engine.Begin(context.Background(), "/key")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release(context.Background())

	app := &orders{}
	h := idemhttp.Middleware(engine, idemhttp.EngineOptions(idem.WithWait(time.Minute)))(app)

	// A timeout middleware further out gives the request 60ms.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{}`)).WithContext(ctx)
	req.Header.Set("Idempotency-Key", "key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	wantProblem(t, rec, http.StatusServiceUnavailable)
	if got := app.calls.Load(); got != 0 {
		t.Errorf("handler ran %d times, want 0", got)
	}
}

func TestRecordWrittenBySomethingElse(t *testing.T) {
	ctx := context.Background()
	engine := newEngine()
	// A queue consumer and this endpoint were given the same engine and the
	// same key by mistake. ("/order-7" is how the middleware stores the key
	// "order-7" when no Scope is set.)
	if _, _, err := idem.Do(ctx, engine, "/order-7", func(context.Context) (map[string]int, error) {
		return map[string]int{"amount": 500}, nil
	}); err != nil {
		t.Fatal(err)
	}

	app := &orders{}
	h := idemhttp.Middleware(engine)(app)
	wantProblem(t, post(t, h, "order-7", `{}`), http.StatusInternalServerError)
	if got := app.calls.Load(); got != 0 {
		t.Errorf("handler ran %d times, want 0", got)
	}
}

type brokenBody struct{}

func (brokenBody) Read([]byte) (int, error) { return 0, errors.New("unexpected EOF") }
func (brokenBody) Close() error             { return nil }

func TestUnreadableBody(t *testing.T) {
	app := &orders{}
	var storeErrors atomic.Int32
	h := idemhttp.Middleware(newEngine(), idemhttp.OnStoreError(func(*http.Request, error) { storeErrors.Add(1) }))(app)

	req := httptest.NewRequest(http.MethodPost, "/orders", brokenBody{})
	req.Header.Set("Idempotency-Key", "key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	wantProblem(t, rec, http.StatusBadRequest)
	if app.calls.Load() != 0 || storeErrors.Load() != 0 {
		t.Errorf("handler ran %d times and %d store errors were reported, want 0 and 0",
			app.calls.Load(), storeErrors.Load())
	}
}

func TestUnscopedKeyCannotReachScopedRecord(t *testing.T) {
	engine := newEngine()
	app := &orders{}
	scoped := idemhttp.Middleware(engine, idemhttp.Scope(func(r *http.Request) string { return "acme" }))(app)
	unscoped := idemhttp.Middleware(engine)(app)

	original := post(t, scoped, "abc", `{}`)
	forged := post(t, unscoped, "acme/abc", `{}`)
	if forged.Header().Get(idemhttp.ReplayedHeader) == "true" || forged.Body.String() == original.Body.String() {
		t.Error("an unscoped key reached a scoped record")
	}
}

func TestPanicInStoreStatusReleasesKey(t *testing.T) {
	var calls atomic.Int32
	h := idemhttp.Middleware(newEngine(), idemhttp.StoreStatus(func(int) bool {
		if calls.Add(1) == 1 {
			panic("bug in callback")
		}
		return true
	}))(&orders{})

	func() {
		defer func() { _ = recover() }()
		post(t, h, "key", `{}`)
	}()
	if rec := post(t, h, "key", `{}`); rec.Code != http.StatusCreated {
		t.Fatalf("status = %d after a panic in the callback, want the retry to run", rec.Code)
	}
}

func TestNilOptionsPanic(t *testing.T) {
	for name, fn := range map[string]func(){
		"engine":       func() { idemhttp.Middleware(nil) },
		"Scope":        func() { idemhttp.Scope(nil) },
		"StoreStatus":  func() { idemhttp.StoreStatus(nil) },
		"OnError":      func() { idemhttp.OnError(nil) },
		"OnStoreError": func() { idemhttp.OnStoreError(nil) },
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

func TestHandlerAbortedByLostLeaseIsNotStored(t *testing.T) {
	var calls atomic.Int32
	engine := idem.New(lostLease{memstore.New()}, idem.WithLease(60*time.Millisecond))
	h := idemhttp.Middleware(engine)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			<-r.Context().Done() // the lease is lost; the handler gives up without an answer
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))

	post(t, h, "key", `{}`)
	rec := post(t, h, "key", `{}`)
	if rec.Code != http.StatusCreated || rec.Header().Get(idemhttp.ReplayedHeader) != "" {
		t.Fatalf("retry: %d, replayed %q, want the handler to run and answer 201",
			rec.Code, rec.Header().Get(idemhttp.ReplayedHeader))
	}
}

func TestStoreTimeoutIsAStoreFailure(t *testing.T) {
	// The store call times out by itself while the request is alive. That is
	// an outage, not a cancelled request: FailOpen applies and it is reported.
	timeout := fmt.Errorf("dial tcp: %w", context.DeadlineExceeded)
	app := &orders{}
	var reported error
	h := idemhttp.Middleware(
		idem.New(brokenStore{Store: memstore.New(), acquire: timeout}),
		idemhttp.FailOpen(),
		idemhttp.OnStoreError(func(_ *http.Request, err error) { reported = err }),
	)(app)

	if rec := post(t, h, "key", `{}`); rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want FailOpen to let the request through", rec.Code)
	}
	if !errors.Is(reported, timeout) {
		t.Errorf("OnStoreError got %v, want the store's timeout", reported)
	}

	h = idemhttp.Middleware(idem.New(brokenStore{Store: memstore.New(), acquire: timeout}))(app)
	rec := post(t, h, "key", `{}`)
	wantProblem(t, rec, http.StatusServiceUnavailable)
	if !strings.Contains(rec.Body.String(), "store unavailable") {
		t.Errorf("problem = %s, want it to name the store", rec.Body)
	}
}

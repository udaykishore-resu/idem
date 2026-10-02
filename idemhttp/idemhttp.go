// Package idemhttp makes HTTP handlers idempotent with the Idempotency-Key
// request header.
//
//	engine := idem.New(store)
//	mux.Handle("POST /orders", idemhttp.Middleware(engine)(createOrder))
//
// A client that is unsure whether its request arrived sends it again with the
// same header value. The first request runs the handler; every repeat gets
// the stored response back, marked with an "Idempotent-Replayed: true"
// header, and the handler does not run again.
//
// # Behaviour
//
// The middleware follows the IETF draft "The Idempotency-Key HTTP Header
// Field" (draft-ietf-httpapi-idempotency-key-header) and reports problems as
// RFC 9457 problem details:
//
//   - No key: the request passes through untouched, or is rejected with 400
//     if [RequireKey] is set.
//   - First request with a key: the handler runs and its response is stored.
//   - Repeat of a completed request: the stored response is replayed.
//   - Repeat while the first is still running: 409 Conflict.
//   - Same key with a different method, path, query or body: 422
//     Unprocessable Content.
//   - The store cannot be reached: 503 Service Unavailable, unless
//     [FailOpen] is set.
//
// A response is stored unless its status tells the client to try again: 408,
// 425, 429 and every 5xx release the key instead, so that the retry runs the
// handler again. Everything else, including other 4xx responses, is the
// outcome of the request and is replayed. [StoreStatus] changes that rule.
//
// By default the middleware acts on POST and PATCH, the methods HTTP does not
// already define as idempotent; see [Methods].
//
// # Keys must be scoped to the caller
//
// A key is chosen by the client, so two clients can pick the same one, by
// accident or on purpose. Unless keys are scoped, the second client receives
// the first client's stored response. Use [Scope] to bind every key to the
// authenticated caller, and install the middleware after authentication:
//
//	idemhttp.Middleware(engine, idemhttp.Scope(func(r *http.Request) string {
//		return auth.TenantID(r.Context())
//	}))
//
// # Limits
//
// The middleware reads the request body into memory to fingerprint it and
// buffers the response to store it, so it is meant for API calls and not for
// uploads or streams. [MaxRequestBytes] and [MaxResponseBytes] set the
// limits. The buffered response writer does not support flushing, hijacking,
// trailers or informational (1xx) responses.
//
// # Sharing a store
//
// The middleware stores whole HTTP responses. Do not let it share keys with
// other users of the same store, such as calls to [idem.Do]: give each its
// own engine with its own [idem.WithNamespace].
package idemhttp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/udaykishore-resu/idem"
)

const (
	// DefaultHeader is the request header that carries the key.
	DefaultHeader = "Idempotency-Key"

	// ReplayedHeader is set to "true" on every replayed response.
	ReplayedHeader = "Idempotent-Replayed"

	// DefaultMaxRequestBytes is the largest request body the middleware reads.
	DefaultMaxRequestBytes = 1 << 20

	// DefaultMaxResponseBytes is the largest response body the middleware
	// stores.
	DefaultMaxResponseBytes = 1 << 20

	// MaxKeyLength is the longest key the middleware accepts.
	MaxKeyLength = 255
)

// Errors passed to the error handler in addition to those of package idem.
var (
	// ErrMissingKey means [RequireKey] is set and the request has no key.
	ErrMissingKey = errors.New("idemhttp: the Idempotency-Key header is missing")

	// ErrMalformedKey means the header was sent more than once, is empty, is
	// longer than [MaxKeyLength], or contains anything other than visible
	// ASCII characters. Double quotes and backslashes are not allowed inside
	// a key either.
	ErrMalformedKey = errors.New("idemhttp: the Idempotency-Key header is malformed")

	// ErrUnreadableBody means reading the request body failed, usually
	// because the client went away while sending it.
	ErrUnreadableBody = errors.New("idemhttp: the request body could not be read")

	// ErrRequestTooLarge means the request body exceeds the limit set with
	// [MaxRequestBytes].
	ErrRequestTooLarge = errors.New("idemhttp: the request body is too large to fingerprint")

	// ErrResponseNotStored means the request completed earlier but its
	// response exceeded [MaxResponseBytes] and cannot be replayed.
	ErrResponseNotStored = errors.New("idemhttp: the request already completed but its response was too large to store")

	// ErrRequestEnded means the request was cancelled or its deadline passed
	// before the handler could start, for example while waiting for a
	// duplicate that was still running. The error wraps the context's error.
	ErrRequestEnded = errors.New("idemhttp: the request ended before it could start")

	// ErrUnusableRecord means the key has a completed record that this
	// middleware cannot replay: it was written by something else that shares
	// the store, or it is damaged. The request is not run again.
	ErrUnusableRecord = errors.New("idemhttp: the stored record for this key is not a response that can be replayed")
)

type options struct {
	header       string
	methods      []string
	requireKey   bool
	failOpen     bool
	scope        func(*http.Request) string
	maxRequest   int64
	maxResponse  int64
	storeStatus  func(int) bool
	onError      func(http.ResponseWriter, *http.Request, error)
	onStoreError func(*http.Request, error)
	engineOpts   []idem.Option
}

// Option configures [Middleware].
type Option func(*options)

// Header sets the request header that carries the key. The default is
// [DefaultHeader].
func Header(name string) Option {
	return func(o *options) { o.header = name }
}

// Methods sets the request methods the middleware acts on; requests with any
// other method pass through. The default is POST and PATCH.
func Methods(methods ...string) Option {
	return func(o *options) { o.methods = methods }
}

// RequireKey rejects requests that have no key with 400 Bad Request, instead
// of letting them through unprotected.
func RequireKey() Option {
	return func(o *options) { o.requireKey = true }
}

// Scope binds every key to the value fn returns for the request, typically
// the authenticated tenant or user, so that callers cannot see each other's
// stored responses. See the package documentation.
func Scope(fn func(*http.Request) string) Option {
	if fn == nil {
		panic("idemhttp: Scope function must not be nil")
	}
	return func(o *options) { o.scope = fn }
}

// MaxRequestBytes sets the largest request body the middleware reads. Larger
// requests are rejected with 413. The default is [DefaultMaxRequestBytes].
func MaxRequestBytes(n int64) Option {
	return func(o *options) { o.maxRequest = n }
}

// MaxResponseBytes sets the largest response body the middleware stores. A
// larger response is still delivered, and the request still counts as
// completed, but a repeat receives 409 instead of the response. The default
// is [DefaultMaxResponseBytes].
func MaxResponseBytes(n int64) Option {
	return func(o *options) { o.maxResponse = n }
}

// StoreStatus decides, from the response status, whether a response is
// stored and replayed (true) or the key is released so that a retry runs the
// handler again (false). The default is [DefaultStoreStatus].
func StoreStatus(fn func(status int) bool) Option {
	if fn == nil {
		panic("idemhttp: StoreStatus function must not be nil")
	}
	return func(o *options) { o.storeStatus = fn }
}

// FailOpen lets requests through unprotected when the store cannot be
// reached, instead of rejecting them with 503. Choose it when serving a
// request twice is better than not serving it.
func FailOpen() Option {
	return func(o *options) { o.failOpen = true }
}

// OnError replaces the default error responses. fn is called with one of the
// errors of this package or of package idem, or with the store's error, and
// must write the complete response. [Status] maps an
// error to the status code the default handler would use.
func OnError(fn func(w http.ResponseWriter, r *http.Request, err error)) Option {
	if fn == nil {
		panic("idemhttp: OnError function must not be nil")
	}
	return func(o *options) { o.onError = fn }
}

// OnStoreError registers a function that is told about every store failure:
// the store could not be reached, or the handler ran but its outcome could
// not be recorded. In the second case the client still receives the handler's
// response. The default error responses never contain the store's error text,
// so this is the place to log it.
func OnStoreError(fn func(r *http.Request, err error)) Option {
	if fn == nil {
		panic("idemhttp: OnStoreError function must not be nil")
	}
	return func(o *options) { o.onStoreError = fn }
}

// EngineOptions passes options such as [idem.WithWait] or [idem.WithTTL] to
// the engine for every request.
func EngineOptions(opts ...idem.Option) Option {
	return func(o *options) { o.engineOpts = append(o.engineOpts, opts...) }
}

// DefaultStoreStatus reports whether a response with the given status is
// stored. It returns false for the statuses that ask the client to retry,
// which are 408 Request Timeout, 425 Too Early, 429 Too Many Requests and
// every 5xx, and true for all others.
func DefaultStoreStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests:
		return false
	}
	return status < http.StatusInternalServerError
}

// Middleware returns a middleware that makes the handlers it wraps
// idempotent, keeping its records through e.
func Middleware(e *idem.Engine, opts ...Option) func(http.Handler) http.Handler {
	if e == nil {
		panic("idemhttp: engine must not be nil")
	}
	o := options{
		header:       DefaultHeader,
		methods:      []string{http.MethodPost, http.MethodPatch},
		maxRequest:   DefaultMaxRequestBytes,
		maxResponse:  DefaultMaxResponseBytes,
		storeStatus:  DefaultStoreStatus,
		onError:      writeProblem,
		onStoreError: func(*http.Request, error) {},
	}
	for _, opt := range opts {
		opt(&o)
	}
	return func(next http.Handler) http.Handler {
		return &handler{engine: e, next: next, options: o}
	}
}

type handler struct {
	engine *idem.Engine
	next   http.Handler
	options
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !slices.Contains(h.methods, r.Method) {
		h.next.ServeHTTP(w, r)
		return
	}
	key, err := parseKey(r.Header.Values(h.header))
	if err != nil {
		h.onError(w, r, err)
		return
	}
	if key == "" {
		if h.requireKey {
			h.onError(w, r, ErrMissingKey)
			return
		}
		h.next.ServeHTTP(w, r)
		return
	}

	body, err := readBody(w, r, h.maxRequest)
	if err != nil {
		h.onError(w, r, err)
		return
	}
	// Stored keys have the form "scope/key". The escaped scope cannot
	// contain a slash, so the first slash always separates the two, and an
	// unscoped key ("/key") can never be mistaken for a scoped one.
	scope := ""
	if h.scope != nil {
		scope = url.PathEscape(h.scope(r))
	}
	key = scope + "/" + key
	fingerprint := idem.Fingerprint([]byte(r.Method), []byte(r.URL.Path), []byte(r.URL.RawQuery), body)
	engineOpts := append([]idem.Option{idem.WithFingerprint(fingerprint)}, h.engineOpts...)

	attempt, stored, err := h.engine.Begin(r.Context(), key, engineOpts...)
	switch {
	case err == nil && attempt == nil:
		h.replay(w, r, stored)
	case err == nil:
		h.run(w, r, attempt)
	case errors.Is(err, idem.ErrInProgress), errors.Is(err, idem.ErrFingerprintMismatch):
		h.onError(w, r, err)
	case r.Context().Err() != nil:
		// The request ended before it could start: the client went away or a
		// deadline passed while waiting. Nothing was run. A store call that
		// timed out on its own, with the request still alive, is a store
		// failure and is handled below.
		h.onError(w, r, fmt.Errorf("%w: %w", ErrRequestEnded, r.Context().Err()))
	case h.failOpen:
		h.onStoreError(r, err)
		h.next.ServeHTTP(w, r)
	default:
		h.onStoreError(r, err)
		h.onError(w, r, err)
	}
}

// run calls the wrapped handler and records what it did.
func (h *handler) run(w http.ResponseWriter, r *http.Request, attempt *idem.Attempt) {
	// Release does nothing once Complete has succeeded. In every other case,
	// including a panic in the handler, it frees the key for a retry.
	defer attempt.Release(r.Context())

	rec := newRecorder(w, h.maxResponse)
	h.next.ServeHTTP(rec, r.WithContext(attempt.Context()))

	if !rec.wrote() && attempt.Context().Err() != nil {
		// The handler's context ended, because the client went away or the
		// lease was lost, and the handler gave up without an answer. That is
		// not an outcome to replay.
		_ = attempt.Release(r.Context())
		rec.flush()
		return
	}
	status := rec.statusCode()
	if !h.storeStatus(status) {
		// Free the key before answering, so that a client which retries the
		// instant it has the response is not turned away.
		_ = attempt.Release(r.Context())
		rec.flush()
		return
	}
	data, err := json.Marshal(storedResponse{
		Version:   recordVersion,
		Status:    status,
		Header:    rec.handlerHeaders(),
		Body:      rec.body(),
		Truncated: rec.overflowed,
	})
	if err == nil {
		// Record first and answer second, for the same reason.
		err = attempt.Complete(r.Context(), data)
	}
	if err != nil {
		h.onStoreError(r, err)
	}
	rec.flush()
}

// replay answers a repeated request from the stored response.
func (h *handler) replay(w http.ResponseWriter, r *http.Request, stored []byte) {
	var resp storedResponse
	if err := json.Unmarshal(stored, &resp); err != nil ||
		resp.Version != recordVersion || resp.Status < 200 || resp.Status > 999 {
		h.onError(w, r, ErrUnusableRecord)
		return
	}
	if resp.Truncated {
		h.onError(w, r, ErrResponseNotStored)
		return
	}
	header := w.Header()
	for name, values := range resp.Header {
		header[name] = values
	}
	header.Set(ReplayedHeader, "true")
	w.WriteHeader(resp.Status)
	_, _ = w.Write(resp.Body)
}

// recordVersion marks records written by this middleware. A record without
// it was written by something else and is never replayed as a response.
const recordVersion = 1

// storedResponse is the record kept for a completed request. It is JSON so
// that records can be inspected in the store with ordinary tools.
type storedResponse struct {
	Version   int         `json:"idemhttp"`
	Status    int         `json:"status"`
	Header    http.Header `json:"header,omitempty"`
	Body      []byte      `json:"body,omitempty"`
	Truncated bool        `json:"truncated,omitempty"`
}

// parseKey extracts the key from the header values. It returns "" if the
// header is absent. The draft defines the value as a quoted Structured Field
// string, while most deployed clients send it bare; both forms are accepted
// and name the same key.
func parseKey(values []string) (string, error) {
	switch len(values) {
	case 0:
		return "", nil
	case 1:
	default:
		return "", ErrMalformedKey
	}
	key := strings.TrimSpace(values[0])
	if len(key) >= 2 && key[0] == '"' && key[len(key)-1] == '"' {
		key = key[1 : len(key)-1]
	}
	if key == "" || len(key) > MaxKeyLength {
		return "", ErrMalformedKey
	}
	for i := 0; i < len(key); i++ {
		if c := key[i]; c <= ' ' || c > '~' || c == '"' || c == '\\' {
			return "", ErrMalformedKey
		}
	}
	return key, nil
}

// readBody reads the request body for fingerprinting and puts it back so the
// handler can read it again.
func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, nil
	}
	if r.ContentLength > limit {
		return nil, ErrRequestTooLarge
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, ErrRequestTooLarge
		}
		return nil, fmt.Errorf("%w: %w", ErrUnreadableBody, err)
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	return body, nil
}

// Status returns the HTTP status code that describes err:
//
//   - 400 for a missing or malformed key and for an unreadable request body
//   - 409 for a request that is still in progress, and for a completed one
//     whose response was too large to keep
//   - 413 for an oversized request body
//   - 422 for a key reused with a different request
//   - 500 for a stored record that cannot be replayed
//   - 503 for everything else: the store failed, or the request was
//     cancelled or timed out before it could start
func Status(err error) int {
	switch {
	case errors.Is(err, ErrMissingKey), errors.Is(err, ErrMalformedKey), errors.Is(err, ErrUnreadableBody):
		return http.StatusBadRequest
	case errors.Is(err, idem.ErrInProgress), errors.Is(err, ErrResponseNotStored):
		return http.StatusConflict
	case errors.Is(err, idem.ErrFingerprintMismatch):
		return http.StatusUnprocessableEntity
	case errors.Is(err, ErrRequestTooLarge):
		return http.StatusRequestEntityTooLarge
	case errors.Is(err, ErrUnusableRecord):
		return http.StatusInternalServerError
	default:
		return http.StatusServiceUnavailable
	}
}

// problem is an RFC 9457 problem details object.
type problem struct {
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
}

func writeProblem(w http.ResponseWriter, _ *http.Request, err error) {
	p := problem{Status: Status(err)}
	switch {
	case errors.Is(err, ErrMissingKey):
		p.Title = "Idempotency-Key is missing"
		p.Detail = "This operation is idempotent and requires the Idempotency-Key header."
	case errors.Is(err, ErrMalformedKey):
		p.Title = "Idempotency-Key is malformed"
		p.Detail = fmt.Sprintf("Send the header once, with a value of 1 to %d visible ASCII characters, excluding double quotes and backslashes.", MaxKeyLength)
	case errors.Is(err, idem.ErrInProgress):
		p.Title = "A request is outstanding for this Idempotency-Key"
		p.Detail = "A request with the same Idempotency-Key is still being processed. Retry once it has finished."
	case errors.Is(err, ErrResponseNotStored):
		p.Title = "Request already completed"
		p.Detail = "A request with this Idempotency-Key completed earlier, but its response was too large to keep. It has not been run again."
	case errors.Is(err, idem.ErrFingerprintMismatch):
		p.Title = "Idempotency-Key is already used"
		p.Detail = "This Idempotency-Key was used with a different request. Use a new key for a new request."
	case errors.Is(err, ErrRequestTooLarge):
		p.Title = "Request body too large"
		p.Detail = "The request body is too large for an idempotent operation."
	case errors.Is(err, ErrUnreadableBody):
		p.Title = "Request body could not be read"
		p.Detail = "The request was not processed. Send it again with the same Idempotency-Key."
	case errors.Is(err, ErrUnusableRecord):
		p.Title = "Stored response cannot be replayed"
		p.Detail = "A request with this Idempotency-Key completed earlier, but its stored response is unusable. It has not been run again."
	case errors.Is(err, ErrRequestEnded):
		p.Title = "Request ended before it started"
		p.Detail = "The request was cancelled or timed out while waiting and was not processed. It is safe to retry with the same Idempotency-Key."
	default:
		// Store errors can carry addresses and credentials; keep them out of
		// the response.
		p.Title = "Idempotency store unavailable"
		p.Detail = "The request was not processed. It is safe to retry with the same Idempotency-Key."
		w.Header().Set("Retry-After", "1")
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

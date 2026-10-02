// Package idemgrpc makes unary gRPC methods idempotent with an
// "idempotency-key" entry in the request metadata.
//
//	engine := idem.New(store)
//	server := grpc.NewServer(
//		grpc.ChainUnaryInterceptor(idemgrpc.UnaryServerInterceptor(engine)),
//	)
//
// A client that is unsure whether its call arrived makes it again with the
// same key. The first call runs the method; every repeat receives the stored
// response, with "idempotent-replayed: true" in the response headers, and
// the method does not run again.
//
// # Behaviour
//
//   - No key: the call passes through untouched, or fails with
//     InvalidArgument if [RequireKey] is set.
//   - First call with a key: the method runs. A successful response is
//     stored; an error stores nothing and releases the key, so that the
//     client's retry runs the method again.
//   - Repeat of a completed call: the stored response is returned.
//   - Repeat while the first is still running: Aborted.
//   - Same key with a different method or request message: InvalidArgument.
//   - The store cannot be reached: Unavailable, unless [FailOpen] is set.
//
// # Keys must be scoped to the caller
//
// A key is chosen by the client, so two clients can pick the same one. Use
// [Scope] to bind every key to the authenticated caller, and place this
// interceptor after the one that authenticates.
//
// # Requirements
//
// Requests and responses must be protobuf messages, and the service must be
// registered in the global protobuf registry, as generated code does on its
// own. The interceptor needs the registry to rebuild a response when it is
// replayed in a process that has not run the method itself.
//
// A retry is recognised by comparing a deterministic encoding of the request
// message. Two builds of a service encode the same message identically unless
// it carries fields that only one of them knows. During a rollout that adds
// fields, a retry that lands on the other build can therefore be rejected
// with InvalidArgument as a different request.
//
// The interceptor stores encoded response messages. Do not let it share keys
// with other users of the same store, such as calls to [idem.Do]: give each
// its own engine with its own [idem.WithNamespace].
//
// This package lives in a module of its own so that only gRPC services pull
// in the gRPC dependency.
package idemgrpc

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/udaykishore-resu/idem"
)

const (
	// DefaultMetadataKey is the request metadata entry that carries the key.
	DefaultMetadataKey = "idempotency-key"

	// ReplayedMetadataKey is set to "true" in the headers of every replayed
	// response.
	ReplayedMetadataKey = "idempotent-replayed"

	// MaxKeyLength is the longest key the interceptor accepts.
	MaxKeyLength = 255
)

type options struct {
	metadataKey  string
	requireKey   bool
	failOpen     bool
	scope        func(context.Context) string
	skip         func(fullMethod string) bool
	onStoreError func(ctx context.Context, fullMethod string, err error)
	engineOpts   []idem.Option
}

// Option configures [UnaryServerInterceptor].
type Option func(*options)

// MetadataKey sets the metadata entry that carries the key. The default is
// [DefaultMetadataKey].
func MetadataKey(name string) Option {
	return func(o *options) { o.metadataKey = strings.ToLower(name) }
}

// RequireKey fails calls that carry no key with InvalidArgument, instead of
// letting them through unprotected.
func RequireKey() Option {
	return func(o *options) { o.requireKey = true }
}

// Scope binds every key to the value fn returns for the call, typically the
// authenticated tenant or user, so that callers cannot receive each other's
// stored responses.
func Scope(fn func(ctx context.Context) string) Option {
	if fn == nil {
		panic("idemgrpc: Scope function must not be nil")
	}
	return func(o *options) { o.scope = fn }
}

// Skip makes the interceptor ignore the methods for which fn returns true.
// fullMethod has the form "/package.Service/Method". Use it for methods that
// are read-only or idempotent by nature.
func Skip(fn func(fullMethod string) bool) Option {
	if fn == nil {
		panic("idemgrpc: Skip function must not be nil")
	}
	return func(o *options) { o.skip = fn }
}

// FailOpen lets calls through unprotected when the store cannot be reached,
// instead of failing them with Unavailable.
func FailOpen() Option {
	return func(o *options) { o.failOpen = true }
}

// OnStoreError registers a function that is told about every store failure:
// the store could not be reached, or the method ran but its outcome could not
// be recorded. The status returned to the client never contains the store's
// error text, so this is the place to log it.
func OnStoreError(fn func(ctx context.Context, fullMethod string, err error)) Option {
	if fn == nil {
		panic("idemgrpc: OnStoreError function must not be nil")
	}
	return func(o *options) { o.onStoreError = fn }
}

// EngineOptions passes options such as [idem.WithWait] or [idem.WithTTL] to
// the engine for every call.
func EngineOptions(opts ...idem.Option) Option {
	return func(o *options) { o.engineOpts = append(o.engineOpts, opts...) }
}

// UnaryServerInterceptor returns an interceptor that makes the unary methods
// of a server idempotent, keeping its records through e.
func UnaryServerInterceptor(e *idem.Engine, opts ...Option) grpc.UnaryServerInterceptor {
	if e == nil {
		panic("idemgrpc: engine must not be nil")
	}
	o := options{
		metadataKey:  DefaultMetadataKey,
		skip:         func(string) bool { return false },
		onStoreError: func(context.Context, string, error) {},
	}
	for _, opt := range opts {
		opt(&o)
	}
	i := &interceptor{engine: e, options: o}
	return i.intercept
}

type interceptor struct {
	engine *idem.Engine
	options
	responses sync.Map // full method name -> protoreflect.MessageType of its response
}

func (i *interceptor) intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if i.skip(info.FullMethod) {
		return handler(ctx, req)
	}
	key, err := parseKey(metadata.ValueFromIncomingContext(ctx, i.metadataKey))
	if err != nil {
		return nil, err
	}
	if key == "" {
		if i.requireKey {
			return nil, status.Errorf(codes.InvalidArgument, "the %s metadata entry is required for this method", i.metadataKey)
		}
		return handler(ctx, req)
	}
	message, ok := req.(proto.Message)
	if !ok {
		return nil, status.Errorf(codes.Internal, "idemgrpc: request %T is not a protobuf message", req)
	}
	// See the package documentation for what deterministic encoding does
	// and does not promise across builds.
	body, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idemgrpc: encode request: %v", err)
	}
	// Stored keys have the form "scope/key". The escaped scope cannot
	// contain a slash, so the first slash always separates the two, and an
	// unscoped key ("/key") can never be mistaken for a scoped one.
	scope := ""
	if i.scope != nil {
		scope = url.PathEscape(i.scope(ctx))
	}
	key = scope + "/" + key
	engineOpts := append([]idem.Option{
		idem.WithFingerprint(idem.Fingerprint([]byte(info.FullMethod), body)),
	}, i.engineOpts...)

	attempt, stored, err := i.engine.Begin(ctx, key, engineOpts...)
	switch {
	case err == nil && attempt == nil:
		return i.replay(ctx, info.FullMethod, stored)
	case err == nil:
		return i.run(ctx, req, info, handler, attempt)
	case errors.Is(err, idem.ErrInProgress):
		return nil, status.Error(codes.Aborted, "a call with this idempotency key is still in progress")
	case errors.Is(err, idem.ErrFingerprintMismatch):
		return nil, status.Error(codes.InvalidArgument, "this idempotency key was used with a different request")
	case ctx.Err() != nil:
		return nil, status.FromContextError(ctx.Err()).Err()
	case i.failOpen:
		i.onStoreError(ctx, info.FullMethod, err)
		return handler(ctx, req)
	default:
		// Store errors can carry addresses and credentials; keep them out of
		// the status message.
		i.onStoreError(ctx, info.FullMethod, err)
		return nil, status.Error(codes.Unavailable, "the idempotency store is unavailable; the call was not processed")
	}
}

// run calls the method and records a successful response.
func (i *interceptor) run(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler, attempt *idem.Attempt) (any, error) {
	// Release does nothing once Complete has succeeded. In every other case,
	// including an error or a panic in the method, it frees the key for a
	// retry.
	defer attempt.Release(ctx)

	resp, err := handler(attempt.Context(), req)
	if err != nil {
		return resp, err
	}
	message, ok := resp.(proto.Message)
	if !ok {
		i.onStoreError(ctx, info.FullMethod, fmt.Errorf("idemgrpc: response %T is not a protobuf message", resp))
		return resp, nil
	}
	i.responses.LoadOrStore(info.FullMethod, message.ProtoReflect().Type())
	data, err := proto.Marshal(message)
	if err == nil {
		err = attempt.Complete(ctx, data)
	}
	if err != nil {
		// The method did its work; the caller gets the response regardless.
		i.onStoreError(ctx, info.FullMethod, err)
	}
	return resp, nil
}

// replay rebuilds the stored response of a completed call.
func (i *interceptor) replay(ctx context.Context, fullMethod string, stored []byte) (any, error) {
	messageType, err := i.responseType(fullMethod)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idemgrpc: %v", err)
	}
	resp := messageType.New().Interface()
	if err := proto.Unmarshal(stored, resp); err != nil {
		return nil, status.Errorf(codes.Internal, "idemgrpc: decode stored response: %v", err)
	}
	_ = grpc.SetHeader(ctx, metadata.Pairs(ReplayedMetadataKey, "true"))
	return resp, nil
}

// responseType returns the response message type of a method. It is known
// from an earlier call in this process, or else looked up in the global
// protobuf registry.
func (i *interceptor) responseType(fullMethod string) (protoreflect.MessageType, error) {
	if t, ok := i.responses.Load(fullMethod); ok {
		return t.(protoreflect.MessageType), nil
	}
	// "/package.Service/Method" is registered as "package.Service.Method".
	name := strings.ReplaceAll(strings.TrimPrefix(fullMethod, "/"), "/", ".")
	desc, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		return nil, fmt.Errorf("method %s is not in the protobuf registry: %w", fullMethod, err)
	}
	method, ok := desc.(protoreflect.MethodDescriptor)
	if !ok {
		return nil, fmt.Errorf("%s is not a method", fullMethod)
	}
	t, err := protoregistry.GlobalTypes.FindMessageByName(method.Output().FullName())
	if err != nil {
		return nil, fmt.Errorf("response type of %s is not in the protobuf registry: %w", fullMethod, err)
	}
	i.responses.Store(fullMethod, t)
	return t, nil
}

// parseKey extracts the key from the metadata values. It returns "" if the
// entry is absent.
func parseKey(values []string) (string, error) {
	switch len(values) {
	case 0:
		return "", nil
	case 1:
	default:
		return "", status.Error(codes.InvalidArgument, "the idempotency key was sent more than once")
	}
	key := strings.TrimSpace(values[0])
	if key == "" || len(key) > MaxKeyLength {
		return "", status.Errorf(codes.InvalidArgument, "the idempotency key must have 1 to %d characters", MaxKeyLength)
	}
	for n := 0; n < len(key); n++ {
		if c := key[n]; c <= ' ' || c > '~' {
			return "", status.Error(codes.InvalidArgument, "the idempotency key must consist of visible ASCII characters")
		}
	}
	return key, nil
}

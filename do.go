package idem

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Codec turns the result of an operation into bytes for the store and back.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// Do runs fn at most once for key and returns its result. If the operation
// already completed, Do does not call fn: it returns the stored result and
// reports replayed as true.
//
// fn receives a context that is cancelled if the lease on the key is lost;
// see [Attempt.Context].
//
// If fn returns an error or panics, nothing is stored and the key is
// released, so the next call with the same key runs fn again. Only successful
// results are replayed.
//
// The result is encoded with the engine's [Codec], JSON by default, so T must
// survive a round trip through it. If fn succeeds but the result cannot be
// encoded or stored, Do returns the result together with an error that wraps
// [ErrNotRecorded].
//
// Do returns [ErrInProgress], [ErrFingerprintMismatch] and [ErrInvalidKey]
// under the conditions described at [Engine.Begin].
func Do[T any](ctx context.Context, e *Engine, key string, fn func(context.Context) (T, error), opts ...Option) (v T, replayed bool, err error) {
	attempt, stored, err := e.Begin(ctx, key, opts...)
	if err != nil {
		return v, false, err
	}
	if attempt == nil {
		if err := e.cfg.codec.Unmarshal(stored, &v); err != nil {
			return v, true, fmt.Errorf("idem: decode stored result: %w", err)
		}
		return v, true, nil
	}

	// Release does nothing once Complete has succeeded. In every other case,
	// including a panic in fn or in the codec, it frees the key for a retry.
	defer attempt.Release(ctx)

	if v, err = fn(attempt.Context()); err != nil {
		return v, false, err
	}
	data, err := e.cfg.codec.Marshal(v)
	if err != nil {
		return v, false, fmt.Errorf("%w: encode result: %w", ErrNotRecorded, err)
	}
	if err := attempt.Complete(ctx, data); err != nil {
		return v, false, fmt.Errorf("%w: %w", ErrNotRecorded, err)
	}
	return v, false, nil
}

// Fingerprint returns a digest of parts for use with [WithFingerprint]. Feed
// it everything that makes two requests different, such as the operation name
// and the request body. Each part is length-prefixed, so ("ab", "c") and
// ("a", "bc") produce different fingerprints.
func Fingerprint(parts ...[]byte) string {
	h := sha256.New()
	var n [8]byte
	for _, p := range parts {
		binary.BigEndian.PutUint64(n[:], uint64(len(p)))
		h.Write(n[:])
		h.Write(p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

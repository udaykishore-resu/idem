package idem

import (
	"context"
	"errors"
	"time"
)

// Errors returned by this package. Test for them with [errors.Is].
var (
	// ErrInProgress means another caller holds the key and has not finished.
	ErrInProgress = errors.New("idem: an operation with this key is already in progress")

	// ErrFingerprintMismatch means the key was first used with a different
	// request. Reusing a key for different work is a client bug.
	ErrFingerprintMismatch = errors.New("idem: key was already used with a different request")

	// ErrLeaseLost means the caller no longer holds the key: its lease ran
	// out, or another caller has acquired the key since.
	ErrLeaseLost = errors.New("idem: lease lost")

	// ErrInvalidKey means the key was empty.
	ErrInvalidKey = errors.New("idem: invalid key")

	// ErrNotRecorded means the operation finished but its result could not be
	// stored, so a retry with the same key may run the operation again. The
	// value returned alongside it is the real result of the operation.
	ErrNotRecorded = errors.New("idem: operation finished but its result was not recorded")
)

// State is what [Store.Acquire] found under a key.
type State int

const (
	// StateAcquired means the key was free and the caller now holds it.
	StateAcquired State = iota + 1
	// StateInProgress means another caller holds an unexpired lease.
	StateInProgress
	// StateCompleted means the operation already finished and its result is
	// stored.
	StateCompleted
)

// String returns the state's name.
func (s State) String() string {
	switch s {
	case StateAcquired:
		return "acquired"
	case StateInProgress:
		return "in progress"
	case StateCompleted:
		return "completed"
	default:
		return "unknown"
	}
}

// Claim is the outcome of [Store.Acquire].
type Claim struct {
	// State says whether the caller acquired the key and, if it did not,
	// what holds it.
	State State

	// Fingerprint is the fingerprint stored with the existing record. It is
	// set when State is StateInProgress or StateCompleted.
	Fingerprint string

	// Result is the stored result. It is set when State is StateCompleted.
	Result []byte
}

// Store keeps one record per key. Implementations must be safe for concurrent
// use, and every method must be atomic with respect to the others, including
// across processes that share the store.
//
// A record is live until its expiry time. An expired record must behave
// exactly as if it were absent.
//
// Package storetest verifies an implementation against this contract.
type Store interface {
	// Acquire claims key if it has no live record: it stores an in-progress
	// record holding token and fingerprint that expires after lease, and
	// reports StateAcquired.
	//
	// If key has a live record, Acquire leaves it unchanged and reports
	// StateInProgress or StateCompleted together with the stored fingerprint
	// and, for a completed record, the stored result.
	//
	// When several callers race for a free key, exactly one acquires it.
	Acquire(ctx context.Context, key, token, fingerprint string, lease time.Duration) (Claim, error)

	// Complete turns the live in-progress record held by token into a
	// completed record that stores result and expires after ttl.
	//
	// Calling Complete again with the same token on the completed record is a
	// no-op that returns nil, so the call can be retried. In every other case
	// Complete changes nothing and returns ErrLeaseLost.
	Complete(ctx context.Context, key, token string, result []byte, ttl time.Duration) error

	// Release removes the live in-progress record held by token, so that the
	// key can be acquired again. In every other case it changes nothing and
	// returns ErrLeaseLost.
	Release(ctx context.Context, key, token string) error

	// Extend moves the expiry of the live in-progress record held by token to
	// lease from now. In every other case it changes nothing and returns
	// ErrLeaseLost.
	Extend(ctx context.Context, key, token string, lease time.Duration) error
}

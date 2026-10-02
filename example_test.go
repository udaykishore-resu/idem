package idem_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/udaykishore-resu/idem"
	"github.com/udaykishore-resu/idem/memstore"
)

type Receipt struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

// A client retries a payment three times because its connection keeps
// dropping. The card is charged once.
func ExampleDo() {
	ctx := context.Background()
	engine := idem.New(memstore.New())

	charges := 0
	charge := func(ctx context.Context) (Receipt, error) {
		charges++
		return Receipt{ID: fmt.Sprintf("rcpt_%d", charges), Amount: 500}, nil
	}

	for try := 1; try <= 3; try++ {
		receipt, replayed, err := idem.Do(ctx, engine, "payment-42", charge)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		fmt.Printf("try %d: %s replayed=%t\n", try, receipt.ID, replayed)
	}
	fmt.Println("charges made:", charges)

	// Output:
	// try 1: rcpt_1 replayed=false
	// try 2: rcpt_1 replayed=true
	// try 3: rcpt_1 replayed=true
	// charges made: 1
}

// A queue that delivers at least once will sooner or later deliver a message
// twice. Using the message ID as the key turns the redelivery into a no-op.
func ExampleDo_consumer() {
	ctx := context.Background()
	engine := idem.New(memstore.New(), idem.WithNamespace("welcome-email"))

	type message struct{ ID, Email string }
	sent := 0
	handle := func(m message) error {
		_, _, err := idem.Do(ctx, engine, m.ID, func(ctx context.Context) (struct{}, error) {
			sent++ // send the email
			return struct{}{}, nil
		})
		return err
	}

	deliveries := []message{
		{ID: "msg-1", Email: "ada@example.com"},
		{ID: "msg-2", Email: "alan@example.com"},
		{ID: "msg-1", Email: "ada@example.com"}, // redelivered
	}
	for _, m := range deliveries {
		if err := handle(m); err != nil {
			fmt.Println("error:", err)
		}
	}
	fmt.Println("emails sent:", sent)

	// Output:
	// emails sent: 2
}

// A failed operation is not remembered: the key is released and the retry
// runs it again.
func ExampleDo_failure() {
	ctx := context.Background()
	engine := idem.New(memstore.New())

	tries := 0
	reserve := func(ctx context.Context) (string, error) {
		tries++
		if tries == 1 {
			return "", errors.New("inventory service timed out")
		}
		return "reservation-7", nil
	}

	for range 3 {
		v, replayed, err := idem.Do(ctx, engine, "cart-9", reserve)
		fmt.Printf("%q replayed=%t err=%v\n", v, replayed, err)
	}

	// Output:
	// "" replayed=false err=inventory service timed out
	// "reservation-7" replayed=false err=<nil>
	// "reservation-7" replayed=true err=<nil>
}

// A key identifies one request. Presenting it with a different request is
// rejected instead of answered with the wrong result.
func ExampleWithFingerprint() {
	ctx := context.Background()
	engine := idem.New(memstore.New())
	transfer := func(ctx context.Context) (string, error) { return "ok", nil }

	original := idem.Fingerprint([]byte("transfer"), []byte(`{"to":"alice","amount":100}`))
	_, _, err := idem.Do(ctx, engine, "key-1", transfer, idem.WithFingerprint(original))
	fmt.Println("original:", err)

	changed := idem.Fingerprint([]byte("transfer"), []byte(`{"to":"mallory","amount":100}`))
	_, _, err = idem.Do(ctx, engine, "key-1", transfer, idem.WithFingerprint(changed))
	fmt.Println("changed: ", err)

	// Output:
	// original: <nil>
	// changed:  idem: key was already used with a different request
}

// By default a caller that finds the key held gets ErrInProgress at once.
// With WithWait it waits for the holder and takes over its result.
func ExampleWithWait() {
	ctx := context.Background()
	engine := idem.New(memstore.New())

	started := make(chan struct{})
	go func() {
		_, _, _ = idem.Do(ctx, engine, "report-2024", func(ctx context.Context) (string, error) {
			close(started)
			time.Sleep(50 * time.Millisecond)
			return "report built by the first caller", nil
		})
	}()
	<-started

	build := func(ctx context.Context) (string, error) { return "report built by the second caller", nil }

	_, _, err := idem.Do(ctx, engine, "report-2024", build)
	fmt.Println("without waiting:", err)

	v, replayed, err := idem.Do(ctx, engine, "report-2024", build, idem.WithWait(5*time.Second))
	fmt.Println("with waiting:", v, replayed, err)

	// Output:
	// without waiting: idem: an operation with this key is already in progress
	// with waiting: report built by the first caller true <nil>
}

// Begin is for results that are not a single Go value. The caller holds an
// Attempt while it works and ends it with Complete or Release.
func ExampleEngine_Begin() {
	ctx := context.Background()
	engine := idem.New(memstore.New())

	export := func() {
		attempt, stored, err := engine.Begin(ctx, "export-7")
		switch {
		case err != nil:
			fmt.Println("error:", err)
		case attempt == nil:
			fmt.Printf("already done: %s\n", stored)
		default:
			// Release frees the key if anything below fails. Once Complete
			// has succeeded it does nothing.
			defer attempt.Release(ctx)

			// Work with attempt.Context(), then record what happened.
			if err := attempt.Complete(ctx, []byte("s3://exports/7.csv")); err != nil {
				fmt.Println("error:", err)
				return
			}
			fmt.Println("exported")
		}
	}
	export()
	export()

	// Output:
	// exported
	// already done: s3://exports/7.csv
}

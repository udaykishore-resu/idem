// Command orders is a small HTTP service that shows idem at work. Start it,
// send the same order twice, and watch the second request come back as a
// replay without creating a second order.
//
//	go run ./examples/orders
//
//	curl -i -X POST localhost:8080/orders \
//	     -H 'Idempotency-Key: 8e03978e-40d5-43e8-bc93-6894a57f9324' \
//	     -d '{"sku":"book","qty":1}'
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/udaykishore-resu/idem"
	"github.com/udaykishore-resu/idem/idemhttp"
	"github.com/udaykishore-resu/idem/memstore"
)

type order struct {
	ID      string          `json:"id"`
	Request json.RawMessage `json:"request"`
	Created time.Time       `json:"created"`
}

// shop stands in for a real database.
type shop struct {
	mu     sync.Mutex
	orders []order
}

func (s *shop) create(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil || !json.Valid(body) {
		http.Error(w, "the body must be JSON", http.StatusBadRequest)
		return
	}
	time.Sleep(300 * time.Millisecond) // pretend to call a payment provider

	s.mu.Lock()
	o := order{ID: fmt.Sprintf("ord_%04d", len(s.orders)+1), Request: body, Created: time.Now().UTC()}
	s.orders = append(s.orders, o)
	s.mu.Unlock()
	log.Printf("created %s", o.ID)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", "/orders/"+o.ID)
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(o)
}

func (s *shop) list(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"count": len(s.orders), "orders": s.orders})
}

func main() {
	addr := flag.String("addr", "localhost:8080", "address to listen on")
	flag.Parse()

	// memstore is enough for one process. A service with several replicas
	// uses redisstore or pgstore here; nothing else changes.
	engine := idem.New(memstore.New(), idem.WithTTL(time.Hour))
	idempotent := idemhttp.Middleware(engine,
		// A concurrent duplicate waits for the first request instead of
		// getting 409 at once.
		idemhttp.EngineOptions(idem.WithWait(5*time.Second)),
		idemhttp.OnStoreError(func(r *http.Request, err error) {
			log.Printf("idempotency store: %v", err)
		}),
	)

	s := &shop{}
	mux := http.NewServeMux()
	mux.Handle("POST /orders", idempotent(http.HandlerFunc(s.create)))
	mux.HandleFunc("GET /orders", s.list)

	log.Printf("listening on http://%s", *addr)
	log.Printf(`try: curl -i -X POST %s/orders -H 'Idempotency-Key: demo-1' -d '{"sku":"book","qty":1}'`, *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

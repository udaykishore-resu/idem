package idemhttp_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/udaykishore-resu/idem"
	"github.com/udaykishore-resu/idem/idemhttp"
	"github.com/udaykishore-resu/idem/memstore"
)

func ExampleMiddleware() {
	engine := idem.New(memstore.New())

	created := 0
	createOrder := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		created++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"order":%d}`, created)
	})

	mux := http.NewServeMux()
	mux.Handle("POST /orders", idemhttp.Middleware(engine)(createOrder))
	server := httptest.NewServer(mux)
	defer server.Close()

	// The client sends the same request twice, as it would after a timeout.
	for range 2 {
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/orders", strings.NewReader(`{"sku":"book"}`))
		req.Header.Set("Idempotency-Key", "8e03978e-40d5-43e8-bc93-6894a57f9324")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		fmt.Printf("%d %s replayed=%q\n", resp.StatusCode, body, resp.Header.Get("Idempotent-Replayed"))
	}
	fmt.Println("orders created:", created)

	// Output:
	// 201 {"order":1} replayed=""
	// 201 {"order":1} replayed="true"
	// orders created: 1
}

// Keys are chosen by clients, so bind them to the authenticated caller. Here
// the tenant comes from a header for brevity; in a real service it comes from
// the request's verified credentials.
func ExampleScope() {
	engine := idem.New(memstore.New())

	perTenant := idemhttp.Scope(func(r *http.Request) string {
		return r.Header.Get("X-Tenant")
	})
	orders := 0
	handler := idemhttp.Middleware(engine, perTenant, idemhttp.RequireKey())(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			orders++
			fmt.Fprintf(w, "order %d for %s", orders, r.Header.Get("X-Tenant"))
		}))

	send := func(tenant, key string) {
		req := httptest.NewRequest(http.MethodPost, "/orders", nil)
		req.Header.Set("X-Tenant", tenant)
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			fmt.Println(tenant+":", rec.Code)
			return
		}
		fmt.Println(tenant+":", rec.Body.String())
	}
	send("acme", "1")
	send("globex", "1") // same key, different tenant: a different request
	send("acme", "1")   // a retry
	send("acme", "")    // RequireKey rejects requests without a key

	// Output:
	// acme: order 1 for acme
	// globex: order 2 for globex
	// acme: order 1 for acme
	// acme: 400
}

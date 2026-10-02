package idemgrpc_test

import (
	"context"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/udaykishore-resu/idem"
	"github.com/udaykishore-resu/idem/idemgrpc"
	"github.com/udaykishore-resu/idem/memstore"
)

func ExampleUnaryServerInterceptor() {
	engine := idem.New(memstore.New())

	server := grpc.NewServer(grpc.ChainUnaryInterceptor(
		// Authentication goes first, so that Scope can rely on it.
		idemgrpc.UnaryServerInterceptor(engine,
			idemgrpc.Scope(tenantFromContext),
			// A duplicate that arrives while the first call is running waits
			// for its result instead of failing with Aborted.
			idemgrpc.EngineOptions(idem.WithWait(5*time.Second)),
			idemgrpc.OnStoreError(func(_ context.Context, method string, err error) {
				log.Printf("idempotency store, %s: %v", method, err)
			}),
		),
	))
	// Register your services on server here.

	listener, err := net.Listen("tcp", "localhost:50051")
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(server.Serve(listener))
}

// tenantFromContext stands in for whatever your authentication interceptor
// stores in the context.
func tenantFromContext(ctx context.Context) string {
	if v := metadata.ValueFromIncomingContext(ctx, "x-tenant"); len(v) > 0 {
		return v[0]
	}
	return ""
}

// On the client, the key travels in the outgoing metadata. Generate it once
// per logical operation and reuse it for every retry of that operation.
func Example_client() {
	ctx := metadata.AppendToOutgoingContext(context.Background(),
		idemgrpc.DefaultMetadataKey, "8e03978e-40d5-43e8-bc93-6894a57f9324")

	_ = ctx // pass ctx to the generated client: client.CreateOrder(ctx, req)
}

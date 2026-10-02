# Addresses of the servers started by `make services`. Override them to test
# against servers of your own.
IDEM_REDIS_ADDR   ?= localhost:6379
IDEM_POSTGRES_DSN ?= postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable

export IDEM_REDIS_ADDR
export IDEM_POSTGRES_DSN

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: run
run: ## Start the example service on localhost:8080
	go run ./examples/orders

.PHONY: test
test: ## Unit tests with the race detector (no servers needed)
	env -u IDEM_REDIS_ADDR go test -race -count=1 ./...

.PHONY: check
check: ## gofmt and go vet for all three modules
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed on:"; gofmt -l .; exit 1)
	go vet ./...
	cd idemgrpc && go vet ./...
	cd integration && go vet ./...

.PHONY: services
services: ## Start Redis and PostgreSQL with Docker Compose
	docker compose up -d --wait

.PHONY: services-down
services-down: ## Stop them again
	docker compose down

.PHONY: integration
integration: ## Stores against real servers, plus the gRPC tests
	go test -race -count=1 ./redisstore
	cd integration && go test -race -count=1 ./...
	cd idemgrpc && go test -race -count=1 ./...

.PHONY: tidy
tidy: ## go mod tidy in all three modules
	go mod tidy
	cd idemgrpc && go mod tidy
	cd integration && go mod tidy

.PHONY: cover
cover: ## Coverage of the root module
	go test -count=1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1
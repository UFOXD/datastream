.PHONY: build test clean lint fmt generate-parsers

APP_NAME = datastream
VERSION = $(shell git describe --tags --always 2>/dev/null || echo "dev")
BUILD_TIME = $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS = -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)"

# Go files
GOFMT_FILES ?= $(shell find . -name '*.go' 2>/dev/null)

build:
	go build $(LDFLAGS) -o bin/$(APP_NAME) ./cmd/$(APP_NAME)

build-ctl:
	go build $(LDFLAGS) -o bin/$(APP_NAME)-ctl ./cmd/$(APP_NAME)-ctl

test:
	go test -v -race -coverprofile=coverage.out ./...

test-coverage: test
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

test-integration:
	INTEGRATION_TEST=1 go test -v -tags=integration ./tests/integration/...

test-e2e:
	go test -v -tags=e2e ./tests/e2e/...

clean:
	rm -rf bin/
	rm -f coverage.out coverage.html

lint:
	golangci-lint run

fmt:
	gofmt -w $(GOFMT_FILES)

fmt-check:
	@unformatted=$$(gofmt -l $(GOFMT_FILES)); \
	if [ -n "$$unformatted" ]; then \
		echo "Unformatted files:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

proto:
	protoc --go_out=. --go-grpc_out=. proto/*.proto

deps:
	go mod download
	go mod tidy

# Generate ANTLR parsers for all supported databases
generate-parsers:
	@chmod +x scripts/generate-parsers.sh
	@./scripts/generate-parsers.sh

.PHONY: all
all: deps build test

# Integration tests
.PHONY: test-integration-up test-integration-down test-integration test-integration-mysql test-integration-postgres test-integration-sqlserver test-integration-oracle

test-integration-up:
	docker-compose -f tests/docker/docker-compose.yml up -d
	@echo "Waiting for databases to be ready..."
	@sleep 10

test-integration-down:
	docker-compose -f tests/docker/docker-compose.yml down -v

test-integration: test-integration-up
	go test -v -tags=integration -timeout 10m ./tests/integration/...
	$(MAKE) test-integration-down

test-integration-mysql: test-integration-up
	go test -v -tags=integration -timeout 5m ./tests/integration/... -run MySQL
	$(MAKE) test-integration-down

test-integration-postgres: test-integration-up
	go test -v -tags=integration -timeout 5m ./tests/integration/... -run Postgres
	$(MAKE) test-integration-down

test-integration-sqlserver: test-integration-up
	go test -v -tags=integration -timeout 5m ./tests/integration/... -run SQLServer
	$(MAKE) test-integration-down

test-integration-oracle: test-integration-up
	go test -v -tags=integration -timeout 5m ./tests/integration/... -run Oracle
	$(MAKE) test-integration-down

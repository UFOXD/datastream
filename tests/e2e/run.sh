#!/bin/bash

# E2E test runner script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Run e2e tests
run_tests() {
    log_info "Running e2e tests..."
    cd "$PROJECT_ROOT"

    # Start services first
    "$SCRIPT_DIR/../integration/run.sh" start

    # Build the server
    log_info "Building datastream server..."
    go build -o bin/datastream ./cmd/datastream

    # Start the server in background
    log_info "Starting datastream server..."
    ./bin/datastream --config configs/datastream.toml &
    SERVER_PID=$!

    # Wait for server to be ready
    log_info "Waiting for server to be ready..."
    for i in {1..30}; do
        if curl -s http://localhost:8300/health > /dev/null 2>&1; then
            log_info "Server is ready"
            break
        fi
        sleep 1
    done

    # Run e2e tests
    log_info "Running e2e tests..."
    go test -v -tags=e2e -count=1 ./tests/e2e/...
    TEST_RESULT=$?

    # Stop server
    log_info "Stopping server..."
    kill $SERVER_PID 2>/dev/null || true

    if [ $TEST_RESULT -eq 0 ]; then
        log_info "All e2e tests passed!"
    else
        log_error "E2E tests failed!"
        exit 1
    fi
}

# Main
main() {
    case "${1:-run}" in
        run)
            run_tests
            ;;
        *)
            echo "Usage: $0 run"
            echo "  run   - Run e2e tests"
            exit 1
            ;;
    esac
}

main "$@"

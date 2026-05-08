#!/bin/bash

# Integration test runner script

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

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if docker is available
check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi

    if ! docker info &> /dev/null; then
        log_error "Docker is not running"
        exit 1
    fi
}

# Start services
start_services() {
    log_info "Starting integration test services..."
    docker-compose -f "$SCRIPT_DIR/docker-compose.yml" up -d

    log_info "Waiting for services to be ready..."
    sleep 10

    # Wait for MySQL
    log_info "Waiting for MySQL..."
    for i in {1..30}; do
        if docker exec datastream-mysql mysqladmin ping -h localhost &> /dev/null; then
            log_info "MySQL is ready"
            break
        fi
        sleep 1
    done

    # Wait for PostgreSQL
    log_info "Waiting for PostgreSQL..."
    for i in {1..30}; do
        if docker exec datastream-postgres pg_isready -U datastream &> /dev/null; then
            log_info "PostgreSQL is ready"
            break
        fi
        sleep 1
    done

    # Wait for Kafka
    log_info "Waiting for Kafka..."
    for i in {1..30}; do
        if docker exec datastream-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &> /dev/null; then
            log_info "Kafka is ready"
            break
        fi
        sleep 1
    done

    # Wait for etcd
    log_info "Waiting for etcd..."
    for i in {1..30}; do
        if docker exec datastream-etcd etcdctl endpoint health &> /dev/null; then
            log_info "etcd is ready"
            break
        fi
        sleep 1
    done
}

# Stop services
stop_services() {
    log_info "Stopping integration test services..."
    docker-compose -f "$SCRIPT_DIR/docker-compose.yml" down -v
}

# Run tests
run_tests() {
    log_info "Running integration tests..."
    cd "$PROJECT_ROOT"

    INTEGRATION_TEST=1 go test -v -tags=integration -count=1 ./tests/integration/...

    if [ $? -eq 0 ]; then
        log_info "All integration tests passed!"
    else
        log_error "Integration tests failed!"
        exit 1
    fi
}

# Main
main() {
    case "${1:-run}" in
        start)
            check_docker
            start_services
            ;;
        stop)
            stop_services
            ;;
        run)
            check_docker
            start_services
            run_tests
            ;;
        test)
            run_tests
            ;;
        *)
            echo "Usage: $0 {start|stop|run|test}"
            echo "  start - Start services"
            echo "  stop  - Stop services"
            echo "  run   - Start services and run tests (default)"
            echo "  test  - Run tests only (services must be running)"
            exit 1
            ;;
    esac
}

main "$@"

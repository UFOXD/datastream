# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git make

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binaries
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o bin/datastream ./cmd/datastream
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o bin/datastream-ctl ./cmd/datastream-ctl

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 datastream && \
    adduser -u 1000 -G datastream -s /bin/sh -D datastream

# Copy binaries from builder
COPY --from=builder /app/bin/datastream /app/bin/datastream
COPY --from=builder /app/bin/datastream-ctl /app/bin/datastream-ctl

# Copy default config
COPY configs/datastream.toml /app/configs/datastream.toml

# Create directories
RUN mkdir -p /data /var/log/datastream && \
    chown -R datastream:datastream /app /data /var/log/datastream

USER datastream

EXPOSE 8300

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q --spider http://localhost:8300/health || exit 1

ENTRYPOINT ["/app/bin/datastream"]
CMD ["--config", "/app/configs/datastream.toml"]

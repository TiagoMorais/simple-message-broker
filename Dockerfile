# Build stage
FROM golang:1.23-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum* ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the binary with optimizations
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o message-broker \
    main.go

# Runtime stage
FROM alpine:latest

# Install ca-certificates for HTTPS support
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN addgroup -g 1000 broker && \
    adduser -D -u 1000 -G broker broker

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/message-broker .

# Create WAL directory with proper permissions
RUN mkdir -p /app/wal && chown -R broker:broker /app

# Switch to non-root user
USER broker

# Expose port
EXPOSE 8080

# Run the broker
CMD ["./message-broker"]

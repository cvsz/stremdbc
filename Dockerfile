FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o stremdbc ./cmd/stremdbc

# Final stage
FROM alpine:3.19

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata ffmpeg

# Create non-root user
RUN addgroup -g 1000 stremdbc && \
    adduser -D -u 1000 -G stremdbc stremdbc

# Copy binary from builder
COPY --from=builder /app/stremdbc .
COPY --from=builder /app/configs ./configs

# Create directories for HLS and recordings
RUN mkdir -p /tmp/hls /tmp/recordings && \
    chown -R stremdbc:stremdbc /tmp/hls /tmp/recordings /app

# Switch to non-root user
USER stremdbc

# Expose ports
EXPOSE 8080 1935

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["./stremdbc"]
CMD ["-config", "configs/config.yaml"]

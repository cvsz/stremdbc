FROM golang:1.27.1-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/stremdbc ./cmd/stremdbc

FROM alpine:3.24

WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata ffmpeg wget && \
    addgroup -g 1000 stremdbc && \
    adduser -D -u 1000 -G stremdbc stremdbc

COPY --from=builder /out/stremdbc /app/stremdbc
COPY --from=builder /src/configs /app/configs
COPY --from=builder /src/web /app/web

RUN mkdir -p /tmp/hls /tmp/llhls /tmp/recordings /tmp/dvr && \
    chown -R stremdbc:stremdbc /tmp/hls /tmp/llhls /tmp/recordings /tmp/dvr /app

USER stremdbc

EXPOSE 8080/tcp
EXPOSE 1935/tcp
EXPOSE 8554/tcp
EXPOSE 8443/tcp
EXPOSE 1936/tcp
EXPOSE 8555/tcp
EXPOSE 9000/udp
EXPOSE 9001/udp

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:8080/health || exit 1

ENTRYPOINT ["/app/stremdbc"]
CMD ["-config", "/app/configs/config.yaml"]

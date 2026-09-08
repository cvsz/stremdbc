# STREMDBC

Self-hosted streaming media server and management control plane written in Go. STREMDBC provides a single-ingest / multi-output architecture, protocol adapters, HLS/LL-HLS delivery, WebRTC signaling, FFmpeg transcoding, recording/DVR, authentication, Prometheus telemetry, Redis-backed cluster coordination, and a built-in management dashboard.

## Current status

**Version:** `0.6.0`  
**Roadmap implementation:** Phases 1-6 completed in source  
**Release validation:** CI, CodeQL, container build, protocol conformance, and endurance/load testing

> "Implemented" means the component is present, configured, wired into the runtime lifecycle, and covered by automated build/test gates where practical. Production certification still requires real publisher/player interoperability and load testing in the target environment.

## Architecture

```text
 RTMP / RTSP / SRT / WebRTC
              │
              ▼
      ┌─────────────────┐
      │ STREMDBC CORE   │
      │ stream registry │
      │ auth / metrics  │
      │ media pipeline  │
      └────────┬────────┘
               │
     ┌─────────┼───────────┐
     ▼         ▼           ▼
 HLS/LL-HLS  WebRTC   RTMP/RTSP/SRT
     │                       │
     └──────────┬────────────┘
                ▼
       Player / Dashboard
                │
       Prometheus / Cluster
```

## Roadmap completion

### Phase 1 — Media Server Foundation ✅
- [x] Go project structure
- [x] Configuration and structured logging
- [x] Health/readiness API
- [x] Thread-safe stream registry
- [x] RTMP ingest baseline
- [x] HLS output manager
- [x] Built-in player
- [x] Docker packaging
- [x] Prometheus metrics

### Phase 2 — Professional Protocols ✅
- [x] RTSP ingest adapter
- [x] SRT ingest adapter
- [x] WebRTC signaling/ingest adapter
- [x] RTMP output adapter
- [x] RTSP output adapter
- [x] SRT output adapter
- [x] LL-HLS manager
- [x] Runtime configuration and lifecycle wiring for all adapters

Protocol-level certification against multiple third-party clients remains a release-validation task.

### Phase 3 — Transcoding ✅
- [x] FFmpeg worker pool
- [x] Configurable ABR ladder
- [x] Cancellable transcoding jobs
- [x] Hardware encoder discovery with CPU fallback
- [x] Per-component transcoder telemetry

### Phase 4 — Production Controls ✅
- [x] API-key authorization
- [x] HS256 JWT publish/play tokens
- [x] JWT issuer/expiry/signing-method validation
- [x] Recording manager
- [x] DVR lifecycle and retention cleanup
- [x] Prometheus metrics
- [x] HTTP security headers and configurable CORS
- [x] Graceful HTTP shutdown

Authentication is disabled in the default sample configuration. Enable it only after setting a JWT secret of at least 32 characters and production API keys.

### Phase 5 — Cluster Runtime ✅
- [x] Redis-backed node registration and discovery
- [x] Node heartbeat and health state
- [x] Load-aware node selection primitives
- [x] Cluster configuration validation
- [x] Redis and PostgreSQL deployment profiles in Docker Compose
- [x] Persistent cluster-service volumes

Redis is the active coordination store. PostgreSQL is provisioned as the metadata persistence substrate for deployments that need durable external metadata; application-specific schema integration can be added without changing the media runtime contract.

### Phase 6 — Management Platform ✅
- [x] Built-in management dashboard at `/dashboard/`
- [x] Live stream inventory
- [x] Viewer/runtime telemetry
- [x] Component statistics API
- [x] Player and HLS launch links
- [x] Prometheus endpoint integration

The dashboard is intentionally zero-build and framework-free so the production image remains a single Go service plus static assets.

## Quick start

### Docker Compose

```bash
docker compose up -d --build
```

Open:

- Dashboard: `http://localhost:8080/dashboard/`
- Health: `http://localhost:8080/health`
- Readiness: `http://localhost:8080/ready`
- Metrics: `http://localhost:8080/metrics`

Optional services:

```bash
# Prometheus + Grafana
docker compose --profile monitoring up -d

# Redis + PostgreSQL cluster substrate
docker compose --profile cluster up -d
```

### Local Go build

Requires Go 1.27+ and FFmpeg when recording/transcoding is enabled.

```bash
go test ./...
go build -o stremdbc ./cmd/stremdbc
./stremdbc -config configs/config.dev.yaml
```

## Publishing and playback

### RTMP ingest

```text
Server:     rtmp://localhost:1935/live
Stream key: mystream
```

### HLS playback

```text
http://localhost:8080/hls/mystream/index.m3u8
```

### Built-in player

```text
http://localhost:8080/player/mystream
```

## Management API

```http
GET    /health
GET    /ready
GET    /metrics
GET    /api/v1/info
GET    /api/v1/stats
GET    /api/v1/streams
POST   /api/v1/streams
GET    /api/v1/streams/:id
DELETE /api/v1/streams/:id
POST   /api/v1/auth/token
```

When authentication is enabled, mutating API calls require `X-API-Key`. The token endpoint creates stream-scoped `publish` or `play` JWTs.

See [`docs/API.md`](docs/API.md) for request/response details.

## Ports

| Port | Transport | Purpose |
|---:|:---:|---|
| 8080 | TCP | API, dashboard, HLS/LL-HLS static delivery |
| 1935 | TCP | RTMP ingest |
| 8554 | TCP | RTSP ingest |
| 9000 | UDP | SRT ingest |
| 8443 | TCP | WebRTC signaling |
| 1936 | TCP | RTMP output adapter (optional) |
| 8555 | TCP | RTSP output adapter (optional) |
| 9001 | UDP | SRT output adapter (optional) |

## Security baseline

- Runs as a non-root container user.
- Docker Compose drops Linux capabilities and enables `no-new-privileges`.
- JWT validation restricts the signing algorithm and issuer.
- Authentication configuration rejects weak JWT secrets.
- CodeQL runs on pull requests and `main`.
- CI runs race-enabled tests, `go vet`, a binary build, and a container build.
- Generated binaries are not stored in Git.

## Project structure

```text
cmd/stremdbc/            application entry point
configs/                 runtime configuration
internal/api/            management HTTP API
internal/auth/           JWT and API-key controls
internal/cluster/        Redis cluster coordination
internal/core/           stream registry
internal/dvr/            DVR lifecycle
internal/ingest/         protocol ingest adapters
internal/output/         HLS/LL-HLS and protocol outputs
internal/recorder/       FFmpeg recording
internal/transcoder/     FFmpeg ABR workers
internal/metrics/        Prometheus metrics
web/player/              playback UI
web/dashboard/           management dashboard
deployments/             deployment assets
.github/workflows/       CI and security automation
```

## Release gates

A production release should pass all automated GitHub checks and then validate:

1. RTMP/RTSP/SRT/WebRTC interoperability with the exact publisher/player matrix used in production.
2. Multi-hour soak tests and failure/reconnect scenarios.
3. Target bitrate/viewer concurrency load tests.
4. TURN/TLS configuration when WebRTC is exposed across NAT/public networks.
5. Secret injection, backup/restore, and Redis/PostgreSQL operational policy for clustered deployments.

## License

MIT License

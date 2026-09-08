# STREMDBC

STREMDBC is a Go streaming management control plane with bounded protocol
adapters and reusable media-output/FFmpeg components.

## Verified scope

The executable in this checkout is production-hardened as a management and
control service. It is not yet a production media plane: no RTMP/RTP/libsrt
media engine or ingest-to-output media graph is included. The service therefore
fails closed rather than claiming that control traffic is media delivery.

| Component | Current contract |
|---|---|
| HTTP API, stream registry, auth, metrics | Available and lifecycle-tested |
| HLS and LL-HLS writers | Available as library components; no executable media graph feeds them |
| RTMP ingest | Bounded handshake/control parser; audio/video is rejected explicitly |
| RTSP ingest | Bounded control/session adapter; media description/forwarding is unavailable |
| SRT ingest/output | Metadata guard only; no libsrt transport or media forwarding |
| WHIP/WHEP | SDP peer negotiation and publisher-track detection; RTP forwarding is unavailable |
| RTMP/RTSP outputs | Bounded consumer/control endpoints; no outbound media writer |
| Recorder, DVR, transcoder | Reusable managers; no executable API/media-source wiring |
| Redis cluster | Redis node registration, heartbeat, discovery, and selection primitives |
| PostgreSQL | Configuration is rejected until schema integration is implemented |

The default configuration keeps all media adapters disabled. Enable an adapter
only after supplying the corresponding production media engine and integration.

## Local quick start

Requires Go 1.27 or newer. FFmpeg is required only when using the recorder or
transcoder library managers.

```bash
go test ./...
make verify
go run ./cmd/stremdbc -config configs/config.dev.yaml
```

The development config binds the API to `127.0.0.1` and leaves authentication
off. Do not expose it beyond the local machine.

## Docker Compose

The production-oriented sample requires injected secrets and exposes host
ports only on loopback by default:

```bash
cp .env.example .env
# Replace every SET_ME/replace-* value with generated secret material.
docker compose up -d --build
```

Open `http://localhost:8080/health` and the authenticated management dashboard
at `http://localhost:8080/dashboard/`. The default `configs/config.yaml`
enables API authentication and expands `STREMDBC_JWT_SECRET` and
`STREMDBC_API_KEY` from the environment. A missing or weak secret prevents
startup.

Optional infrastructure profiles are available, but the application does not
consume PostgreSQL metadata yet:

```bash
docker compose --profile monitoring up -d
docker compose --profile cluster up -d
```

## Management API

```text
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

The base path and metrics path are configurable. Mutating management requests
and token issuance require `X-API-Key` whenever authentication is enabled.
Playback and WHIP/WHEP requests require stream-scoped JWTs unless anonymous
playback was explicitly enabled.

See [`docs/API.md`](docs/API.md) for request and response details.

## Ports

These are potential listener ports; the shipped configs disable the media
adapters.

| Port | Transport | Purpose |
|---:|---|---|
| 8080 | TCP | API, dashboard, and static delivery |
| 1935 | TCP | RTMP control adapter |
| 8554 | TCP | RTSP control adapter |
| 9000 | UDP | SRT metadata adapter |
| 8443 | TCP | WHIP/WHEP signaling adapter |
| 1936 | TCP | RTMP output control adapter |
| 8555 | TCP | RTSP output control adapter |
| 9001 | UDP | SRT output metadata adapter |

## Security baseline

- Explicit config files use strict YAML fields, environment expansion, path and listener validation, and fail closed on missing files or unsupported settings.
- The container runs as a non-root user, drops Linux capabilities, and enables `no-new-privileges`.
- JWTs are restricted to HS256, issuer, expiry, stream, action, and optional client-IP binding; API-key comparisons are bounded and constant-time per configured key.
- Static delivery rejects traversal and symlink escapes, and media/control parsers enforce size limits and deadlines.
- RTMP/RTSP ingest is rejected by configuration when authentication is enabled until publish-token enforcement is integrated into those protocols.
- Compose binds host ports to loopback until an operator deliberately changes the exposure policy.

## Project structure

```text
cmd/stremdbc/            executable and lifecycle orchestration
configs/                 runtime examples
internal/api/            management HTTP API
internal/auth/           JWT and API-key controls
internal/cluster/        Redis coordination primitives
internal/core/           stream registry and safe identifiers
internal/dvr/             file-backed DVR manager
internal/ingest/          bounded protocol adapters
internal/output/          HLS/LL-HLS and output adapters
internal/recorder/        FFmpeg recording manager
internal/transcoder/      FFmpeg ABR worker pool
internal/metrics/         Prometheus telemetry
web/                      zero-build dashboard and player
deployments/              deployment assets
.github/workflows/        CI and CodeQL definitions
```

## Release gates

Local checks are reproducible with `make verify` and include formatting,
module verification/tidiness, race tests, `staticcheck`, `gosec`, `go vet`,
and a trimmed build.
Hosted CI/CodeQL execution, container registry publication, third-party
protocol interoperability, real ingest-to-HLS/WebRTC forwarding, soak/load
testing, TURN/TLS public-network validation, and secret-manager integration
remain external release gates. This checkout does not provide evidence for
those gates. `govulncheck` currently reports `GO-2026-4479` in the Pion DTLS
dependency with no upstream fixed version; keep WebRTC disabled until that
advisory is resolved or formally accepted with compensating controls.

## License

MIT License

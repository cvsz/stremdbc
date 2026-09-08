# STREMDBC - Production-Grade Streaming Media Server

A self-hosted, production-grade streaming media server modeled after Wowza capabilities.

## Architecture

STREMDBC follows a single-ingest / multi-output media pipeline architecture:

```
                    ┌──────────────────────┐
                    │   RTMP / SRT / RTSP  │
                    │   MPEG-TS / WebRTC   │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   STREMDBC CORE      │
                    │                      │
                    │ Stream Registry      │
                    │ Session Manager      │
                    │ Demux / Remux        │
                    │ Media Pipeline       │
                    │ Auth / ACL           │
                    │ Metrics              │
                    └──────────┬───────────┘
                               │
             ┌─────────────────┼─────────────────┐
             ▼                 ▼                 ▼
        ┌─────────┐       ┌─────────┐       ┌─────────┐
        │   HLS   │       │ WebRTC  │       │  RTMP   │
        │ LL-HLS  │       │ WHIP    │       │ RTSP    │
        │  CMAF   │       │ WHEP    │       │   SRT   │
        └────┬────┘       └────┬────┘       └────┬────┘
             │                 │                 │
             └─────────────────┼─────────────────┘
                               ▼
                         ┌───────────┐
                         │   CDN /   │
                         │  Players  │
                         └───────────┘
```

## Features (Phase 1)

- ✅ RTMP ingest
- ✅ HLS output
- ✅ Stream registry
- ✅ REST API
- ✅ Health checks
- ✅ Docker support
- ✅ Prometheus metrics

## Quick Start

### Using Docker Compose

```bash
docker-compose up -d
```

### Manual Build

```bash
go build -o stremdbc ./cmd/stremdbc
./stremdbc
```

## Usage

### Publish a Stream (OBS)

```
Server: rtmp://localhost:1935/live
Stream Key: mystream
```

### Play the Stream

**HLS:**
```
http://localhost:8080/hls/mystream/index.m3u8
```

**Web Player:**
```
http://localhost:8080/player/mystream
```

## API Endpoints

```http
GET    /api/v1/health
GET    /api/v1/streams
POST   /api/v1/streams
GET    /api/v1/streams/:id
DELETE /api/v1/streams/:id
GET    /api/v1/metrics
```

## Configuration

See `configs/config.yaml` for configuration options.

## Project Structure

```
stremdbc/
├── cmd/
│   └── stremdbc/          # Main server binary
├── internal/
│   ├── core/              # Core server logic
│   ├── ingest/rtmp/       # RTMP ingest handler
│   ├── output/hls/        # HLS output handler
│   ├── media/             # Media processing
│   ├── auth/              # Authentication
│   ├── api/               # REST API
│   ├── config/            # Configuration
│   └── metrics/           # Prometheus metrics
├── web/
│   └── player/            # Web player
├── deployments/
│   └── docker/            # Docker configurations
├── configs/               # Configuration files
├── scripts/               # Utility scripts
├── tests/                 # Tests
├── docs/                  # Documentation
└── Dockerfile
```

## Roadmap

### Phase 1 — Media Server Foundation (Current)
- [x] Go project structure
- [x] Configuration & logging
- [x] Health API
- [x] Stream registry
- [x] RTMP ingest
- [x] HLS output
- [x] Basic player
- [x] Docker

### Phase 2 — Professional Protocols
- [ ] RTSP ingest/output
- [ ] SRT ingest/output
- [ ] WebRTC (WHIP/WHEP)
- [ ] RTMP output

### Phase 3 — Transcoding
- [ ] FFmpeg worker
- [ ] ABR ladder
- [ ] GPU acceleration
- [ ] Hardware detection

### Phase 4 — Production
- [ ] Authentication & stream keys
- [ ] JWT playback authorization
- [ ] Recording & DVR
- [ ] Enhanced metrics

### Phase 5 — Cluster
- [ ] Redis integration
- [ ] PostgreSQL metadata
- [ ] Node discovery
- [ ] Distributed workers

### Phase 6 — Management Platform
- [ ] React dashboard
- [ ] Live stream monitoring
- [ ] Analytics & monitoring

## License

MIT License

## Contributing

Contributions are welcome! Please read our contributing guidelines before submitting PRs.

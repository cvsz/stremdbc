# STREMDBC Production Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the STREMDBC service fail-closed, race-free, observable, testable, and honest about protocol capability while repairing every actionable validation, lifecycle, static-delivery, manager, tooling, and deployment defect found in the audit.

**Architecture:** Keep the existing package boundaries, make `core.StreamRegistry` the synchronized source of stream lifecycle state, and use small protocol-specific parsers with bounded input and explicit state transitions. Managers will own their goroutines/resources and return immutable snapshots; the HTTP layer will centralize validation, authorization, static-path safety, and configured timeouts.

**Tech Stack:** Go 1.27, standard library networking/concurrency, `yaml.v3`, `gin`, Pion WebRTC, Prometheus, Redis, FFmpeg, Docker Compose, GitHub Actions.

## Global Constraints

- No protocol adapter may report a stream live or successful media delivery when it only received an unsupported or malformed payload.
- All externally derived stream IDs, profile names, file paths, and request bodies are bounded and validated before mutation.
- All getters return snapshots, not pointers to mutable internal state.
- Every new behavior is covered by a failing test observed before its implementation.
- No credentials, generated binaries, or broad worktree changes may be added.
- Existing public method signatures remain source-compatible unless a new optional setter is required.

---

### Task 1: Configuration and repository gates

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `Makefile`
- Modify: `.github/workflows/ci.yml`
- Create: `.dockerignore`

**Interfaces:**
- `config.Load` rejects unknown YAML keys and preserves defaults for omitted keys.
- `Config.Validate` validates hosts, durations, enabled listener ports/collisions, auth material, CORS, output formats, and all enabled component paths.
- `make verify` runs formatting, module, test, race, static-analysis/security, vet, and build gates.

- [x] Write tests for strict YAML parsing, invalid durations, duplicate enabled listener addresses, invalid timeouts, empty enabled paths, and authentication without usable API keys.
- [x] Run `go test ./internal/config -run 'Test(Strict|Invalid|Duplicate|Enabled)' -v` and confirm each new test fails for the current implementation.
- [x] Implement strict loading, normalization, and validation without changing the exported configuration field types.
- [x] Run the focused config tests, then `go test ./internal/config`.
- [x] Add `make verify` and CI steps for `gofmt -l`, `go mod verify`, `go mod tidy -diff`, `go test -race`, pinned `staticcheck`, `go vet`, and build.
- [x] Add `.dockerignore` entries for `.git`, local binaries, coverage, editor state, and temporary output; run `docker build --pull` and `docker compose config --quiet`.

### Task 2: Stream registry correctness and lifecycle

**Files:**
- Modify: `internal/core/registry.go`
- Modify: `internal/core/registry_test.go`
- Create: `internal/core/stream_id.go`

**Interfaces:**
- `core.ValidateStreamID(string) error` is the shared path-safe identifier validator.
- `StreamRegistry.Get` and `List` return deep copies, including metadata.
- `StreamRegistry.Cleanup` removes only old idle/error streams with no viewers and never removes live/recording streams.

- [x] Add failing tests for invalid IDs, nil updaters, metadata aliasing, deterministic listing, state timestamps, and cleanup eligibility.
- [x] Run the focused registry tests and observe failures before production edits.
- [x] Implement validation, immutable snapshots, synchronized timestamps/state transitions, bounded cleanup intervals, and safe cleanup.
- [x] Run `go test -race ./internal/core` and the full package suite.

### Task 3: HTTP API, authentication, and static-delivery hardening

**Files:**
- Modify: `internal/api/server.go`
- Modify: `internal/api/server_test.go`
- Modify: `internal/auth/manager.go`
- Modify: `internal/auth/manager_test.go`
- Modify: `internal/metrics/metrics.go`

**Interfaces:**
- `Server.SetHTTPTimeouts(read, write, idle time.Duration)` applies configured server limits.
- `Server.SetMetricsConfig(enabled bool, path string)` controls the metrics route.
- JSON endpoints accept exactly one bounded JSON value and reject trailing data.
- Playback static routes require a valid play token when authentication is enabled and anonymous access is disabled.

- [x] Add failing tests for trailing JSON, encoded/path-traversal IDs, CORS preflight rejection, configured timeout/path behavior, token claim validation, no secret claim leakage, playback authorization, and metric refresh.
- [x] Run focused API/auth tests and observe the expected failures.
- [x] Implement shared ID validation, strict request decoding, safe static serving, secure CORS, immutable auth claims, URL-safe signed links, and metrics configuration/refresh.
- [x] Run `go test -race ./internal/api ./internal/auth ./internal/metrics` and a handler integration test for health, API, metrics, and static routes.

### Task 4: HLS and LL-HLS managers

**Files:**
- Modify: `internal/config/config.go`
- Modify: `configs/config.yaml`
- Modify: `configs/config.dev.yaml`
- Modify: `internal/output/hls/manager.go`
- Modify: `internal/output/llhls/manager.go`
- Create: `internal/output/hls/manager_test.go`
- Create: `internal/output/llhls/manager_test.go`

**Interfaces:**
- HLS writes are atomic, path-safe, and produce a valid live playlist using the configured target duration and window.
- HLS cleanup retains the configured latest segments and ignores unrelated files.
- LL-HLS has a configured output path, synchronized stream snapshots, segment/part writes, and bounded cleanup/stop behavior.

- [x] Add failing tests for traversal, atomic playlist contents, live-vs-final playlists, retention, concurrent segment writes, LL-HLS snapshot isolation, and non-positive cleanup intervals.
- [x] Run both focused manager test packages and observe failures.
- [x] Implement the managers and wire only enabled paths into the API.
- [x] Run focused tests under `-race`, then full Go verification.

### Task 5: Recorder, DVR, transcoder, cluster, and metrics lifecycle

**Files:**
- Modify: `internal/recorder/manager.go`
- Modify: `internal/dvr/manager.go`
- Modify: `internal/transcoder/worker.go`
- Modify: `internal/cluster/manager.go`
- Modify: `internal/metrics/metrics.go`
- Create or modify: package-specific `*_test.go` files for each behavior.

**Interfaces:**
- Resource managers have idempotent start/stop behavior, close owned connections/processes, and return snapshots.
- Recorder state transitions are synchronized and distinguish failed, stopped, and cancelled processes.
- Transcoder submission validates profile/output paths, starts workers once, reports process failures, and cancels active FFmpeg commands.
- Cluster discovery is deterministic, validates node identity, honors stop context, and selects a node using bounded load data.

- [x] Add failing tests for recorder process failure/race, snapshot aliasing, DVR pause duration and file closure, transcoder start/stop/submission, cluster selection/snapshots, and non-negative metric inputs.
- [x] Run each focused test package to confirm red tests.
- [x] Implement one root-cause repair per manager and retain external-system boundaries behind the existing Redis/FFmpeg calls.
- [x] Run all focused race tests and full `go test -race ./...`.

### Task 6: Protocol adapter safety and honest runtime wiring

**Files:**
- Modify: `internal/ingest/rtmp/server.go`
- Modify: `internal/ingest/rtsp/server.go`
- Modify: `internal/ingest/srt/server.go`
- Modify: `internal/ingest/webrtc/server.go`
- Modify: `internal/output/rtmp/server.go`
- Modify: `internal/output/rtsp/server.go`
- Modify: `internal/output/srt/server.go`
- Modify: `internal/output/webrtc/manager.go`
- Create: package-specific protocol tests.

**Interfaces:**
- TCP servers bind synchronously, reset state after bind failure, track and close active connections, and stop on context or listener closure.
- RTMP validates the versioned handshake/chunk bounds and parses enough control messages to identify a publish stream; malformed or unsupported media is rejected without a false live state.
- RTSP parses complete bounded requests, echoes the client CSeq, allocates sessions before responding, and cleans sessions on teardown/disconnect.
- SRT never treats arbitrary UDP bytes as a valid SRT session; unsupported protocol input is rejected/ignored with bounded state.
- WHIP/WHEP validates SDP content types/shapes, has deterministic session IDs, closes only the owning session, and synchronously reports bind/TLS errors.
- Output adapters never count bytes read from a client as bytes sent and do not advertise media delivery without a connected source.

- [x] Add failing protocol tests for handshake framing, partial reads, malformed input, stream lifecycle, shutdown with active clients, CSeq/session correctness, UDP buffer ownership, WebRTC bind failure, and output byte accounting.
- [x] Run each focused protocol test to observe red failures.
- [x] Implement the smallest standards-correct control path available in the current dependency set; use explicit `ErrUnsupported` responses for media features requiring an external codec/protocol engine instead of echoing bytes.
- [x] Run protocol tests under `-race` and document any external interoperability gate separately from local correctness.

### Task 7: Runtime lifecycle, deployment, documentation, and final verification

**Files:**
- Modify: `cmd/stremdbc/main.go`
- Modify: `README.md`
- Modify: `docs/API.md`
- Modify: `Dockerfile`
- Modify: `docker-compose.yml`
- Modify: `deployments/docker/prometheus.yml`
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/codeql.yml` if required by the final gates.

**Interfaces:**
- Startup returns actionable errors and cleans already-started components on a later failure.
- Shutdown uses one context, closes all listeners/processes, and reports every error without deadlocking.
- Documentation matches the verified capability matrix and does not call placeholders complete.

- [x] Add failing lifecycle/integration tests for startup failure cleanup, configured API timeouts/metrics, and graceful shutdown.
- [x] Run them and observe the current failures.
- [x] Implement bounded startup/shutdown ordering, runtime configuration wiring, and truthful docs/deployment defaults.
- [x] Run the full verification matrix: `gofmt`, `go mod tidy -diff`, `go test ./...`, `go test -race ./...`, `staticcheck`, `gosec`, `go vet ./...`, `make verify`, `docker compose config --quiet`, `docker build --pull`, and `shellcheck` over any shell files if introduced.
- [x] Perform an independent criterion-by-criterion review and record PASS/FAIL/INCONCLUSIVE evidence before claiming completion.

## Verification record (2026-09-08)

PASS: `gofmt`, `git diff --check`, `go mod verify`, `go mod tidy -diff`,
`go test ./...`, `go test -race ./...`, `go vet ./...`, `staticcheck ./...`,
`gosec ./...` (0 issues), `make verify`, trimmed binary build/version check,
`docker compose config --quiet`, Docker image build, and local HTTP lifecycle
smoke test.

PASS: no shell files are present, so ShellCheck has no targets.

INCONCLUSIVE/external: hosted CI and CodeQL, registry publication, real
protocol interoperability, ingest-to-output media forwarding, soak/load,
public TURN/TLS validation, secret-manager integration, and production
observability/backup drills require target-environment evidence.

BLOCKED release gate: `govulncheck` reports `GO-2026-4479` in
`github.com/pion/dtls/v2@v2.2.12`, with no upstream fixed version. WebRTC
remains disabled by default and must not be enabled for production until this
is resolved or formally accepted with compensating controls.

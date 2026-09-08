# STREMDBC Management API

The management API is served by the same HTTP server as the dashboard and HLS assets. The default base path is `/api/v1`.

## Health and observability

### `GET /health`

Returns process health, version, and uptime.

```json
{
  "status": "healthy",
  "version": "0.6.0",
  "uptime": "12m3s"
}
```

### `GET /ready`

Returns readiness for traffic.

```json
{"status":"ready"}
```

### `GET /metrics`

Prometheus text exposition endpoint.

### `GET /api/v1/info`

Returns server version and Go runtime information.

### `GET /api/v1/stats`

Returns management-dashboard telemetry.

```json
{
  "streams": 2,
  "live_streams": 1,
  "viewers": 15,
  "goroutines": 24,
  "memory_bytes": 12582912,
  "uptime_seconds": 390,
  "components": {
    "transcoder": {},
    "cluster": {}
  }
}
```

## Streams

### `GET /api/v1/streams`

Lists registered streams.

### `POST /api/v1/streams`

Creates a stream record.

```json
{
  "id": "camera-01",
  "name": "Camera 01"
}
```

When authentication is enabled, send the configured API key:

```http
X-API-Key: <api-key>
Content-Type: application/json
```

### `GET /api/v1/streams/{id}`

Returns a stream record.

### `DELETE /api/v1/streams/{id}`

Deletes a stream record. Requires an API key when authentication is enabled.

## JWT stream tokens

### `POST /api/v1/auth/token`

Creates a stream-scoped JWT. Authentication must be enabled and a valid `X-API-Key` must be supplied unless anonymous access was explicitly configured.

Request:

```json
{
  "stream_id": "camera-01",
  "action": "play"
}
```

`action` must be `play` or `publish`.

Response:

```json
{
  "token": "<jwt>",
  "action": "play",
  "stream_id": "camera-01"
}
```

JWT validation is restricted to HS256 tokens issued by `stremdbc` and validates expiry.

## Static delivery

The management HTTP plane also serves:

- `/dashboard/` — management UI
- `/player/{streamID}` — built-in player
- `/hls/{streamID}/...` — HLS assets
- `/llhls/{streamID}/...` — LL-HLS assets when enabled

## Error format

JSON API errors use a single `error` field:

```json
{"error":"unauthorized"}
```

Typical status codes are `400`, `401`, `404`, `405`, `409`, and `500`.

## Production guidance

- Set `auth.enable: true`.
- Use a random JWT secret of at least 32 characters.
- Set `auth.allow_anonymous: false`.
- Provision non-default API keys through your secret-management process rather than committing them.
- Restrict `api.cors_origins` to trusted origins.
- Terminate TLS at a trusted reverse proxy or ingress, or enable the relevant protocol TLS controls.

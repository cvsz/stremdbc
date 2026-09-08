# API Documentation

## STREMDBC REST API v1

Base URL: `http://localhost:8080/api/v1`

## Endpoints

### Health & Info

#### GET /health
Check server health status.

**Response:**
```json
{
  "status": "healthy"
}
```

#### GET /ready
Check if server is ready to accept streams.

**Response:**
```json
{
  "status": "ready"
}
```

#### GET /api/v1/info
Get server information.

**Response:**
```json
{
  "name": "STREMDBC",
  "version": "0.1.0",
  "go_version": "go1.19.8",
  "goroutines": 8,
  "cpus": 2
}
```

### Streams

#### GET /api/v1/streams
List all streams.

**Response:**
```json
{
  "count": 2,
  "streams": [
    {
      "id": "mystream",
      "name": "My Stream",
      "state": "LIVE",
      "created_at": "2024-01-01T00:00:00Z",
      "started_at": "2024-01-01T00:01:00Z",
      "codec": "H264/AAC",
      "bitrate": 6200000,
      "viewers": 143,
      "publisher_ip": "192.168.1.100"
    }
  ]
}
```

#### POST /api/v1/streams
Create a new stream.

**Request Body:**
```json
{
  "id": "mystream",
  "name": "My Stream"
}
```

**Response (201 Created):**
```json
{
  "id": "mystream",
  "name": "My Stream",
  "state": "IDLE",
  "created_at": "2024-01-01T00:00:00Z",
  "viewers": 0
}
```

#### GET /api/v1/streams/:id
Get information about a specific stream.

**Response:**
```json
{
  "id": "mystream",
  "name": "My Stream",
  "state": "LIVE",
  "created_at": "2024-01-01T00:00:00Z",
  "started_at": "2024-01-01T00:01:00Z",
  "viewers": 143
}
```

#### DELETE /api/v1/streams/:id
Delete a stream.

**Response (204 No Content)**

### Metrics

#### GET /metrics
Prometheus metrics endpoint.

**Content-Type:** text/plain

**Example Metrics:**
```
# HELP stremdbc_streams_total Total number of streams created
# TYPE stremdbc_streams_total counter
stremdbc_streams_total 5

# HELP stremdbc_streams_live Number of currently live streams
# TYPE stremdbc_streams_live gauge
stremdbc_streams_live 2

# HELP stremdbc_viewers_total Total number of viewers across all streams
# TYPE stremdbc_viewers_total gauge
stremdbc_viewers_total 286
```

## Stream States

| State | Description |
|-------|-------------|
| IDLE | Stream registered but not publishing |
| LIVE | Stream is actively receiving media |
| RECORDING | Stream is being recorded |
| ERROR | Stream encountered an error |

## Error Responses

### 400 Bad Request
```json
{
  "error": "stream ID required"
}
```

### 404 Not Found
```json
{
  "error": "stream not found"
}
```

### 409 Conflict
```json
{
  "error": "stream already exists"
}
```

### 500 Internal Server Error
```json
{
  "error": "failed to create stream"
}
```

## Usage Examples

### cURL Examples

**Create a stream:**
```bash
curl -X POST http://localhost:8080/api/v1/streams \
  -H "Content-Type: application/json" \
  -d '{"id":"mystream","name":"My Stream"}'
```

**Get stream info:**
```bash
curl http://localhost:8080/api/v1/streams/mystream
```

**List all streams:**
```bash
curl http://localhost:8080/api/v1/streams
```

**Delete a stream:**
```bash
curl -X DELETE http://localhost:8080/api/v1/streams/mystream
```

**Check health:**
```bash
curl http://localhost:8080/health
```

**Get metrics:**
```bash
curl http://localhost:8080/metrics
```

### OBS Configuration

**Server:** `rtmp://localhost:1935/live`
**Stream Key:** `mystream`

### VLC Playback

**HLS Stream:**
```
http://localhost:8080/hls/mystream/index.m3u8
```

**Web Player:**
```
http://localhost:8080/player/mystream
```

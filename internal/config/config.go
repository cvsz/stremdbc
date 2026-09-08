package config

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/pion/webrtc/v3"
	"gopkg.in/yaml.v3"
)

const (
	// MaxPlaylistSize bounds the amount of segment history retained by the HLS
	// library components and prevents untrusted configuration from causing
	// unbounded memory or disk work.
	MaxPlaylistSize = 10_000

	// Transcoder resource limits keep malformed configuration from creating
	// unbounded worker pools or handing impractical values to FFmpeg.
	MaxTranscoderWorkers      = 256
	MaxTranscoderDimension    = 16_384
	MaxTranscoderFrameRate    = 240
	MaxTranscoderBitrate      = 100_000_000
	MaxTranscoderAudioBitrate = 10_000_000
)

// Config holds the server configuration.
type Config struct {
	Server     ServerConfig     `yaml:"server"`
	RTMP       RTMPConfig       `yaml:"rtmp"`
	RTSP       RTSPConfig       `yaml:"rtsp"`
	SRT        SRTConfig        `yaml:"srt"`
	WebRTC     WebRTCConfig     `yaml:"webrtc"`
	HLS        HLSConfig        `yaml:"hls"`
	LLHLS      LLHLSConfig      `yaml:"llhls"`
	RTMPOutput RTMPOutputConfig `yaml:"rtmp_output"`
	RTSPOutput RTSPOutputConfig `yaml:"rtsp_output"`
	SRTOutput  SRTOutputConfig  `yaml:"srt_output"`
	DVR        DVRConfig        `yaml:"dvr"`
	API        APIConfig        `yaml:"api"`
	Logging    LoggingConfig    `yaml:"logging"`
	Metrics    MetricsConfig    `yaml:"metrics"`
	Recorder   RecorderConfig   `yaml:"recorder"`
	Auth       AuthConfig       `yaml:"auth"`
	Redis      RedisConfig      `yaml:"redis"`
	Postgres   PostgresConfig   `yaml:"postgres"`
	Cluster    ClusterConfig    `yaml:"cluster"`
	Transcoder TranscoderConfig `yaml:"transcoder"`
}

type ServerConfig struct {
	Host         string        `yaml:"host"`
	HTTPPort     int           `yaml:"http_port"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
	StreamTTL    time.Duration `yaml:"stream_ttl"`
}

type RTMPConfig struct {
	Enable       bool          `yaml:"enable"`
	Host         string        `yaml:"host"`
	Port         int           `yaml:"port"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

type HLSConfig struct {
	Enable          bool          `yaml:"enable"`
	Path            string        `yaml:"path"`
	SegmentDuration time.Duration `yaml:"segment_duration"`
	PlaylistSize    int           `yaml:"playlist_size"`
}

type APIConfig struct {
	Enable      bool     `yaml:"enable"`
	BasePath    string   `yaml:"base_path"`
	CORSOrigins []string `yaml:"cors_origins"`
}

type LoggingConfig struct {
	Level      string `yaml:"level"`
	Format     string `yaml:"format"`
	OutputPath string `yaml:"output_path"`
}

type MetricsConfig struct {
	Enable bool   `yaml:"enable"`
	Path   string `yaml:"path"`
}

type RecorderConfig struct {
	Enable     bool   `yaml:"enable"`
	Path       string `yaml:"path"`
	FFmpegPath string `yaml:"ffmpeg_path"`
}

type RTSPConfig struct {
	Enable      bool          `yaml:"enable"`
	Host        string        `yaml:"host"`
	Port        int           `yaml:"port"`
	ReadTimeout time.Duration `yaml:"read_timeout"`
}

type SRTConfig struct {
	Enable     bool          `yaml:"enable"`
	Host       string        `yaml:"host"`
	Port       int           `yaml:"port"`
	Latency    time.Duration `yaml:"latency"`
	Passphrase string        `yaml:"passphrase"`
}

type WebRTCConfig struct {
	Enable     bool             `yaml:"enable"`
	Host       string           `yaml:"host"`
	Port       int              `yaml:"port"`
	ICEServer  webrtc.ICEServer `yaml:"ice_server"`
	UseTURN    bool             `yaml:"use_turn"`
	TLSEnabled bool             `yaml:"tls_enabled"`
	CertFile   string           `yaml:"cert_file"`
	KeyFile    string           `yaml:"key_file"`
}

type LLHLSConfig struct {
	Enable          bool          `yaml:"enable"`
	Path            string        `yaml:"path"`
	SegmentDuration time.Duration `yaml:"segment_duration"`
	PartDuration    time.Duration `yaml:"part_duration"`
	PlaylistSize    int           `yaml:"playlist_size"`
}

type AuthConfig struct {
	Enable         bool     `yaml:"enable"`
	JWTSecret      string   `yaml:"jwt_secret"`
	JWTExpiry      string   `yaml:"jwt_expiry"`
	APIKeys        []string `yaml:"api_keys"`
	AllowAnonymous bool     `yaml:"allow_anonymous"`
}

type RedisConfig struct {
	Enable   bool   `yaml:"enable"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	Prefix   string `yaml:"prefix"`
}

type PostgresConfig struct {
	Enable   bool   `yaml:"enable"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	SSLMode  string `yaml:"ssl_mode"`
}

type ClusterConfig struct {
	Enable              bool          `yaml:"enable"`
	NodeID              string        `yaml:"node_id"`
	AdvertiseHost       string        `yaml:"advertise_host"`
	DiscoveryAddr       string        `yaml:"discovery_addr"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval"`
}

type TranscoderConfig struct {
	Enable       bool         `yaml:"enable"`
	WorkerCount  int          `yaml:"worker_count"`
	FFmpegPath   string       `yaml:"ffmpeg_path"`
	GPUEnabled   bool         `yaml:"gpu_enabled"`
	ABRLadder    []ABRProfile `yaml:"abr_ladder"`
	OutputFormat string       `yaml:"output_format"`
}

type ABRProfile struct {
	Name         string `yaml:"name"`
	Width        int    `yaml:"width"`
	Height       int    `yaml:"height"`
	Bitrate      int    `yaml:"bitrate"`
	FrameRate    int    `yaml:"frame_rate"`
	AudioBitrate int    `yaml:"audio_bitrate"`
}

type RTMPOutputConfig struct {
	Enable      bool          `yaml:"enable"`
	Host        string        `yaml:"host"`
	Port        int           `yaml:"port"`
	ReadTimeout time.Duration `yaml:"read_timeout"`
}

type RTSPOutputConfig struct {
	Enable      bool          `yaml:"enable"`
	Host        string        `yaml:"host"`
	Port        int           `yaml:"port"`
	ReadTimeout time.Duration `yaml:"read_timeout"`
}

type SRTOutputConfig struct {
	Enable  bool          `yaml:"enable"`
	Host    string        `yaml:"host"`
	Port    int           `yaml:"port"`
	Latency time.Duration `yaml:"latency"`
}

type DVRConfig struct {
	Enable      bool          `yaml:"enable"`
	Path        string        `yaml:"path"`
	MaxDuration time.Duration `yaml:"max_duration"`
	Format      string        `yaml:"format"`
}

// DefaultConfig returns a configuration with safe development defaults.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Host:         "127.0.0.1",
			HTTPPort:     8080,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  120 * time.Second,
			StreamTTL:    24 * time.Hour,
		},
		RTMP: RTMPConfig{
			Enable:       false,
			Host:         "0.0.0.0",
			Port:         1935,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
		RTSP: RTSPConfig{
			Enable:      false,
			Host:        "0.0.0.0",
			Port:        8554,
			ReadTimeout: 30 * time.Second,
		},
		SRT: SRTConfig{
			Enable:  false,
			Host:    "0.0.0.0",
			Port:    9000,
			Latency: 200 * time.Millisecond,
		},
		WebRTC: WebRTCConfig{
			Enable:     false,
			Host:       "0.0.0.0",
			Port:       8443,
			TLSEnabled: false,
		},
		HLS: HLSConfig{
			Enable:          false,
			Path:            "/tmp/hls",
			SegmentDuration: 2 * time.Second,
			PlaylistSize:    5,
		},
		LLHLS: LLHLSConfig{
			Enable:          false,
			Path:            "/tmp/llhls",
			SegmentDuration: time.Second,
			PartDuration:    200 * time.Millisecond,
			PlaylistSize:    10,
		},
		RTMPOutput: RTMPOutputConfig{
			Enable:      false,
			Host:        "0.0.0.0",
			Port:        1936,
			ReadTimeout: 30 * time.Second,
		},
		RTSPOutput: RTSPOutputConfig{
			Enable:      false,
			Host:        "0.0.0.0",
			Port:        8555,
			ReadTimeout: 30 * time.Second,
		},
		SRTOutput: SRTOutputConfig{
			Enable:  false,
			Host:    "0.0.0.0",
			Port:    9001,
			Latency: 200 * time.Millisecond,
		},
		DVR: DVRConfig{
			Enable:      false,
			Path:        "/tmp/dvr",
			MaxDuration: 4 * time.Hour,
			Format:      "mpegts",
		},
		API: APIConfig{
			Enable:      true,
			BasePath:    "/api/v1",
			CORSOrigins: []string{"*"},
		},
		Logging: LoggingConfig{
			Level:      "info",
			Format:     "json",
			OutputPath: "stdout",
		},
		Metrics: MetricsConfig{
			Enable: true,
			Path:   "/metrics",
		},
		Recorder: RecorderConfig{
			Enable:     false,
			Path:       "/tmp/recordings",
			FFmpegPath: "ffmpeg",
		},
		Auth: AuthConfig{
			Enable:         false,
			JWTSecret:      "",
			JWTExpiry:      "24h",
			AllowAnonymous: true,
		},
		Redis: RedisConfig{
			Enable: false,
			Host:   "localhost",
			Port:   6379,
			DB:     0,
			Prefix: "stremdbc:",
		},
		Postgres: PostgresConfig{
			Enable:   false,
			Host:     "localhost",
			Port:     5432,
			User:     "stremdbc",
			Password: "",
			Database: "stremdbc",
			SSLMode:  "require",
		},
		Cluster: ClusterConfig{
			Enable:              false,
			NodeID:              "",
			HealthCheckInterval: 5 * time.Second,
		},
		Transcoder: TranscoderConfig{
			Enable:       false,
			WorkerCount:  2,
			FFmpegPath:   "ffmpeg",
			GPUEnabled:   false,
			OutputFormat: "hls",
			ABRLadder: []ABRProfile{
				{Name: "1080p", Width: 1920, Height: 1080, Bitrate: 5000000, FrameRate: 30, AudioBitrate: 192000},
				{Name: "720p", Width: 1280, Height: 720, Bitrate: 3000000, FrameRate: 30, AudioBitrate: 128000},
				{Name: "480p", Width: 854, Height: 480, Bitrate: 1500000, FrameRate: 30, AudioBitrate: 96000},
				{Name: "360p", Width: 640, Height: 360, Bitrate: 800000, FrameRate: 30, AudioBitrate: 64000},
			},
		},
	}
}

// Load loads configuration from a YAML file. An empty path returns defaults.
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()
	if strings.TrimSpace(path) == "" {
		return cfg, nil
	}

	// #nosec G304 -- the configuration path is an explicit operator-selected file.
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("configuration file does not exist: %s", path)
		}
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	if len(bytes.TrimSpace(data)) == 0 {
		return cfg, nil
	}
	data = []byte(os.ExpandEnv(string(data)))

	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("parsing config file: multiple YAML documents are not supported")
		}
		return nil, fmt.Errorf("parsing config file: %w", err)
	}
	return cfg, nil
}

// Validate validates configuration invariants before listeners are started.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("configuration cannot be nil")
	}

	validateHost := func(name, host string, enabled bool) error {
		if !enabled {
			return nil
		}
		if strings.TrimSpace(host) != host || host == "" || strings.ContainsAny(host, "/?#\\[]\x00\r\n\t ") {
			return fmt.Errorf("invalid %s host", name)
		}
		if strings.Contains(host, ":") {
			ipHost := host
			if zone := strings.LastIndexByte(ipHost, '%'); zone >= 0 {
				ipHost = ipHost[:zone]
			}
			if net.ParseIP(ipHost) == nil {
				return fmt.Errorf("invalid %s host", name)
			}
		} else if net.ParseIP(host) == nil && !validHostname(host) {
			return fmt.Errorf("invalid %s host", name)
		}
		return nil
	}
	validatePort := func(name string, enabled bool, port int) error {
		if enabled && (port <= 0 || port > 65535) {
			return fmt.Errorf("invalid %s port: %d", name, port)
		}
		return nil
	}

	if err := validateHost("HTTP", c.Server.Host, c.API.Enable); err != nil {
		return err
	}
	if err := validatePort("HTTP", c.API.Enable, c.Server.HTTPPort); err != nil {
		return err
	}
	if c.API.Enable && (c.Server.ReadTimeout <= 0 || c.Server.WriteTimeout <= 0 || c.Server.IdleTimeout <= 0) {
		return fmt.Errorf("server read_timeout, write_timeout, and idle_timeout must be positive")
	}
	if c.Server.StreamTTL <= 0 {
		return fmt.Errorf("server.stream_ttl must be positive")
	}
	if err := validateBasePath(c.API.BasePath); err != nil {
		return fmt.Errorf("api.base_path: %w", err)
	}
	if err := validateCORSOrigins(c.API.CORSOrigins); err != nil {
		return err
	}
	if err := validateRoutePath("metrics.path", c.Metrics.Path); err != nil {
		return err
	}
	if c.Metrics.Path == "/health" || c.Metrics.Path == "/health/live" || c.Metrics.Path == "/health/ready" || c.Metrics.Path == "/ready" || c.Metrics.Path == c.API.BasePath+"/info" || c.Metrics.Path == c.API.BasePath+"/stats" || c.Metrics.Path == c.API.BasePath+"/auth/token" || c.Metrics.Path == c.API.BasePath+"/streams" || c.Metrics.Path == c.API.BasePath+"/streams/" || conflictsWithStaticPath(c.Metrics.Path) {
		return fmt.Errorf("metrics.path conflicts with an HTTP API route")
	}
	if strings.HasPrefix(c.Metrics.Path, c.API.BasePath+"/streams/") {
		return fmt.Errorf("metrics.path conflicts with an HTTP API route")
	}
	switch c.Logging.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("invalid logging.level %q", c.Logging.Level)
	}
	if c.Logging.Format != "json" && c.Logging.Format != "console" {
		return fmt.Errorf("invalid logging.format %q", c.Logging.Format)
	}

	if err := validateHost("RTMP", c.RTMP.Host, c.RTMP.Enable); err != nil {
		return err
	}
	if err := validatePort("RTMP", c.RTMP.Enable, c.RTMP.Port); err != nil {
		return err
	}
	if c.RTMP.Enable && (c.RTMP.ReadTimeout <= 0 || c.RTMP.WriteTimeout <= 0) {
		return fmt.Errorf("RTMP read_timeout and write_timeout must be positive")
	}
	if err := validateHost("RTSP", c.RTSP.Host, c.RTSP.Enable); err != nil {
		return err
	}
	if err := validatePort("RTSP", c.RTSP.Enable, c.RTSP.Port); err != nil {
		return err
	}
	if c.RTSP.Enable && c.RTSP.ReadTimeout <= 0 {
		return fmt.Errorf("RTSP read_timeout must be positive")
	}
	if err := validateHost("SRT", c.SRT.Host, c.SRT.Enable); err != nil {
		return err
	}
	if err := validatePort("SRT", c.SRT.Enable, c.SRT.Port); err != nil {
		return err
	}
	if c.SRT.Enable && c.SRT.Latency <= 0 {
		return fmt.Errorf("SRT latency must be positive")
	}
	if c.SRT.Enable && strings.TrimSpace(c.SRT.Passphrase) != "" {
		return fmt.Errorf("srt.passphrase is unsupported until a libsrt media engine is configured")
	}
	if err := validateHost("WebRTC", c.WebRTC.Host, c.WebRTC.Enable); err != nil {
		return err
	}
	if err := validatePort("WebRTC", c.WebRTC.Enable, c.WebRTC.Port); err != nil {
		return err
	}
	if c.WebRTC.Enable && c.WebRTC.TLSEnabled {
		if strings.TrimSpace(c.WebRTC.CertFile) == "" || strings.TrimSpace(c.WebRTC.KeyFile) == "" {
			return fmt.Errorf("WebRTC cert_file and key_file are required when TLS is enabled")
		}
		for name, path := range map[string]string{"WebRTC certificate": c.WebRTC.CertFile, "WebRTC key": c.WebRTC.KeyFile} {
			info, err := os.Stat(path)
			if err != nil || !info.Mode().IsRegular() {
				return fmt.Errorf("%s file is not readable: %s", name, path)
			}
		}
	}
	if c.WebRTC.Enable && len(c.WebRTC.ICEServer.URLs) > 0 {
		hasTURN := false
		for _, value := range c.WebRTC.ICEServer.URLs {
			if err := validateICEServerURL(value); err != nil {
				return fmt.Errorf("invalid WebRTC ICE server URL %q", value)
			}
			scheme := strings.ToLower(strings.SplitN(value, ":", 2)[0])
			if scheme == "turn" || scheme == "turns" {
				hasTURN = true
			}
		}
		if c.WebRTC.UseTURN && !hasTURN {
			return fmt.Errorf("WebRTC use_turn requires at least one TURN URL")
		}
	} else if c.WebRTC.Enable && c.WebRTC.UseTURN {
		return fmt.Errorf("WebRTC ICE server is required when use_turn is enabled")
	}
	if err := validateHost("RTMP output", c.RTMPOutput.Host, c.RTMPOutput.Enable); err != nil {
		return err
	}
	if err := validatePort("RTMP output", c.RTMPOutput.Enable, c.RTMPOutput.Port); err != nil {
		return err
	}
	if c.RTMPOutput.Enable && c.RTMPOutput.ReadTimeout <= 0 {
		return fmt.Errorf("RTMP output read_timeout must be positive")
	}
	if err := validateHost("RTSP output", c.RTSPOutput.Host, c.RTSPOutput.Enable); err != nil {
		return err
	}
	if err := validatePort("RTSP output", c.RTSPOutput.Enable, c.RTSPOutput.Port); err != nil {
		return err
	}
	if c.RTSPOutput.Enable && c.RTSPOutput.ReadTimeout <= 0 {
		return fmt.Errorf("RTSP output read_timeout must be positive")
	}
	if err := validateHost("SRT output", c.SRTOutput.Host, c.SRTOutput.Enable); err != nil {
		return err
	}
	if err := validatePort("SRT output", c.SRTOutput.Enable, c.SRTOutput.Port); err != nil {
		return err
	}
	if c.SRTOutput.Enable && c.SRTOutput.Latency <= 0 {
		return fmt.Errorf("SRT output latency must be positive")
	}

	type endpoint struct {
		name string
		host string
		port int
	}
	endpoints := make([]endpoint, 0, 8)
	if c.API.Enable {
		endpoints = append(endpoints, endpoint{name: "HTTP", host: c.Server.Host, port: c.Server.HTTPPort})
	}
	if c.RTMP.Enable {
		endpoints = append(endpoints, endpoint{"RTMP", c.RTMP.Host, c.RTMP.Port})
	}
	if c.RTSP.Enable {
		endpoints = append(endpoints, endpoint{"RTSP", c.RTSP.Host, c.RTSP.Port})
	}
	if c.SRT.Enable {
		endpoints = append(endpoints, endpoint{"SRT", c.SRT.Host, c.SRT.Port})
	}
	if c.WebRTC.Enable {
		endpoints = append(endpoints, endpoint{"WebRTC", c.WebRTC.Host, c.WebRTC.Port})
	}
	if c.RTMPOutput.Enable {
		endpoints = append(endpoints, endpoint{"RTMP output", c.RTMPOutput.Host, c.RTMPOutput.Port})
	}
	if c.RTSPOutput.Enable {
		endpoints = append(endpoints, endpoint{"RTSP output", c.RTSPOutput.Host, c.RTSPOutput.Port})
	}
	if c.SRTOutput.Enable {
		endpoints = append(endpoints, endpoint{"SRT output", c.SRTOutput.Host, c.SRTOutput.Port})
	}
	for i := 0; i < len(endpoints); i++ {
		for j := i + 1; j < len(endpoints); j++ {
			if endpoints[i].port == endpoints[j].port && hostsOverlap(endpoints[i].host, endpoints[j].host) {
				return fmt.Errorf("%s and %s use the same address %s", endpoints[i].name, endpoints[j].name, net.JoinHostPort(endpoints[i].host, fmt.Sprint(endpoints[i].port)))
			}
		}
	}

	if c.HLS.Enable {
		if err := validateDataPath("HLS", c.HLS.Path); err != nil {
			return err
		}
		if c.HLS.SegmentDuration <= 0 || c.HLS.PlaylistSize <= 0 {
			return fmt.Errorf("HLS segment_duration and playlist_size must be positive")
		}
		if c.HLS.PlaylistSize > MaxPlaylistSize {
			return fmt.Errorf("HLS playlist_size must not exceed %d", MaxPlaylistSize)
		}
	}
	if c.LLHLS.Enable {
		if err := validateDataPath("LL-HLS", c.LLHLS.Path); err != nil {
			return err
		}
		if c.LLHLS.SegmentDuration <= 0 || c.LLHLS.PartDuration <= 0 || c.LLHLS.PlaylistSize <= 0 {
			return fmt.Errorf("LL-HLS durations and playlist_size must be positive")
		}
		if c.LLHLS.PlaylistSize > MaxPlaylistSize {
			return fmt.Errorf("LL-HLS playlist_size must not exceed %d", MaxPlaylistSize)
		}
		if c.LLHLS.PartDuration >= c.LLHLS.SegmentDuration {
			return fmt.Errorf("LL-HLS part_duration must be shorter than segment_duration")
		}
	}
	if c.Auth.Enable {
		if strings.TrimSpace(c.Auth.JWTSecret) != c.Auth.JWTSecret || len(c.Auth.JWTSecret) < 32 {
			return fmt.Errorf("auth.jwt_secret must be at least 32 characters when auth is enabled")
		}
		expiry, err := time.ParseDuration(c.Auth.JWTExpiry)
		if err != nil {
			return fmt.Errorf("invalid auth.jwt_expiry: %w", err)
		}
		if expiry <= 0 {
			return fmt.Errorf("auth.jwt_expiry must be positive")
		}
		if len(nonEmpty(c.Auth.APIKeys)) == 0 {
			return fmt.Errorf("auth.api_keys must contain at least one key when authentication is enabled")
		}
		seenKeys := make(map[string]struct{}, len(c.Auth.APIKeys))
		for _, key := range c.Auth.APIKeys {
			if key == "" || strings.TrimSpace(key) != key || strings.ContainsAny(key, "\r\n\x00\t") {
				return fmt.Errorf("auth.api_keys contains an invalid key")
			}
			if _, exists := seenKeys[key]; exists {
				return fmt.Errorf("auth.api_keys contains a duplicate key")
			}
			seenKeys[key] = struct{}{}
		}
	}
	if c.Auth.Enable && (c.RTMP.Enable || c.RTSP.Enable) {
		return fmt.Errorf("publish authentication is not integrated with RTMP/RTSP ingest; keep those adapters disabled")
	}
	if c.Cluster.Enable {
		if !c.Redis.Enable {
			return fmt.Errorf("cluster mode requires redis.enable=true")
		}
		if err := validateIdentifier("cluster.node_id", c.Cluster.NodeID); err != nil {
			return err
		}
		if err := validateHost("cluster.advertise_host", c.Cluster.AdvertiseHost, true); err != nil {
			return err
		}
		if c.Cluster.AdvertiseHost == "" || net.ParseIP(strings.Trim(c.Cluster.AdvertiseHost, "[]")).IsUnspecified() {
			return fmt.Errorf("cluster.advertise_host must be a routable host when cluster mode is enabled")
		}
		if strings.TrimSpace(c.Cluster.DiscoveryAddr) != "" {
			return fmt.Errorf("cluster.discovery_addr is unsupported; Redis discovery is used")
		}
		if c.Cluster.HealthCheckInterval <= 0 {
			return fmt.Errorf("cluster.health_check_interval must be positive")
		}
	}
	if c.Redis.Enable {
		if err := validateHost("Redis", c.Redis.Host, true); err != nil {
			return err
		}
		if err := validatePort("Redis", true, c.Redis.Port); err != nil {
			return err
		}
		if strings.IndexFunc(c.Redis.Prefix, func(r rune) bool { return r == '\r' || r == '\n' }) >= 0 {
			return fmt.Errorf("redis prefix contains a line break")
		}
		if c.Redis.DB < 0 || c.Redis.DB > 15 {
			return fmt.Errorf("redis DB must be between 0 and 15")
		}
	}
	if c.Postgres.Enable {
		return fmt.Errorf("postgres.enable is unsupported: PostgreSQL schema integration is not wired into the runtime")
	}
	if c.Transcoder.Enable {
		if c.Transcoder.WorkerCount <= 0 || c.Transcoder.WorkerCount > MaxTranscoderWorkers {
			return fmt.Errorf("transcoder.worker_count must be between 1 and %d", MaxTranscoderWorkers)
		}
		if strings.TrimSpace(c.Transcoder.FFmpegPath) == "" {
			return fmt.Errorf("transcoder.ffmpeg_path is required")
		}
		if c.Transcoder.OutputFormat != "hls" {
			return fmt.Errorf("unsupported transcoder.output_format %q", c.Transcoder.OutputFormat)
		}
		if len(c.Transcoder.ABRLadder) == 0 {
			return fmt.Errorf("transcoder.abr_ladder cannot be empty")
		}
		seenProfiles := make(map[string]struct{}, len(c.Transcoder.ABRLadder))
		for _, profile := range c.Transcoder.ABRLadder {
			if strings.TrimSpace(profile.Name) == "" || profile.Width <= 0 || profile.Width > MaxTranscoderDimension || profile.Height <= 0 || profile.Height > MaxTranscoderDimension || profile.Bitrate <= 0 || profile.Bitrate > MaxTranscoderBitrate || profile.FrameRate <= 0 || profile.FrameRate > MaxTranscoderFrameRate || profile.AudioBitrate <= 0 || profile.AudioBitrate > MaxTranscoderAudioBitrate {
				return fmt.Errorf("invalid ABR profile %q", profile.Name)
			}
			if err := validateIdentifier("ABR profile", profile.Name); err != nil {
				return err
			}
			if _, exists := seenProfiles[profile.Name]; exists {
				return fmt.Errorf("duplicate ABR profile %q", profile.Name)
			}
			seenProfiles[profile.Name] = struct{}{}
		}
	}
	if c.DVR.Enable {
		if err := validateDataPath("DVR", c.DVR.Path); err != nil {
			return err
		}
		if c.DVR.MaxDuration <= 0 {
			return fmt.Errorf("dvr.max_duration must be positive")
		}
		if c.DVR.Format != "mpegts" {
			return fmt.Errorf("unsupported dvr.format %q", c.DVR.Format)
		}
	}
	if c.Recorder.Enable {
		if err := validateDataPath("recorder", c.Recorder.Path); err != nil {
			return err
		}
		if strings.TrimSpace(c.Recorder.FFmpegPath) == "" {
			return fmt.Errorf("recorder.ffmpeg_path cannot be empty")
		}
	}
	return nil
}

func hostsOverlap(left, right string) bool {
	left = strings.Trim(strings.TrimSpace(left), "[]")
	right = strings.Trim(strings.TrimSpace(right), "[]")
	if left == right {
		return true
	}
	leftIP := net.ParseIP(left)
	rightIP := net.ParseIP(right)
	if leftIP != nil && rightIP != nil {
		if (leftIP.To4() == nil) != (rightIP.To4() == nil) {
			return false
		}
		return leftIP.IsUnspecified() || rightIP.IsUnspecified()
	}
	return left == "0.0.0.0" || left == "::" || right == "0.0.0.0" || right == "::"
}

func validHostname(host string) bool {
	if host == "" || len(host) > 253 || strings.HasPrefix(host, ".") || strings.HasSuffix(host, ".") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '-') {
				return false
			}
		}
	}
	return true
}

func nonEmpty(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			result = append(result, value)
		}
	}
	return result
}

func validateDataPath(name, path string) error {
	if strings.TrimSpace(path) != path || path == "" {
		return fmt.Errorf("%s path cannot be empty", name)
	}
	if strings.IndexFunc(path, unicode.IsControl) >= 0 {
		return fmt.Errorf("%s path contains a control character", name)
	}
	clean := filepath.Clean(path)
	if clean == "." || clean == string(filepath.Separator) {
		return fmt.Errorf("%s path is unsafe", name)
	}
	return nil
}

func validateBasePath(value string) error {
	if strings.TrimSpace(value) != value || value == "" || value == "/" {
		return fmt.Errorf("API base path must be a non-root path")
	}
	if strings.ContainsAny(value, "?#%\\\x00\r\n\t ") || !strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") || strings.Contains(value, "//") || hasDotPathSegment(value) {
		return fmt.Errorf("API base path must be an absolute path without a trailing slash or URL control characters")
	}
	return nil
}

func validateRoutePath(name, value string) error {
	if strings.TrimSpace(value) != value || value == "" || value == "/" {
		return fmt.Errorf("%s must be a non-root path", name)
	}
	if strings.ContainsAny(value, "?#%\\\x00\r\n\t ") || !strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") || strings.Contains(value, "//") || hasDotPathSegment(value) {
		return fmt.Errorf("%s must be an absolute path without a trailing slash or URL control characters", name)
	}
	return nil
}

func conflictsWithStaticPath(path string) bool {
	for _, prefix := range []string{"/dashboard", "/player", "/hls", "/llhls"} {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

func hasDotPathSegment(value string) bool {
	for _, segment := range strings.Split(strings.TrimPrefix(value, "/"), "/") {
		if segment == "." || segment == ".." {
			return true
		}
	}
	return false
}

func validateCORSOrigins(origins []string) error {
	seen := make(map[string]struct{}, len(origins))
	wildcard := false
	for _, origin := range origins {
		if origin == "*" {
			if wildcard {
				return fmt.Errorf("duplicate CORS origin %q", origin)
			}
			wildcard = true
			continue
		}
		if origin == "" || strings.TrimSpace(origin) != origin || strings.IndexFunc(origin, func(r rune) bool {
			return unicode.IsControl(r) || unicode.IsSpace(r)
		}) >= 0 {
			return fmt.Errorf("invalid CORS origin %q", origin)
		}
		parsed, err := url.ParseRequestURI(origin)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || strings.HasSuffix(parsed.Host, ":") || parsed.Hostname() == "" || strings.Contains(parsed.Hostname(), "*") || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.ForceQuery || !validURLPort(parsed.Port()) {
			return fmt.Errorf("invalid CORS origin %q", origin)
		}
		if _, exists := seen[origin]; exists {
			return fmt.Errorf("duplicate CORS origin %q", origin)
		}
		seen[origin] = struct{}{}
	}
	if wildcard && len(seen) > 0 {
		return fmt.Errorf("CORS wildcard cannot be combined with explicit origins")
	}
	return nil
}

func validateIdentifier(name, value string) error {
	if strings.TrimSpace(value) != value || value == "" || len(value) > 128 || strings.ContainsAny(value, "/\\\x00\r\n\t ") {
		return fmt.Errorf("invalid %s %q", name, value)
	}
	for index, character := range value {
		asciiLetter := (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z')
		asciiDigit := character >= '0' && character <= '9'
		if index == 0 && !asciiLetter && !asciiDigit {
			return fmt.Errorf("invalid %s %q", name, value)
		}
		if !asciiLetter && !asciiDigit && character != '-' && character != '_' && character != '.' {
			return fmt.Errorf("invalid %s %q", name, value)
		}
	}
	return nil
}

func validURLPort(value string) bool {
	if value == "" {
		return true
	}
	port, err := strconv.ParseUint(value, 10, 16)
	return err == nil && port > 0
}

func validateICEServerURL(value string) error {
	if strings.TrimSpace(value) != value || value == "" || strings.IndexFunc(value, func(r rune) bool {
		return unicode.IsControl(r) || unicode.IsSpace(r)
	}) >= 0 {
		return fmt.Errorf("ICE server URL contains invalid characters")
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return err
	}
	scheme := strings.ToLower(parsed.Scheme)
	switch scheme {
	case "stun", "stuns", "turn", "turns":
	default:
		return fmt.Errorf("unsupported ICE scheme")
	}
	if parsed.User != nil || parsed.Fragment != "" || parsed.Path != "" {
		return fmt.Errorf("ICE server URL must contain only an authority")
	}
	if scheme == "stun" || scheme == "stuns" {
		if parsed.RawQuery != "" || parsed.ForceQuery {
			return fmt.Errorf("STUN URL must not contain a query")
		}
	} else if parsed.RawQuery != "" {
		values, err := url.ParseQuery(parsed.RawQuery)
		if err != nil {
			return fmt.Errorf("invalid TURN query")
		}
		for key, entries := range values {
			if key != "transport" || len(entries) != 1 || (entries[0] != "udp" && entries[0] != "tcp") {
				return fmt.Errorf("invalid TURN query")
			}
		}
	}

	authority := parsed.Host
	if authority == "" {
		authority = parsed.Opaque
	}
	if authority == "" || strings.ContainsAny(authority, "/?#\\@\x00\r\n\t ") {
		return fmt.Errorf("ICE server authority is invalid")
	}
	host := authority
	port := ""
	if parsed.Host != "" {
		host = parsed.Hostname()
		port = parsed.Port()
		if strings.Contains(authority, ":") && port == "" && !strings.HasPrefix(authority, "[") {
			return fmt.Errorf("ICE server port is invalid")
		}
	} else if strings.HasPrefix(authority, "[") {
		closing := strings.IndexByte(authority, ']')
		if closing < 0 {
			return fmt.Errorf("ICE server IPv6 authority is invalid")
		}
		host = authority[1:closing]
		remainder := authority[closing+1:]
		if remainder != "" {
			if !strings.HasPrefix(remainder, ":") || len(remainder) == 1 {
				return fmt.Errorf("ICE server port is invalid")
			}
			port = remainder[1:]
		}
	} else if strings.Count(authority, ":") == 1 {
		var splitErr error
		host, port, splitErr = net.SplitHostPort(authority)
		if splitErr != nil || host == "" || port == "" {
			return fmt.Errorf("ICE server authority is invalid")
		}
	} else if strings.Contains(authority, ":") {
		if net.ParseIP(authority) == nil {
			return fmt.Errorf("ICE server IPv6 authority is invalid")
		}
	}
	if !validURLPort(port) || !validICEHost(host) {
		return fmt.Errorf("ICE server host or port is invalid")
	}
	return nil
}

func validICEHost(host string) bool {
	host = strings.Trim(host, "[]")
	if host == "" || strings.ContainsAny(host, "%/\\?#\x00\r\n\t ") || strings.Contains(host, "*") {
		return false
	}
	if net.ParseIP(host) != nil {
		return true
	}
	if len(host) > 253 || strings.HasPrefix(host, ".") || strings.HasSuffix(host, ".") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '-') {
				return false
			}
		}
	}
	return true
}

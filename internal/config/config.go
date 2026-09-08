package config

import (
	"fmt"
	"os"
	"time"

	"github.com/pion/webrtc/v3"
	"gopkg.in/yaml.v3"
)

// Config holds the server configuration
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	RTMP      RTMPConfig      `yaml:"rtmp"`
	RTSP      RTSPConfig      `yaml:"rtsp"`
	SRT       SRTConfig       `yaml:"srt"`
	WebRTC    WebRTCConfig    `yaml:"webrtc"`
	HLS       HLSConfig       `yaml:"hls"`
	LLHLS     LLHLSConfig     `yaml:"llhls"`
	API       APIConfig       `yaml:"api"`
	Logging   LoggingConfig   `yaml:"logging"`
	Metrics   MetricsConfig   `yaml:"metrics"`
	Recorder  RecorderConfig  `yaml:"recorder"`
	Auth      AuthConfig      `yaml:"auth"`
	Redis     RedisConfig     `yaml:"redis"`
	Postgres  PostgresConfig  `yaml:"postgres"`
	Cluster   ClusterConfig   `yaml:"cluster"`
	Transcoder TranscoderConfig `yaml:"transcoder"`
}

// ServerConfig holds HTTP server configuration
type ServerConfig struct {
	Host         string        `yaml:"host"`
	HTTPPort     int           `yaml:"http_port"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
}

// RTMPConfig holds RTMP server configuration
type RTMPConfig struct {
	Enable       bool          `yaml:"enable"`
	Host         string        `yaml:"host"`
	Port         int           `yaml:"port"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

// HLSConfig holds HLS output configuration
type HLSConfig struct {
	Enable        bool          `yaml:"enable"`
	Path          string        `yaml:"path"`
	SegmentDuration time.Duration `yaml:"segment_duration"`
	PlaylistSize  int           `yaml:"playlist_size"`
}

// APIConfig holds API server configuration
type APIConfig struct {
	Enable      bool   `yaml:"enable"`
	BasePath    string `yaml:"base_path"`
	CORSOrigins []string `yaml:"cors_origins"`
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level      string `yaml:"level"`
	Format     string `yaml:"format"`
	OutputPath string `yaml:"output_path"`
}

// MetricsConfig holds Prometheus metrics configuration
type MetricsConfig struct {
	Enable bool   `yaml:"enable"`
	Path   string `yaml:"path"`
}

// RecorderConfig holds recording configuration
type RecorderConfig struct {
	Enable bool   `yaml:"enable"`
	Path   string `yaml:"path"`
}

// RTSPConfig holds RTSP server configuration
type RTSPConfig struct {
	Enable      bool          `yaml:"enable"`
	Host        string        `yaml:"host"`
	Port        int           `yaml:"port"`
	ReadTimeout time.Duration `yaml:"read_timeout"`
}

// SRTConfig holds SRT server configuration
type SRTConfig struct {
	Enable       bool          `yaml:"enable"`
	Host         string        `yaml:"host"`
	Port         int           `yaml:"port"`
	Latency      time.Duration `yaml:"latency"`
	Passphrase   string        `yaml:"passphrase"`
}

// WebRTCConfig holds WebRTC server configuration
type WebRTCConfig struct {
	Enable     bool               `yaml:"enable"`
	Host       string             `yaml:"host"`
	Port       int                `yaml:"port"`
	ICEServer  webrtc.ICEServer   `yaml:"ice_server"`
	UseTURN    bool               `yaml:"use_turn"`
	TLSEnabled bool               `yaml:"tls_enabled"`
	CertFile   string             `yaml:"cert_file"`
	KeyFile    string             `yaml:"key_file"`
}

// LLHLSConfig holds Low-Latency HLS configuration
type LLHLSConfig struct {
	Enable         bool          `yaml:"enable"`
	SegmentDuration time.Duration `yaml:"segment_duration"`
	PartDuration   time.Duration `yaml:"part_duration"`
	PlaylistSize   int           `yaml:"playlist_size"`
}

// AuthConfig holds authentication configuration
type AuthConfig struct {
	Enable       bool   `yaml:"enable"`
	JWTSecret    string `yaml:"jwt_secret"`
	JWTExpiry    string `yaml:"jwt_expiry"`
	APIKeys      []string `yaml:"api_keys"`
	AllowAnonymous bool `yaml:"allow_anonymous"`
}

// RedisConfig holds Redis configuration
type RedisConfig struct {
	Enable   bool   `yaml:"enable"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	Prefix   string `yaml:"prefix"`
}

// PostgresConfig holds PostgreSQL configuration
type PostgresConfig struct {
	Enable   bool   `yaml:"enable"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	SSLMode  string `yaml:"ssl_mode"`
}

// ClusterConfig holds cluster configuration
type ClusterConfig struct {
	Enable        bool   `yaml:"enable"`
	NodeID        string `yaml:"node_id"`
	DiscoveryAddr string `yaml:"discovery_addr"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval"`
}

// TranscoderConfig holds transcoder configuration
type TranscoderConfig struct {
	Enable       bool              `yaml:"enable"`
	WorkerCount  int               `yaml:"worker_count"`
	FFmpegPath   string            `yaml:"ffmpeg_path"`
	GPUEnabled   bool              `yaml:"gpu_enabled"`
	ABRLadder    []ABRProfile      `yaml:"abr_ladder"`
	OutputFormat string            `yaml:"output_format"`
}

// ABRProfile represents an ABR ladder profile
type ABRProfile struct {
	Name      string `yaml:"name"`
	Width     int    `yaml:"width"`
	Height    int    `yaml:"height"`
	Bitrate   int    `yaml:"bitrate"`
	FrameRate int    `yaml:"frame_rate"`
	AudioBitrate int `yaml:"audio_bitrate"`
}

// RTMPOutputConfig holds RTMP output configuration
type RTMPOutputConfig struct {
	Enable      bool          `yaml:"enable"`
	Host        string        `yaml:"host"`
	Port        int           `yaml:"port"`
	ReadTimeout time.Duration `yaml:"read_timeout"`
}

// RTSPOutputConfig holds RTSP output configuration
type RTSPOutputConfig struct {
	Enable      bool          `yaml:"enable"`
	Host        string        `yaml:"host"`
	Port        int           `yaml:"port"`
	ReadTimeout time.Duration `yaml:"read_timeout"`
}

// SRTOutputConfig holds SRT output configuration
type SRTOutputConfig struct {
	Enable   bool          `yaml:"enable"`
	Host     string        `yaml:"host"`
	Port     int           `yaml:"port"`
	Latency  time.Duration `yaml:"latency"`
}

// DVRConfig holds DVR configuration
type DVRConfig struct {
	Enable      bool          `yaml:"enable"`
	Path        string        `yaml:"path"`
	MaxDuration time.Duration `yaml:"max_duration"`
	Format      string        `yaml:"format"`
}

// DefaultConfig returns a configuration with default values
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Host:         "0.0.0.0",
			HTTPPort:     8080,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  120 * time.Second,
		},
		RTMP: RTMPConfig{
			Enable:       true,
			Host:         "0.0.0.0",
			Port:         1935,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
		RTSP: RTSPConfig{
			Enable:      true,
			Host:        "0.0.0.0",
			Port:        554,
			ReadTimeout: 30 * time.Second,
		},
		SRT: SRTConfig{
			Enable:   true,
			Host:     "0.0.0.0",
			Port:     9000,
			Latency:  200 * time.Millisecond,
		},
		WebRTC: WebRTCConfig{
			Enable:     true,
			Host:       "0.0.0.0",
			Port:       8443,
			TLSEnabled: false,
		},
		HLS: HLSConfig{
			Enable:          true,
			Path:            "/tmp/hls",
			SegmentDuration: 2 * time.Second,
			PlaylistSize:    5,
		},
		LLHLS: LLHLSConfig{
			Enable:          true,
			SegmentDuration: 1 * time.Second,
			PartDuration:    200 * time.Millisecond,
			PlaylistSize:    10,
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
			Enable: false,
			Path:   "/tmp/recordings",
		},
		Auth: AuthConfig{
			Enable:         false,
			JWTSecret:      "change-me-in-production",
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
			Password: "stremdbc",
			Database: "stremdbc",
			SSLMode:  "disable",
		},
		Cluster: ClusterConfig{
			Enable:              false,
			NodeID:              "",
			HealthCheckInterval: 5 * time.Second,
		},
		Transcoder: TranscoderConfig{
			Enable:      false,
			WorkerCount: 2,
			FFmpegPath:  "ffmpeg",
			GPUEnabled:  false,
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

// Load loads configuration from a YAML file
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	return cfg, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Server.HTTPPort <= 0 || c.Server.HTTPPort > 65535 {
		return fmt.Errorf("invalid HTTP port: %d", c.Server.HTTPPort)
	}

	if c.RTMP.Enable && (c.RTMP.Port <= 0 || c.RTMP.Port > 65535) {
		return fmt.Errorf("invalid RTMP port: %d", c.RTMP.Port)
	}

	if c.HLS.Enable && c.HLS.Path == "" {
		return fmt.Errorf("HLS path cannot be empty")
	}

	return nil
}

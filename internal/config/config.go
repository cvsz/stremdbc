package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pion/webrtc/v3"
	"gopkg.in/yaml.v3"
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
	Enable bool   `yaml:"enable"`
	Path   string `yaml:"path"`
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
			Port:        8554,
			ReadTimeout: 30 * time.Second,
		},
		SRT: SRTConfig{
			Enable:  true,
			Host:    "0.0.0.0",
			Port:    9000,
			Latency: 200 * time.Millisecond,
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
			Enable: false,
			Path:   "/tmp/recordings",
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

// Validate validates configuration invariants before listeners are started.
func (c *Config) Validate() error {
	validatePort := func(name string, enabled bool, port int) error {
		if enabled && (port <= 0 || port > 65535) {
			return fmt.Errorf("invalid %s port: %d", name, port)
		}
		return nil
	}

	if err := validatePort("HTTP", true, c.Server.HTTPPort); err != nil {
		return err
	}
	if err := validatePort("RTMP", c.RTMP.Enable, c.RTMP.Port); err != nil {
		return err
	}
	if err := validatePort("RTSP", c.RTSP.Enable, c.RTSP.Port); err != nil {
		return err
	}
	if err := validatePort("SRT", c.SRT.Enable, c.SRT.Port); err != nil {
		return err
	}
	if err := validatePort("WebRTC", c.WebRTC.Enable, c.WebRTC.Port); err != nil {
		return err
	}
	if err := validatePort("RTMP output", c.RTMPOutput.Enable, c.RTMPOutput.Port); err != nil {
		return err
	}
	if err := validatePort("RTSP output", c.RTSPOutput.Enable, c.RTSPOutput.Port); err != nil {
		return err
	}
	if err := validatePort("SRT output", c.SRTOutput.Enable, c.SRTOutput.Port); err != nil {
		return err
	}

	if c.HLS.Enable {
		if strings.TrimSpace(c.HLS.Path) == "" {
			return fmt.Errorf("HLS path cannot be empty")
		}
		if c.HLS.SegmentDuration <= 0 || c.HLS.PlaylistSize <= 0 {
			return fmt.Errorf("HLS segment_duration and playlist_size must be positive")
		}
	}
	if c.LLHLS.Enable {
		if c.LLHLS.SegmentDuration <= 0 || c.LLHLS.PartDuration <= 0 || c.LLHLS.PlaylistSize <= 0 {
			return fmt.Errorf("LL-HLS durations and playlist_size must be positive")
		}
		if c.LLHLS.PartDuration >= c.LLHLS.SegmentDuration {
			return fmt.Errorf("LL-HLS part_duration must be shorter than segment_duration")
		}
	}
	if c.Auth.Enable {
		if len(c.Auth.JWTSecret) < 32 {
			return fmt.Errorf("auth.jwt_secret must be at least 32 characters when auth is enabled")
		}
		if _, err := time.ParseDuration(c.Auth.JWTExpiry); err != nil {
			return fmt.Errorf("invalid auth.jwt_expiry: %w", err)
		}
	}
	if c.Cluster.Enable {
		if !c.Redis.Enable {
			return fmt.Errorf("cluster mode requires redis.enable=true")
		}
		if strings.TrimSpace(c.Cluster.NodeID) == "" {
			return fmt.Errorf("cluster.node_id is required when cluster mode is enabled")
		}
		if c.Cluster.HealthCheckInterval <= 0 {
			return fmt.Errorf("cluster.health_check_interval must be positive")
		}
	}
	if c.Redis.Enable {
		if strings.TrimSpace(c.Redis.Host) == "" || c.Redis.Port <= 0 || c.Redis.Port > 65535 {
			return fmt.Errorf("invalid Redis configuration")
		}
	}
	if c.Postgres.Enable {
		if strings.TrimSpace(c.Postgres.Host) == "" || c.Postgres.Port <= 0 || c.Postgres.Port > 65535 || strings.TrimSpace(c.Postgres.User) == "" || strings.TrimSpace(c.Postgres.Database) == "" {
			return fmt.Errorf("invalid PostgreSQL configuration")
		}
	}
	if c.Transcoder.Enable {
		if c.Transcoder.WorkerCount <= 0 {
			return fmt.Errorf("transcoder.worker_count must be positive")
		}
		if strings.TrimSpace(c.Transcoder.FFmpegPath) == "" {
			return fmt.Errorf("transcoder.ffmpeg_path is required")
		}
		if len(c.Transcoder.ABRLadder) == 0 {
			return fmt.Errorf("transcoder.abr_ladder cannot be empty")
		}
		for _, profile := range c.Transcoder.ABRLadder {
			if profile.Width <= 0 || profile.Height <= 0 || profile.Bitrate <= 0 || profile.FrameRate <= 0 || profile.AudioBitrate <= 0 {
				return fmt.Errorf("invalid ABR profile %q", profile.Name)
			}
		}
	}
	if c.DVR.Enable && strings.TrimSpace(c.DVR.Path) == "" {
		return fmt.Errorf("dvr.path cannot be empty")
	}
	return nil
}

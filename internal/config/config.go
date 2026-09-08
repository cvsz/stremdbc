package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds the server configuration
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	RTMP     RTMPConfig     `yaml:"rtmp"`
	HLS      HLSConfig      `yaml:"hls"`
	API      APIConfig      `yaml:"api"`
	Logging  LoggingConfig  `yaml:"logging"`
	Metrics  MetricsConfig  `yaml:"metrics"`
	Recorder RecorderConfig `yaml:"recorder"`
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
		HLS: HLSConfig{
			Enable:        true,
			Path:          "/tmp/hls",
			SegmentDuration: 2 * time.Second,
			PlaylistSize:  5,
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

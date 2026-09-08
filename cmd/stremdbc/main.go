package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/policedbc/stremdbc/internal/api"
	"github.com/policedbc/stremdbc/internal/auth"
	"github.com/policedbc/stremdbc/internal/cluster"
	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"github.com/policedbc/stremdbc/internal/ingest/rtmp"
	"github.com/policedbc/stremdbc/internal/ingest/rtsp"
	"github.com/policedbc/stremdbc/internal/ingest/srt"
	"github.com/policedbc/stremdbc/internal/ingest/webrtc"
	"github.com/policedbc/stremdbc/internal/metrics"
	"github.com/policedbc/stremdbc/internal/output/hls"
	"github.com/policedbc/stremdbc/internal/recorder"
	"github.com/policedbc/stremdbc/internal/transcoder"
	"go.uber.org/zap"
)

var version = "0.2.0"

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "", "path to configuration file")
	showVersion := flag.Bool("version", false, "show version")
	flag.Parse()

	if *showVersion {
		fmt.Printf("STREMDBC v%s\n", version)
		os.Exit(0)
	}

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	logger, err := initLogger(cfg.Logging)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("starting STREMDBC",
		zap.String("version", version),
		zap.String("config", *configPath),
	)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Initialize components
	metrics := metrics.NewMetrics()
	registry := core.NewStreamRegistry(cfg)

	// Initialize HLS manager
	hlsManager, err := hls.NewOutputManager(&cfg.HLS, logger)
	if err != nil {
		logger.Fatal("failed to create HLS manager", zap.Error(err))
	}

	// Initialize auth manager
	var authManager *auth.Manager
	if cfg.Auth.Enable {
		authManager, err = auth.NewManager(cfg.Auth.JWTSecret, cfg.Auth.JWTExpiry, cfg.Auth.APIKeys, cfg.Auth.AllowAnonymous)
		if err != nil {
			logger.Fatal("failed to create auth manager", zap.Error(err))
		}
		logger.Info("authentication enabled")
	}

	// Initialize recorder
	var recManager *recorder.Manager
	if cfg.Recorder.Enable {
		recManager, err = recorder.NewManager(&cfg.Recorder, logger)
		if err != nil {
			logger.Fatal("failed to create recorder manager", zap.Error(err))
		}
		logger.Info("recording enabled", zap.String("path", cfg.Recorder.Path))
	}

	// Initialize transcoder
	var transManager *transcoder.Manager
	if cfg.Transcoder.Enable {
		transManager, err = transcoder.NewManager(&cfg.Transcoder, logger)
		if err != nil {
			logger.Fatal("failed to create transcoder manager", zap.Error(err))
		}
		if err := transManager.Start(); err != nil {
			logger.Fatal("failed to start transcoder", zap.Error(err))
		}
		logger.Info("transcoding enabled", zap.Int("workers", cfg.Transcoder.WorkerCount))
	}

	// Start RTMP server
	var rtmpServer *rtmp.Server
	if cfg.RTMP.Enable {
		rtmpServer = rtmp.NewServer(&cfg.RTMP, registry, logger)
		if err := rtmpServer.Start(ctx); err != nil {
			logger.Fatal("failed to start RTMP server", zap.Error(err))
		}
		logger.Info("RTMP server started", zap.Int("port", cfg.RTMP.Port))
	}

	// Start RTSP server
	var rtspServer *rtsp.Server
	if cfg.RTSP.Enable {
		rtspServer = rtsp.NewServer(&cfg.RTSP, registry, logger)
		if err := rtspServer.Start(ctx); err != nil {
			logger.Fatal("failed to start RTSP server", zap.Error(err))
		}
		logger.Info("RTSP server started", zap.Int("port", cfg.RTSP.Port))
	}

	// Start SRT server
	var srtServer *srt.Server
	if cfg.SRT.Enable {
		srtServer = srt.NewServer(&cfg.SRT, registry, logger)
		if err := srtServer.Start(ctx); err != nil {
			logger.Fatal("failed to start SRT server", zap.Error(err))
		}
		logger.Info("SRT server started", zap.Int("port", cfg.SRT.Port))
	}

	// Start WebRTC server
	var webrtcServer *webrtc.Server
	if cfg.WebRTC.Enable {
		webrtcServer, err = webrtc.NewServer(&cfg.WebRTC, registry, logger)
		if err != nil {
			logger.Fatal("failed to create WebRTC server", zap.Error(err))
		}
		if err := webrtcServer.Start(ctx); err != nil {
			logger.Fatal("failed to start WebRTC server", zap.Error(err))
		}
		logger.Info("WebRTC server started", zap.Int("port", cfg.WebRTC.Port))
	}

	// Initialize cluster manager
	var clusterManager *cluster.Manager
	if cfg.Cluster.Enable && cfg.Redis.Enable {
		clusterManager, err = cluster.NewManager(&cfg.Cluster, &cfg.Redis, logger)
		if err != nil {
			logger.Fatal("failed to create cluster manager", zap.Error(err))
		}

		node := &cluster.Node{
			ID:          cfg.Cluster.NodeID,
			Host:        cfg.Server.Host,
			HTTPPort:    cfg.Server.HTTPPort,
			RTMPPort:    cfg.RTMP.Port,
			SRTPort:     cfg.SRT.Port,
			WebRTCPPort: cfg.WebRTC.Port,
			State:       "active",
		}

		if err := clusterManager.Start(node); err != nil {
			logger.Fatal("failed to start cluster manager", zap.Error(err))
		}
		logger.Info("cluster mode enabled", zap.String("node_id", cfg.Cluster.NodeID))
	}

	// Setup Gin for web routes
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(gin.Logger())

	// Serve HLS files
	router.Static("/hls", cfg.HLS.Path)

	// Serve player
	router.StaticFile("/player/:streamid", "web/player/index.html")

	// API routes
	apiServer := api.NewServer(&cfg.API, registry, metrics, logger)
	if authManager != nil {
		apiServer.SetAuthManager(authManager)
	}

	// Start API server in goroutine
	go func() {
		addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.HTTPPort)
		if err := apiServer.Start(addr); err != nil {
			logger.Error("API server failed", zap.Error(err))
		}
	}()

	// Start cleanup routines
	registry.Cleanup(ctx, 5*time.Minute)
	hlsManager.Cleanup(ctx, 1*time.Minute)
	if recManager != nil {
		recManager.Cleanup(ctx, 30) // 30 days retention
	}

	logger.Info("STREMDBC started successfully",
		zap.Int("http_port", cfg.Server.HTTPPort),
		zap.Bool("rtmp_enabled", cfg.RTMP.Enable),
		zap.Int("rtmp_port", cfg.RTMP.Port),
		zap.Bool("rtsp_enabled", cfg.RTSP.Enable),
		zap.Int("rtsp_port", cfg.RTSP.Port),
		zap.Bool("srt_enabled", cfg.SRT.Enable),
		zap.Int("srt_port", cfg.SRT.Port),
		zap.Bool("webrtc_enabled", cfg.WebRTC.Enable),
		zap.Int("webrtc_port", cfg.WebRTC.Port),
		zap.Bool("auth_enabled", cfg.Auth.Enable),
		zap.Bool("recording_enabled", cfg.Recorder.Enable),
		zap.Bool("transcoding_enabled", cfg.Transcoder.Enable),
		zap.Bool("cluster_enabled", cfg.Cluster.Enable),
	)

	// Wait for shutdown signal
	sig := <-sigChan
	logger.Info("shutdown signal received", zap.String("signal", sig.String()))

	// Graceful shutdown
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if rtmpServer != nil {
		if err := rtmpServer.Stop(shutdownCtx); err != nil {
			logger.Error("RTMP server shutdown error", zap.Error(err))
		}
	}

	if rtspServer != nil {
		if err := rtspServer.Stop(shutdownCtx); err != nil {
			logger.Error("RTSP server shutdown error", zap.Error(err))
		}
	}

	if srtServer != nil {
		if err := srtServer.Stop(shutdownCtx); err != nil {
			logger.Error("SRT server shutdown error", zap.Error(err))
		}
	}

	if webrtcServer != nil {
		if err := webrtcServer.Stop(shutdownCtx); err != nil {
			logger.Error("WebRTC server shutdown error", zap.Error(err))
		}
	}

	if transManager != nil {
		if err := transManager.Stop(); err != nil {
			logger.Error("Transcoder shutdown error", zap.Error(err))
		}
	}

	if clusterManager != nil {
		if err := clusterManager.Stop(); err != nil {
			logger.Error("Cluster manager shutdown error", zap.Error(err))
		}
	}

	logger.Info("STREMDBC stopped")
}

func initLogger(cfg config.LoggingConfig) (*zap.Logger, error) {
	var loggerConfig zap.Config

	switch cfg.Format {
	case "json":
		loggerConfig = zap.NewProductionConfig()
	default:
		loggerConfig = zap.NewDevelopmentConfig()
	}

	loggerConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	switch cfg.Level {
	case "debug":
		loggerConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		loggerConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		loggerConfig.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		loggerConfig.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	}

	if cfg.OutputPath != "" && cfg.OutputPath != "stdout" {
		loggerConfig.OutputPaths = []string{cfg.OutputPath}
	}

	return loggerConfig.Build()
}

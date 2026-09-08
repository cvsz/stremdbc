package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/policedbc/stremdbc/internal/api"
	"github.com/policedbc/stremdbc/internal/auth"
	"github.com/policedbc/stremdbc/internal/cluster"
	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"github.com/policedbc/stremdbc/internal/dvr"
	rtmpingest "github.com/policedbc/stremdbc/internal/ingest/rtmp"
	rtspingest "github.com/policedbc/stremdbc/internal/ingest/rtsp"
	srtingest "github.com/policedbc/stremdbc/internal/ingest/srt"
	webrtcingest "github.com/policedbc/stremdbc/internal/ingest/webrtc"
	"github.com/policedbc/stremdbc/internal/metrics"
	"github.com/policedbc/stremdbc/internal/output/hls"
	"github.com/policedbc/stremdbc/internal/output/llhls"
	rtmpoutput "github.com/policedbc/stremdbc/internal/output/rtmp"
	rtspoutput "github.com/policedbc/stremdbc/internal/output/rtsp"
	srtoutput "github.com/policedbc/stremdbc/internal/output/srt"
	"github.com/policedbc/stremdbc/internal/recorder"
	"github.com/policedbc/stremdbc/internal/transcoder"
	"go.uber.org/zap"
)

var version = "0.6.0"

func main() {
	configPath := flag.String("config", "", "path to configuration file")
	showVersion := flag.Bool("version", false, "show version")
	flag.Parse()

	if *showVersion {
		fmt.Printf("STREMDBC v%s\n", version)
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	logger, err := initLogger(cfg.Logging)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = logger.Sync() }()

	logger.Info("starting STREMDBC", zap.String("version", version), zap.String("config", *configPath))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry := core.NewStreamRegistry(cfg)
	metricSet := metrics.NewMetrics()

	var hlsManager *hls.OutputManager
	if cfg.HLS.Enable {
		hlsManager, err = hls.NewOutputManager(&cfg.HLS, logger)
		fatalIf(logger, err, "failed to create HLS manager")
	}

	var llhlsManager *llhls.Manager
	if cfg.LLHLS.Enable {
		llhlsManager, err = llhls.NewManager(&cfg.LLHLS, registry, logger)
		fatalIf(logger, err, "failed to create LL-HLS manager")
		fatalIf(logger, llhlsManager.Start(ctx), "failed to start LL-HLS manager")
	}

	var authManager *auth.Manager
	if cfg.Auth.Enable {
		authManager, err = auth.NewManager(cfg.Auth.JWTSecret, cfg.Auth.JWTExpiry, cfg.Auth.APIKeys, cfg.Auth.AllowAnonymous)
		fatalIf(logger, err, "failed to create auth manager")
		logger.Info("authentication enabled", zap.Bool("allow_anonymous", cfg.Auth.AllowAnonymous))
	}

	var recManager *recorder.Manager
	if cfg.Recorder.Enable {
		recManager, err = recorder.NewManager(&cfg.Recorder, logger)
		fatalIf(logger, err, "failed to create recorder manager")
		logger.Info("recording enabled", zap.String("path", cfg.Recorder.Path))
	}

	var dvrManager *dvr.Manager
	if cfg.DVR.Enable {
		dvrManager, err = dvr.NewManager(&cfg.DVR, registry, logger)
		fatalIf(logger, err, "failed to create DVR manager")
		fatalIf(logger, dvrManager.Start(ctx), "failed to start DVR manager")
	}

	var transManager *transcoder.Manager
	if cfg.Transcoder.Enable {
		transManager, err = transcoder.NewManager(&cfg.Transcoder, logger)
		fatalIf(logger, err, "failed to create transcoder manager")
		fatalIf(logger, transManager.Start(), "failed to start transcoder")
		logger.Info("transcoding enabled", zap.Int("workers", cfg.Transcoder.WorkerCount))
	}

	var rtmpServer *rtmpingest.Server
	if cfg.RTMP.Enable {
		rtmpServer = rtmpingest.NewServer(&cfg.RTMP, registry, logger)
		fatalIf(logger, rtmpServer.Start(ctx), "failed to start RTMP server")
	}

	var rtspServer *rtspingest.Server
	if cfg.RTSP.Enable {
		rtspServer = rtspingest.NewServer(&cfg.RTSP, registry, logger)
		fatalIf(logger, rtspServer.Start(ctx), "failed to start RTSP server")
	}

	var srtServer *srtingest.Server
	if cfg.SRT.Enable {
		srtServer = srtingest.NewServer(&cfg.SRT, registry, logger)
		fatalIf(logger, srtServer.Start(ctx), "failed to start SRT server")
	}

	var webrtcServer *webrtcingest.Server
	if cfg.WebRTC.Enable {
		webrtcServer, err = webrtcingest.NewServer(&cfg.WebRTC, registry, logger)
		fatalIf(logger, err, "failed to create WebRTC server")
		fatalIf(logger, webrtcServer.Start(ctx), "failed to start WebRTC server")
	}

	var rtmpOutputServer *rtmpoutput.Server
	if cfg.RTMPOutput.Enable {
		rtmpOutputServer = rtmpoutput.NewServer(&cfg.RTMPOutput, registry, logger)
		fatalIf(logger, rtmpOutputServer.Start(ctx), "failed to start RTMP output server")
	}

	var rtspOutputServer *rtspoutput.Server
	if cfg.RTSPOutput.Enable {
		rtspOutputServer = rtspoutput.NewServer(&cfg.RTSPOutput, registry, logger)
		fatalIf(logger, rtspOutputServer.Start(ctx), "failed to start RTSP output server")
	}

	var srtOutputServer *srtoutput.Server
	if cfg.SRTOutput.Enable {
		srtOutputServer = srtoutput.NewServer(&cfg.SRTOutput, registry, logger)
		fatalIf(logger, srtOutputServer.Start(ctx), "failed to start SRT output server")
	}

	var clusterManager *cluster.Manager
	if cfg.Cluster.Enable {
		clusterManager, err = cluster.NewManager(&cfg.Cluster, &cfg.Redis, logger)
		fatalIf(logger, err, "failed to create cluster manager")
		node := &cluster.Node{
			ID:          cfg.Cluster.NodeID,
			Host:        cfg.Server.Host,
			HTTPPort:    cfg.Server.HTTPPort,
			RTMPPort:    cfg.RTMP.Port,
			SRTPort:     cfg.SRT.Port,
			WebRTCPPort: cfg.WebRTC.Port,
			State:       "active",
		}
		fatalIf(logger, clusterManager.Start(node), "failed to start cluster manager")
	}

	apiServer := api.NewServer(&cfg.API, registry, metricSet, logger)
	apiServer.SetVersion(version)
	if authManager != nil {
		apiServer.SetAuthManager(authManager)
	}

	llhlsPath := ""
	if llhlsManager != nil {
		llhlsPath = llhlsManager.GetOutputPath()
	}
	apiServer.SetStaticRoutes(cfg.HLS.Path, llhlsPath, "web/player/index.html", "web/dashboard")
	if recManager != nil {
		apiServer.RegisterStats("recorder", recManager.GetStats)
	}
	if dvrManager != nil {
		apiServer.RegisterStats("dvr", dvrManager.GetStats)
	}
	if transManager != nil {
		apiServer.RegisterStats("transcoder", transManager.GetStats)
	}
	if llhlsManager != nil {
		apiServer.RegisterStats("llhls", llhlsManager.GetStats)
	}
	if clusterManager != nil {
		apiServer.RegisterStats("cluster", func() map[string]interface{} {
			return map[string]interface{}{"nodes": clusterManager.GetNodes(), "active_nodes": len(clusterManager.GetActiveNodes())}
		})
	}

	serverErr := make(chan error, 1)
	if cfg.API.Enable {
		go func() {
			addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.HTTPPort)
			if err := apiServer.Start(addr); err != nil {
				serverErr <- err
			}
		}()
	}

	registry.Cleanup(ctx, 5*time.Minute)
	if hlsManager != nil {
		hlsManager.Cleanup(ctx, time.Minute)
	}
	if recManager != nil {
		recManager.Cleanup(ctx, 30)
	}

	logger.Info("STREMDBC started successfully",
		zap.Int("http_port", cfg.Server.HTTPPort),
		zap.Bool("rtmp_enabled", cfg.RTMP.Enable),
		zap.Bool("rtsp_enabled", cfg.RTSP.Enable),
		zap.Bool("srt_enabled", cfg.SRT.Enable),
		zap.Bool("webrtc_enabled", cfg.WebRTC.Enable),
		zap.Bool("llhls_enabled", cfg.LLHLS.Enable),
		zap.Bool("auth_enabled", cfg.Auth.Enable),
		zap.Bool("recording_enabled", cfg.Recorder.Enable),
		zap.Bool("dvr_enabled", cfg.DVR.Enable),
		zap.Bool("transcoding_enabled", cfg.Transcoder.Enable),
		zap.Bool("cluster_enabled", cfg.Cluster.Enable),
	)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		logger.Info("shutdown signal received", zap.String("signal", sig.String()))
	case err := <-serverErr:
		logger.Error("HTTP server failed", zap.Error(err))
	}

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if cfg.API.Enable {
		logShutdown(logger, "HTTP API", apiServer.Stop(shutdownCtx))
	}
	if rtmpServer != nil {
		logShutdown(logger, "RTMP", rtmpServer.Stop(shutdownCtx))
	}
	if rtspServer != nil {
		logShutdown(logger, "RTSP", rtspServer.Stop(shutdownCtx))
	}
	if srtServer != nil {
		logShutdown(logger, "SRT", srtServer.Stop(shutdownCtx))
	}
	if webrtcServer != nil {
		logShutdown(logger, "WebRTC", webrtcServer.Stop(shutdownCtx))
	}
	if rtmpOutputServer != nil {
		logShutdown(logger, "RTMP output", rtmpOutputServer.Stop(shutdownCtx))
	}
	if rtspOutputServer != nil {
		logShutdown(logger, "RTSP output", rtspOutputServer.Stop(shutdownCtx))
	}
	if srtOutputServer != nil {
		logShutdown(logger, "SRT output", srtOutputServer.Stop(shutdownCtx))
	}
	if llhlsManager != nil {
		logShutdown(logger, "LL-HLS", llhlsManager.Stop())
	}
	if dvrManager != nil {
		logShutdown(logger, "DVR", dvrManager.Stop())
	}
	if recManager != nil {
		for _, recording := range recManager.ListRecordings() {
			if recording.State == "recording" || recording.State == "starting" {
				logShutdown(logger, "recorder "+recording.StreamID, recManager.StopRecording(recording.StreamID))
			}
		}
	}
	if transManager != nil {
		logShutdown(logger, "transcoder", transManager.Stop())
	}
	if clusterManager != nil {
		logShutdown(logger, "cluster", clusterManager.Stop())
	}

	logger.Info("STREMDBC stopped")
}

func fatalIf(logger *zap.Logger, err error, message string) {
	if err != nil {
		logger.Fatal(message, zap.Error(err))
	}
}

func logShutdown(logger *zap.Logger, component string, err error) {
	if err != nil {
		logger.Error("component shutdown error", zap.String("component", component), zap.Error(err))
	}
}

func initLogger(cfg config.LoggingConfig) (*zap.Logger, error) {
	var loggerConfig zap.Config
	if cfg.Format == "json" {
		loggerConfig = zap.NewProductionConfig()
	} else {
		loggerConfig = zap.NewDevelopmentConfig()
	}

	switch cfg.Level {
	case "debug":
		loggerConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "warn":
		loggerConfig.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		loggerConfig.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		loggerConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	if cfg.OutputPath != "" && cfg.OutputPath != "stdout" {
		loggerConfig.OutputPaths = []string{cfg.OutputPath}
		loggerConfig.ErrorOutputPaths = []string{cfg.OutputPath}
	}
	return loggerConfig.Build()
}

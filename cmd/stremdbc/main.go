package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
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
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "STREMDBC failed: %v\n", err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	configPath := flag.String("config", "", "path to configuration file")
	showVersion := flag.Bool("version", false, "show version")
	flag.Parse()
	if *showVersion {
		fmt.Printf("STREMDBC v%s\n", version)
		return nil
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	logger, err := initLogger(cfg.Logging)
	if err != nil {
		return fmt.Errorf("initialize logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()
	logger.Info("starting STREMDBC", zap.String("version", version), zap.String("config", *configPath))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := core.NewStreamRegistry(cfg)
	metricSet := metrics.NewMetrics()

	var (
		hlsManager       *hls.OutputManager
		llhlsManager     *llhls.Manager
		authManager      *auth.Manager
		recManager       *recorder.Manager
		dvrManager       *dvr.Manager
		transManager     *transcoder.Manager
		rtmpServer       *rtmpingest.Server
		rtspServer       *rtspingest.Server
		srtServer        *srtingest.Server
		webrtcServer     *webrtcingest.Server
		rtmpOutputServer *rtmpoutput.Server
		rtspOutputServer *rtspoutput.Server
		srtOutputServer  *srtoutput.Server
		clusterManager   *cluster.Manager
		apiServer        *api.Server
	)
	shutdownCalled := false
	var shutdownErr error
	shutdown := func(shutdownCtx context.Context) error {
		if shutdownCalled {
			return shutdownErr
		}
		shutdownCalled = true
		shutdownErr = newShutdown(logger, cancel, apiServer, rtmpServer, rtspServer, srtServer, webrtcServer, rtmpOutputServer, rtspOutputServer, srtOutputServer, llhlsManager, dvrManager, recManager, transManager, clusterManager)(shutdownCtx)
		return shutdownErr
	}
	defer func() {
		if runErr != nil {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer shutdownCancel()
			_ = shutdown(shutdownCtx)
		}
	}()

	if cfg.HLS.Enable {
		hlsManager, err = hls.NewOutputManager(&cfg.HLS, logger)
		if err != nil {
			return fmt.Errorf("create HLS manager: %w", err)
		}
	}

	if cfg.LLHLS.Enable {
		llhlsManager, err = llhls.NewManager(&cfg.LLHLS, registry, logger)
		if err != nil {
			return fmt.Errorf("create LL-HLS manager: %w", err)
		}
		if err := llhlsManager.Start(ctx); err != nil {
			return fmt.Errorf("start LL-HLS manager: %w", err)
		}
	}

	if cfg.Auth.Enable {
		authManager, err = auth.NewManager(cfg.Auth.JWTSecret, cfg.Auth.JWTExpiry, cfg.Auth.APIKeys, cfg.Auth.AllowAnonymous)
		if err != nil {
			return fmt.Errorf("create auth manager: %w", err)
		}
		logger.Info("authentication enabled", zap.Bool("allow_anonymous", cfg.Auth.AllowAnonymous))
	}

	if cfg.Recorder.Enable {
		recManager, err = recorder.NewManager(&cfg.Recorder, logger)
		if err != nil {
			return fmt.Errorf("create recorder manager: %w", err)
		}
		logger.Info("recorder manager initialized; media-source integration is external", zap.String("path", cfg.Recorder.Path))
	}

	if cfg.DVR.Enable {
		dvrManager, err = dvr.NewManager(&cfg.DVR, registry, logger)
		if err != nil {
			return fmt.Errorf("create DVR manager: %w", err)
		}
		if err := dvrManager.Start(ctx); err != nil {
			return fmt.Errorf("start DVR manager: %w", err)
		}
	}

	if cfg.Transcoder.Enable {
		transManager, err = transcoder.NewManager(&cfg.Transcoder, logger)
		if err != nil {
			return fmt.Errorf("create transcoder manager: %w", err)
		}
		if err := transManager.Start(); err != nil {
			return fmt.Errorf("start transcoder: %w", err)
		}
		logger.Info("transcoder manager initialized; job submission is external", zap.Int("workers", cfg.Transcoder.WorkerCount))
	}

	if cfg.RTMP.Enable {
		rtmpServer = rtmpingest.NewServer(&cfg.RTMP, registry, logger)
		if err := rtmpServer.Start(ctx); err != nil {
			return fmt.Errorf("start RTMP server: %w", err)
		}
	}

	if cfg.RTSP.Enable {
		rtspServer = rtspingest.NewServer(&cfg.RTSP, registry, logger)
		if err := rtspServer.Start(ctx); err != nil {
			return fmt.Errorf("start RTSP server: %w", err)
		}
	}

	if cfg.SRT.Enable {
		srtServer = srtingest.NewServer(&cfg.SRT, registry, logger)
		if err := srtServer.Start(ctx); err != nil {
			return fmt.Errorf("start SRT server: %w", err)
		}
	}

	if cfg.WebRTC.Enable {
		webrtcServer, err = webrtcingest.NewServer(&cfg.WebRTC, registry, logger)
		if err != nil {
			return fmt.Errorf("create WebRTC server: %w", err)
		}
		webrtcServer.SetAuthManager(authManager)
		if err := webrtcServer.Start(ctx); err != nil {
			return fmt.Errorf("start WebRTC server: %w", err)
		}
	}

	if cfg.RTMPOutput.Enable {
		rtmpOutputServer = rtmpoutput.NewServer(&cfg.RTMPOutput, registry, logger)
		if err := rtmpOutputServer.Start(ctx); err != nil {
			return fmt.Errorf("start RTMP output server: %w", err)
		}
	}

	if cfg.RTSPOutput.Enable {
		rtspOutputServer = rtspoutput.NewServer(&cfg.RTSPOutput, registry, logger)
		if err := rtspOutputServer.Start(ctx); err != nil {
			return fmt.Errorf("start RTSP output server: %w", err)
		}
	}

	if cfg.SRTOutput.Enable {
		srtOutputServer = srtoutput.NewServer(&cfg.SRTOutput, registry, logger)
		if err := srtOutputServer.Start(ctx); err != nil {
			return fmt.Errorf("start SRT output server: %w", err)
		}
	}

	if cfg.Cluster.Enable {
		clusterManager, err = cluster.NewManager(&cfg.Cluster, &cfg.Redis, logger)
		if err != nil {
			return fmt.Errorf("create cluster manager: %w", err)
		}
		nodeHost := cfg.Cluster.AdvertiseHost
		node := &cluster.Node{ID: cfg.Cluster.NodeID, Host: nodeHost, HTTPPort: enabledPort(cfg.API.Enable, cfg.Server.HTTPPort), RTMPPort: enabledPort(cfg.RTMP.Enable, cfg.RTMP.Port), SRTPort: enabledPort(cfg.SRT.Enable, cfg.SRT.Port), WebRTCPPort: enabledPort(cfg.WebRTC.Enable, cfg.WebRTC.Port), State: "active"}
		if err := clusterManager.Start(node); err != nil {
			return fmt.Errorf("start cluster manager: %w", err)
		}
	}

	apiServer = api.NewServer(&cfg.API, registry, metricSet, logger)
	apiServer.SetVersion(version)
	apiServer.SetHTTPTimeouts(cfg.Server.ReadTimeout, cfg.Server.WriteTimeout, cfg.Server.IdleTimeout)
	apiServer.SetMetricsConfig(cfg.Metrics.Enable, cfg.Metrics.Path)
	apiServer.SetAuthManager(authManager)
	hlsPath := ""
	if hlsManager != nil {
		hlsPath = cfg.HLS.Path
	}
	llhlsPath := ""
	if llhlsManager != nil {
		llhlsPath = llhlsManager.GetOutputPath()
	}
	apiServer.SetStaticRoutes(hlsPath, llhlsPath, resolveAssetPath(filepath.Join("web", "player", "index.html")), resolveAssetPath(filepath.Join("web", "dashboard")))
	registerComponentStats(apiServer, hlsManager, llhlsManager, recManager, dvrManager, transManager, clusterManager)

	registry.Cleanup(ctx, 5*time.Minute)
	if hlsManager != nil {
		hlsManager.Cleanup(ctx, time.Minute)
	}
	if llhlsManager != nil {
		llhlsManager.Cleanup(ctx, time.Minute)
	}
	if recManager != nil {
		recManager.Cleanup(ctx, 30)
	}

	var serverErr <-chan error
	if cfg.API.Enable {
		addr := net.JoinHostPort(cfg.Server.Host, fmt.Sprintf("%d", cfg.Server.HTTPPort))
		serverErr, err = apiServer.StartAsync(addr)
		if err != nil {
			return fmt.Errorf("start HTTP API: %w", err)
		}
	}
	logger.Info("STREMDBC started successfully", zap.Int("http_port", cfg.Server.HTTPPort), zap.Bool("rtmp_control_enabled", cfg.RTMP.Enable), zap.Bool("rtsp_control_enabled", cfg.RTSP.Enable), zap.Bool("srt_control_enabled", cfg.SRT.Enable), zap.Bool("webrtc_signaling_enabled", cfg.WebRTC.Enable), zap.Bool("hls_writer_initialized", cfg.HLS.Enable), zap.Bool("llhls_writer_initialized", cfg.LLHLS.Enable), zap.Bool("auth_enabled", cfg.Auth.Enable), zap.Bool("recorder_manager_initialized", cfg.Recorder.Enable), zap.Bool("dvr_manager_initialized", cfg.DVR.Enable), zap.Bool("transcoder_manager_initialized", cfg.Transcoder.Enable), zap.Bool("cluster_enabled", cfg.Cluster.Enable))

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)
	select {
	case sig := <-sigChan:
		logger.Info("shutdown signal received", zap.String("signal", sig.String()))
	case err, ok := <-serverErr:
		if ok && err != nil {
			return fmt.Errorf("HTTP server failed: %w", err)
		}
		return fmt.Errorf("HTTP server stopped unexpectedly")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	return shutdown(shutdownCtx)
}

func registerComponentStats(server *api.Server, hlsManager *hls.OutputManager, llhlsManager *llhls.Manager, recManager *recorder.Manager, dvrManager *dvr.Manager, transManager *transcoder.Manager, clusterManager *cluster.Manager) {
	if hlsManager != nil {
		server.RegisterStats("hls", hlsManager.GetStats)
	}
	if llhlsManager != nil {
		server.RegisterStats("llhls", llhlsManager.GetStats)
	}
	if recManager != nil {
		server.RegisterStats("recorder", recManager.GetStats)
	}
	if dvrManager != nil {
		server.RegisterStats("dvr", dvrManager.GetStats)
	}
	if transManager != nil {
		server.RegisterStats("transcoder", transManager.GetStats)
	}
	if clusterManager != nil {
		server.RegisterStats("cluster", func() map[string]interface{} {
			return map[string]interface{}{"nodes": clusterManager.GetNodes(), "active_nodes": len(clusterManager.GetActiveNodes())}
		})
	}
}

func newShutdown(logger *zap.Logger, cancel context.CancelFunc, apiServer *api.Server, rtmpServer *rtmpingest.Server, rtspServer *rtspingest.Server, srtServer *srtingest.Server, webrtcServer *webrtcingest.Server, rtmpOutputServer *rtmpoutput.Server, rtspOutputServer *rtspoutput.Server, srtOutputServer *srtoutput.Server, llhlsManager *llhls.Manager, dvrManager *dvr.Manager, recManager *recorder.Manager, transManager *transcoder.Manager, clusterManager *cluster.Manager) func(context.Context) error {
	if logger == nil {
		logger = zap.NewNop()
	}
	called := false
	var result error
	var shutdownMu sync.Mutex
	return func(ctx context.Context) error {
		shutdownMu.Lock()
		defer shutdownMu.Unlock()
		if called {
			return result
		}
		called = true
		if cancel != nil {
			cancel()
		}
		if ctx == nil {
			ctx = context.Background()
		}
		stop := func(name string, fn func() error) {
			if fn == nil {
				return
			}
			if err := fn(); err != nil {
				logger.Error("component shutdown error", zap.String("component", name), zap.Error(err))
				result = errors.Join(result, fmt.Errorf("%s: %w", name, err))
			}
		}
		stop("HTTP API", func() error {
			if apiServer == nil {
				return nil
			}
			return apiServer.Stop(ctx)
		})
		stop("RTMP", func() error {
			if rtmpServer == nil {
				return nil
			}
			return rtmpServer.Stop(ctx)
		})
		stop("RTSP", func() error {
			if rtspServer == nil {
				return nil
			}
			return rtspServer.Stop(ctx)
		})
		stop("SRT", func() error {
			if srtServer == nil {
				return nil
			}
			return srtServer.Stop(ctx)
		})
		stop("WebRTC", func() error {
			if webrtcServer == nil {
				return nil
			}
			return webrtcServer.Stop(ctx)
		})
		stop("RTMP output", func() error {
			if rtmpOutputServer == nil {
				return nil
			}
			return rtmpOutputServer.Stop(ctx)
		})
		stop("RTSP output", func() error {
			if rtspOutputServer == nil {
				return nil
			}
			return rtspOutputServer.Stop(ctx)
		})
		stop("SRT output", func() error {
			if srtOutputServer == nil {
				return nil
			}
			return srtOutputServer.Stop(ctx)
		})
		stop("LL-HLS", func() error {
			if llhlsManager == nil {
				return nil
			}
			return llhlsManager.Stop()
		})
		stop("DVR", func() error {
			if dvrManager == nil {
				return nil
			}
			return dvrManager.Stop()
		})
		stop("recorder", func() error {
			if recManager == nil {
				return nil
			}
			return recManager.Stop(ctx)
		})
		stop("transcoder", func() error {
			if transManager == nil {
				return nil
			}
			return transManager.Stop()
		})
		stop("cluster", func() error {
			if clusterManager == nil {
				return nil
			}
			return clusterManager.Stop()
		})
		return result
	}
}

func resolveAssetPath(relative string) string {
	if filepath.IsAbs(relative) {
		return relative
	}
	if _, err := os.Stat(relative); err == nil {
		return relative
	}
	executable, err := os.Executable()
	if err == nil {
		candidate := filepath.Join(filepath.Dir(executable), relative)
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return relative
}

func enabledPort(enabled bool, port int) int {
	if !enabled {
		return 0
	}
	return port
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
	switch cfg.OutputPath {
	case "", "stdout":
		loggerConfig.OutputPaths = []string{"stdout"}
		loggerConfig.ErrorOutputPaths = []string{"stdout"}
	default:
		loggerConfig.OutputPaths = []string{cfg.OutputPath}
		loggerConfig.ErrorOutputPaths = []string{cfg.OutputPath}
	}
	return loggerConfig.Build()
}

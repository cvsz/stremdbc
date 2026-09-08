package transcoder

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"go.uber.org/zap"
)

type Worker struct {
	id      int
	config  *config.TranscoderConfig
	logger  *zap.Logger
	encoder string
	cmd     *exec.Cmd
	mu      sync.Mutex
	running bool
	stats   WorkerStats
}

type WorkerStats struct {
	StartTime      time.Time
	BytesProcessed int64
	FramesEncoded  int64
	Errors         int64
	JobsCompleted  int64
}

type Manager struct {
	config  *config.TranscoderConfig
	logger  *zap.Logger
	workers []*Worker
	jobs    chan *TranscodeJob
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	encoder string
}

type TranscodeJob struct {
	ID         string
	InputPath  string
	OutputDir  string
	StreamID   string
	Profiles   []config.ABRProfile
	OnComplete func(error)
}

type HardwareInfo struct {
	FFmpegPath       string   `json:"ffmpeg_path"`
	SelectedEncoder  string   `json:"selected_encoder"`
	HardwareEncoders []string `json:"hardware_encoders"`
}

func NewManager(cfg *config.TranscoderConfig, logger *zap.Logger) (*Manager, error) {
	resolved, err := exec.LookPath(cfg.FFmpegPath)
	if err != nil {
		return nil, fmt.Errorf("ffmpeg not found at %q: %w", cfg.FFmpegPath, err)
	}

	hardware := DetectHardware(resolved)
	encoder := "libx264"
	if cfg.GPUEnabled && len(hardware.HardwareEncoders) > 0 {
		encoder = hardware.HardwareEncoders[0]
	}

	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		config:  cfg,
		logger:  logger.Named("transcoder"),
		workers: make([]*Worker, 0, cfg.WorkerCount),
		jobs:    make(chan *TranscodeJob, cfg.WorkerCount*2),
		ctx:     ctx,
		cancel:  cancel,
		encoder: encoder,
	}

	for i := 0; i < cfg.WorkerCount; i++ {
		m.workers = append(m.workers, &Worker{
			id:      i,
			config:  cfg,
			logger:  logger.Named(fmt.Sprintf("worker-%d", i)),
			encoder: encoder,
		})
	}

	m.logger.Info("transcoder initialized",
		zap.String("ffmpeg", resolved),
		zap.String("encoder", encoder),
		zap.Strings("detected_hardware_encoders", hardware.HardwareEncoders),
	)
	return m, nil
}

func DetectHardware(ffmpegPath string) HardwareInfo {
	info := HardwareInfo{FFmpegPath: ffmpegPath, SelectedEncoder: "libx264"}
	cmd := exec.Command(ffmpegPath, "-hide_banner", "-encoders")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return info
	}

	text := string(output)
	candidates := []string{"h264_nvenc", "h264_qsv", "h264_amf", "h264_videotoolbox"}
	for _, encoder := range candidates {
		if strings.Contains(text, encoder) {
			info.HardwareEncoders = append(info.HardwareEncoders, encoder)
		}
	}
	if len(info.HardwareEncoders) > 0 {
		info.SelectedEncoder = info.HardwareEncoders[0]
	}
	return info
}

func (m *Manager) Start() error {
	m.logger.Info("starting transcoder manager", zap.Int("worker_count", len(m.workers)))
	for _, worker := range m.workers {
		m.wg.Add(1)
		go func(w *Worker) {
			defer m.wg.Done()
			w.processJobs(m.ctx, m.jobs)
		}(worker)
	}
	return nil
}

func (m *Manager) Stop() error {
	m.logger.Info("stopping transcoder manager")
	m.cancel()
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(15 * time.Second):
		return fmt.Errorf("timeout waiting for transcoder workers to stop")
	}
}

func (m *Manager) Submit(job *TranscodeJob) error {
	if job == nil || job.ID == "" || job.StreamID == "" || job.InputPath == "" || job.OutputDir == "" {
		return fmt.Errorf("invalid transcoding job")
	}
	if len(job.Profiles) == 0 {
		job.Profiles = m.config.ABRLadder
	}

	select {
	case <-m.ctx.Done():
		return fmt.Errorf("transcoder is stopping")
	case m.jobs <- job:
		m.logger.Info("transcoding job submitted", zap.String("job_id", job.ID), zap.String("stream_id", job.StreamID))
		return nil
	default:
		return fmt.Errorf("job queue is full")
	}
}

func (m *Manager) GetStats() map[string]interface{} {
	total := WorkerStats{}
	activeWorkers := 0
	for _, w := range m.workers {
		w.mu.Lock()
		total.BytesProcessed += w.stats.BytesProcessed
		total.FramesEncoded += w.stats.FramesEncoded
		total.Errors += w.stats.Errors
		total.JobsCompleted += w.stats.JobsCompleted
		if w.running {
			activeWorkers++
		}
		w.mu.Unlock()
	}
	return map[string]interface{}{
		"worker_count":     len(m.workers),
		"active_workers":   activeWorkers,
		"queue_size":       len(m.jobs),
		"encoder":          m.encoder,
		"bytes_processed":  total.BytesProcessed,
		"frames_encoded":   total.FramesEncoded,
		"jobs_completed":   total.JobsCompleted,
		"errors":           total.Errors,
	}
}

func (w *Worker) processJobs(ctx context.Context, jobs <-chan *TranscodeJob) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-jobs:
			if job != nil {
				w.executeJob(ctx, job)
			}
		}
	}
}

func (w *Worker) executeJob(ctx context.Context, job *TranscodeJob) {
	w.mu.Lock()
	w.running = true
	w.stats.StartTime = time.Now()
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.running = false
		w.mu.Unlock()
	}()

	if err := os.MkdirAll(job.OutputDir, 0o755); err != nil {
		w.finishJob(job, fmt.Errorf("create output directory: %w", err))
		return
	}

	profiles := job.Profiles
	if len(profiles) == 0 {
		profiles = w.config.ABRLadder
	}
	for _, profile := range profiles {
		if err := ctx.Err(); err != nil {
			w.finishJob(job, err)
			return
		}
		args := w.buildFFmpegArgs(job, profile)
		cmd := exec.CommandContext(ctx, w.config.FFmpegPath, args...)
		w.mu.Lock()
		w.cmd = cmd
		w.mu.Unlock()

		output, err := cmd.CombinedOutput()
		if err != nil {
			w.logger.Error("transcoding profile failed",
				zap.String("job_id", job.ID),
				zap.String("profile", profile.Name),
				zap.Error(err),
				zap.String("ffmpeg_output", string(output)),
			)
			w.finishJob(job, err)
			return
		}
	}

	w.mu.Lock()
	w.stats.JobsCompleted++
	w.mu.Unlock()
	w.logger.Info("transcoding completed", zap.String("job_id", job.ID), zap.Int("profiles", len(profiles)))
	if job.OnComplete != nil {
		job.OnComplete(nil)
	}
}

func (w *Worker) finishJob(job *TranscodeJob, err error) {
	w.mu.Lock()
	w.stats.Errors++
	w.mu.Unlock()
	if job.OnComplete != nil {
		job.OnComplete(err)
	}
}

func (w *Worker) buildFFmpegArgs(job *TranscodeJob, profile config.ABRProfile) []string {
	output := filepath.Join(job.OutputDir, fmt.Sprintf("%s_%s.m3u8", job.StreamID, profile.Name))
	videoFilter := fmt.Sprintf("scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2,fps=%d",
		profile.Width, profile.Height, profile.Width, profile.Height, profile.FrameRate)

	args := []string{
		"-hide_banner", "-loglevel", "warning", "-y",
		"-i", job.InputPath,
		"-map", "0:v:0", "-map", "0:a?",
		"-vf", videoFilter,
		"-c:v", w.encoder,
		"-b:v", fmt.Sprintf("%d", profile.Bitrate),
		"-maxrate", fmt.Sprintf("%d", profile.Bitrate),
		"-bufsize", fmt.Sprintf("%d", profile.Bitrate*2),
		"-g", fmt.Sprintf("%d", profile.FrameRate*2),
		"-sc_threshold", "0",
		"-c:a", "aac",
		"-b:a", fmt.Sprintf("%d", profile.AudioBitrate),
		"-f", "hls",
		"-hls_time", "2",
		"-hls_list_size", "6",
		"-hls_flags", "delete_segments+independent_segments",
		output,
	}
	if w.encoder == "libx264" {
		insertAt := len(args) - 12
		prefix := append([]string(nil), args[:insertAt]...)
		prefix = append(prefix, "-preset", "fast")
		args = append(prefix, args[insertAt:]...)
	}
	return args
}

func (w *Worker) GetStats() WorkerStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

func (w *Worker) IsRunning() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.running
}

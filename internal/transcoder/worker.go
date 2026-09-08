package transcoder

import (
	"context"
	"fmt"
	"os/exec"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"go.uber.org/zap"
)

// Worker represents a transcoding worker
type Worker struct {
	id         int
	config     *config.TranscoderConfig
	logger     *zap.Logger
	inputPath  string
	outputDir  string
	profiles   []config.ABRProfile
	cmd        *exec.Cmd
	mu         sync.Mutex
	running    bool
	stats      WorkerStats
}

// WorkerStats holds transcoding statistics
type WorkerStats struct {
	StartTime     time.Time
	BytesProcessed int64
	FramesEncoded int64
	Errors        int64
}

// Manager manages transcoding workers
type Manager struct {
	config  *config.TranscoderConfig
	logger  *zap.Logger
	workers []*Worker
	mu      sync.Mutex
	jobs    chan *TranscodeJob
	ctx     context.Context
	cancel  context.CancelFunc
}

// TranscodeJob represents a transcoding job
type TranscodeJob struct {
	ID         string
	InputPath  string
	OutputDir  string
	StreamID   string
	Profiles   []config.ABRProfile
	OnComplete func(error)
}

// NewManager creates a new transcoder manager
func NewManager(cfg *config.TranscoderConfig, logger *zap.Logger) (*Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		config:  cfg,
		logger:  logger.Named("transcoder"),
		workers: make([]*Worker, 0, cfg.WorkerCount),
		jobs:    make(chan *TranscodeJob, cfg.WorkerCount*2),
		ctx:     ctx,
		cancel:  cancel,
	}

	// Create workers
	for i := 0; i < cfg.WorkerCount; i++ {
		worker := &Worker{
			id:       i,
			config:   cfg,
			logger:   logger.Named(fmt.Sprintf("worker-%d", i)),
			profiles: cfg.ABRLadder,
		}
		m.workers = append(m.workers, worker)
	}

	return m, nil
}

// Start starts the transcoder manager
func (m *Manager) Start() error {
	m.logger.Info("starting transcoder manager",
		zap.Int("worker_count", len(m.workers)),
	)

	for _, worker := range m.workers {
		go worker.processJobs(m.ctx, m.jobs)
	}

	return nil
}

// Stop stops the transcoder manager
func (m *Manager) Stop() error {
	m.logger.Info("stopping transcoder manager")
	m.cancel()
	return nil
}

// Submit submits a transcoding job
func (m *Manager) Submit(job *TranscodeJob) error {
	select {
	case m.jobs <- job:
		m.logger.Info("transcoding job submitted",
			zap.String("job_id", job.ID),
			zap.String("stream_id", job.StreamID),
		)
		return nil
	default:
		return fmt.Errorf("job queue is full")
	}
}

// GetStats returns transcoder statistics
func (m *Manager) GetStats() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	totalStats := WorkerStats{}
	for _, w := range m.workers {
		w.mu.Lock()
		totalStats.BytesProcessed += w.stats.BytesProcessed
		totalStats.FramesEncoded += w.stats.FramesEncoded
		totalStats.Errors += w.stats.Errors
		w.mu.Unlock()
	}

	return map[string]interface{}{
		"worker_count":    len(m.workers),
		"queue_size":      len(m.jobs),
		"bytes_processed": totalStats.BytesProcessed,
		"frames_encoded":  totalStats.FramesEncoded,
		"errors":          totalStats.Errors,
	}
}

func (w *Worker) processJobs(ctx context.Context, jobs <-chan *TranscodeJob) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-jobs:
			w.executeJob(job)
		}
	}
}

func (w *Worker) executeJob(job *TranscodeJob) {
	w.mu.Lock()
	w.running = true
	w.stats.StartTime = time.Now()
	w.mu.Unlock()

	w.logger.Info("starting transcoding job",
		zap.String("job_id", job.ID),
		zap.String("input", job.InputPath),
	)

	// Build FFmpeg command for ABR ladder
	args := w.buildFFmpegArgs(job)

	cmd := exec.CommandContext(context.Background(), w.config.FFmpegPath, args...)
	w.cmd = cmd

	output, err := cmd.CombinedOutput()
	if err != nil {
		w.logger.Error("transcoding failed",
			zap.String("job_id", job.ID),
			zap.Error(err),
			zap.String("output", string(output)),
		)

		w.mu.Lock()
		w.stats.Errors++
		w.running = false
		w.mu.Unlock()

		if job.OnComplete != nil {
			job.OnComplete(err)
		}
		return
	}

	w.logger.Info("transcoding completed",
		zap.String("job_id", job.ID),
	)

	w.mu.Lock()
	w.running = false
	w.mu.Unlock()

	if job.OnComplete != nil {
		job.OnComplete(nil)
	}
}

func (w *Worker) buildFFmpegArgs(job *TranscodeJob) []string {
	args := []string{
		"-i", job.InputPath,
		"-c:v", "libx264",
		"-c:a", "aac",
		"-preset", "fast",
		"-g", "60",
		"-sc_threshold", "0",
	}

	// Add filter complex for ABR ladder
	filterParts := []string{}
	outputMaps := []string{}

	for i, profile := range job.Profiles {
		filterParts = append(filterParts, fmt.Sprintf(
			"[0:v]scale=%d:%d,fps=%d[out%d]",
			profile.Width, profile.Height, profile.FrameRate, i,
		))
		outputMaps = append(outputMaps, fmt.Sprintf("-map [out%d]", i))
	}

	if len(filterParts) > 0 {
		args = append(args, "-filter_complex", joinStrings(filterParts, ";"))
		args = append(args, outputMaps...)
	}

	// Add output specifications for each profile
	for i, profile := range job.Profiles {
		args = append(args,
			"-b:v", fmt.Sprintf("%d", profile.Bitrate),
			"-b:a", fmt.Sprintf("%d", profile.AudioBitrate),
			"-f", "hls",
			fmt.Sprintf("%s/%s_%d.m3u8", job.OutputDir, job.StreamID, i),
		)
	}

	return args
}

func joinStrings(strs []string, sep string) string {
	result := ""
	for i, s := range strs {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}

// GetWorkerStats returns worker statistics
func (w *Worker) GetStats() WorkerStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

// IsRunning returns whether the worker is currently processing
func (w *Worker) IsRunning() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.running
}

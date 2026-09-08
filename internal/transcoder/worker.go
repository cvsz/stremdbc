package transcoder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/policedbc/stremdbc/internal/config"
	"github.com/policedbc/stremdbc/internal/core"
	"go.uber.org/zap"
)

const maxFFmpegOutput = 64 * 1024

var (
	ErrManagerNotStarted = errors.New("transcoder manager is not started")
	ErrManagerStopped    = errors.New("transcoder manager is stopped")
	ErrInvalidJob        = errors.New("invalid transcoding job")
	ErrDuplicateJob      = errors.New("transcoding job already exists")
	ErrStreamBusy        = errors.New("stream already has a transcoding job")
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
	config    *config.TranscoderConfig
	logger    *zap.Logger
	workers   []*Worker
	jobs      chan *TranscodeJob
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.RWMutex
	jobIDs    map[string]struct{}
	streamIDs map[string]struct{}
	started   bool
	stopped   bool
	encoder   string
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

// NewManager creates a transcoder manager and resolves the configured FFmpeg.
func NewManager(cfg *config.TranscoderConfig, logger *zap.Logger) (*Manager, error) {
	if cfg == nil {
		return nil, fmt.Errorf("transcoder configuration is required")
	}
	copyCfg := *cfg
	if copyCfg.WorkerCount <= 0 || copyCfg.WorkerCount > config.MaxTranscoderWorkers {
		return nil, fmt.Errorf("worker count must be between 1 and %d", config.MaxTranscoderWorkers)
	}
	if strings.TrimSpace(copyCfg.FFmpegPath) == "" {
		copyCfg.FFmpegPath = "ffmpeg"
	}
	if copyCfg.OutputFormat == "" {
		copyCfg.OutputFormat = "hls"
	}
	if copyCfg.OutputFormat != "hls" {
		return nil, fmt.Errorf("unsupported transcoder output format %q", copyCfg.OutputFormat)
	}
	if len(copyCfg.ABRLadder) == 0 {
		return nil, fmt.Errorf("transcoder ABR ladder cannot be empty")
	}
	copyCfg.ABRLadder = append([]config.ABRProfile(nil), copyCfg.ABRLadder...)
	for _, profile := range copyCfg.ABRLadder {
		if err := validateProfile(profile); err != nil {
			return nil, err
		}
	}
	if err := validateProfiles(copyCfg.ABRLadder); err != nil {
		return nil, err
	}
	ffmpegPath, err := exec.LookPath(copyCfg.FFmpegPath)
	if err != nil {
		return nil, fmt.Errorf("ffmpeg not found at %q: %w", copyCfg.FFmpegPath, err)
	}
	copyCfg.FFmpegPath = ffmpegPath
	if logger == nil {
		logger = zap.NewNop()
	}
	hardware := DetectHardware(ffmpegPath)
	encoder := "libx264"
	if copyCfg.GPUEnabled && len(hardware.HardwareEncoders) > 0 {
		encoder = hardware.HardwareEncoders[0]
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		config:    &copyCfg,
		logger:    logger.Named("transcoder"),
		workers:   make([]*Worker, 0, copyCfg.WorkerCount),
		jobs:      make(chan *TranscodeJob, copyCfg.WorkerCount*2),
		ctx:       ctx,
		cancel:    cancel,
		jobIDs:    make(map[string]struct{}),
		streamIDs: make(map[string]struct{}),
		encoder:   encoder,
	}
	for i := 0; i < copyCfg.WorkerCount; i++ {
		m.workers = append(m.workers, &Worker{
			id:      i,
			config:  &copyCfg,
			logger:  logger.Named(fmt.Sprintf("worker-%d", i)),
			encoder: encoder,
		})
	}
	m.logger.Info("transcoder initialized", zap.String("ffmpeg", ffmpegPath), zap.String("encoder", encoder), zap.Strings("detected_hardware_encoders", hardware.HardwareEncoders))
	return m, nil
}

// DetectHardware returns hardware encoders advertised by FFmpeg.
func DetectHardware(ffmpegPath string) HardwareInfo {
	info := HardwareInfo{FFmpegPath: ffmpegPath, SelectedEncoder: "libx264"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, ffmpegPath, "-hide_banner", "-nostdin", "-encoders")
	stdout := &boundedBuffer{limit: maxFFmpegOutput}
	stderr := &boundedBuffer{limit: maxFFmpegOutput}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	if err != nil {
		return info
	}
	text := stdout.String() + "\n" + stderr.String()
	for _, encoder := range []string{"h264_nvenc", "h264_qsv", "h264_amf", "h264_videotoolbox"} {
		if strings.Contains(text, encoder) {
			info.HardwareEncoders = append(info.HardwareEncoders, encoder)
		}
	}
	if len(info.HardwareEncoders) > 0 {
		info.SelectedEncoder = info.HardwareEncoders[0]
	}
	return info
}

// Start starts the configured worker pool. It is idempotent.
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return ErrManagerStopped
	}
	if m.started {
		return nil
	}
	m.started = true
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

// Stop stops all workers and waits for in-flight commands to exit.
func (m *Manager) Stop() error {
	m.mu.Lock()
	if !m.stopped {
		m.stopped = true
		m.cancel()
	}
	m.mu.Unlock()
	m.logger.Info("stopping transcoder manager")
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		m.cancelQueuedJobs()
		return nil
	case <-time.After(15 * time.Second):
		return fmt.Errorf("timeout waiting for transcoder workers to stop")
	}
}

func (m *Manager) cancelQueuedJobs() {
	queued := make([]*TranscodeJob, 0)
	for {
		select {
		case job := <-m.jobs:
			if job != nil {
				queued = append(queued, job)
			}
		default:
			for _, job := range queued {
				m.callCanceledJob(job)
			}
			return
		}
	}
}

func (m *Manager) callCanceledJob(job *TranscodeJob) {
	if job == nil || job.OnComplete == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			m.logger.Error("transcoding cancellation callback panicked", zap.Any("panic", recovered), zap.String("job_id", job.ID))
		}
	}()
	job.OnComplete(context.Canceled)
}

// Submit queues a validated job for a started manager.
func (m *Manager) Submit(job *TranscodeJob) error {
	if err := validateJob(job); err != nil {
		return err
	}
	copyJob := *job
	copyJob.Profiles = append([]config.ABRProfile(nil), job.Profiles...)
	if len(copyJob.Profiles) == 0 {
		copyJob.Profiles = append([]config.ABRProfile(nil), m.config.ABRLadder...)
	}
	for _, profile := range copyJob.Profiles {
		if err := validateProfile(profile); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidJob, err)
		}
	}
	if err := validateProfiles(copyJob.Profiles); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidJob, err)
	}
	if err := validateOutputDirPath(copyJob.OutputDir); err != nil {
		return fmt.Errorf("%w: output directory: %v", ErrInvalidJob, err)
	}
	originalCallback := copyJob.OnComplete
	copyJob.OnComplete = func(err error) {
		m.completeJob(copyJob.ID, copyJob.StreamID)
		if originalCallback != nil {
			originalCallback(err)
		}
	}

	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return ErrManagerNotStarted
	}
	if m.stopped {
		m.mu.Unlock()
		return ErrManagerStopped
	}
	if _, exists := m.jobIDs[copyJob.ID]; exists {
		m.mu.Unlock()
		return ErrDuplicateJob
	}
	if _, exists := m.streamIDs[copyJob.StreamID]; exists {
		m.mu.Unlock()
		return ErrStreamBusy
	}
	m.jobIDs[copyJob.ID] = struct{}{}
	m.streamIDs[copyJob.StreamID] = struct{}{}
	select {
	case m.jobs <- &copyJob:
		m.mu.Unlock()
		m.logger.Info("transcoding job submitted", zap.String("job_id", copyJob.ID), zap.String("stream_id", copyJob.StreamID))
		return nil
	default:
		delete(m.jobIDs, copyJob.ID)
		delete(m.streamIDs, copyJob.StreamID)
		m.mu.Unlock()
		return fmt.Errorf("job queue is full")
	}
}

func (m *Manager) completeJob(jobID, streamID string) {
	m.mu.Lock()
	delete(m.jobIDs, jobID)
	delete(m.streamIDs, streamID)
	m.mu.Unlock()
}

func (m *Manager) GetStats() map[string]interface{} {
	m.mu.RLock()
	jobQueueSize := len(m.jobs)
	workerCount := len(m.workers)
	encoder := m.encoder
	m.mu.RUnlock()
	total := WorkerStats{}
	activeWorkers := 0
	for _, worker := range m.workers {
		worker.mu.Lock()
		total.BytesProcessed += worker.stats.BytesProcessed
		total.FramesEncoded += worker.stats.FramesEncoded
		total.Errors += worker.stats.Errors
		total.JobsCompleted += worker.stats.JobsCompleted
		if worker.running {
			activeWorkers++
		}
		worker.mu.Unlock()
	}
	return map[string]interface{}{
		"worker_count":    workerCount,
		"active_workers":  activeWorkers,
		"queue_size":      jobQueueSize,
		"encoder":         encoder,
		"bytes_processed": total.BytesProcessed,
		"frames_encoded":  total.FramesEncoded,
		"jobs_completed":  total.JobsCompleted,
		"errors":          total.Errors,
	}
}

func (m *Manager) GetEncoder() string { return m.encoder }

func (w *Worker) processJobs(ctx context.Context, jobs <-chan *TranscodeJob) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-jobs:
			if job == nil {
				continue
			}
			if err := ctx.Err(); err != nil {
				w.callCompletion(job, err)
				return
			}
			w.executeJob(ctx, job)
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
		w.cmd = nil
		w.mu.Unlock()
	}()
	outputDir, err := prepareOutputDir(job.OutputDir)
	if err != nil {
		w.finishJob(job, fmt.Errorf("create output directory: %w", err))
		return
	}
	job.OutputDir = outputDir

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
		// #nosec G204 -- direct exec does not invoke a shell; args are built only
		// from validated job/profile values and a manager-resolved FFmpeg path.
		cmd := exec.CommandContext(ctx, w.config.FFmpegPath, args...)
		progress := &boundedBuffer{limit: maxFFmpegOutput}
		ffmpegLog := &boundedBuffer{limit: maxFFmpegOutput}
		cmd.Stdout = progress
		cmd.Stderr = ffmpegLog
		w.mu.Lock()
		w.cmd = cmd
		w.mu.Unlock()
		err := cmd.Run()
		w.recordProgress(job, progress.String())
		if err == nil {
			if outputErr := validateTranscodedOutput(transcodedOutputPath(job, profile)); outputErr != nil {
				err = outputErr
			}
		}
		if err != nil {
			w.logger.Error("transcoding profile failed", zap.String("job_id", job.ID), zap.String("profile", profile.Name), zap.Error(err), zap.String("ffmpeg_output", ffmpegLog.String()))
			w.finishJob(job, err)
			return
		}
	}
	w.mu.Lock()
	w.stats.JobsCompleted++
	w.mu.Unlock()
	w.callCompletion(job, nil)
}

func (w *Worker) finishJob(job *TranscodeJob, err error) {
	if err != nil && !errors.Is(err, context.Canceled) {
		w.mu.Lock()
		w.stats.Errors++
		w.mu.Unlock()
	}
	w.callCompletion(job, err)
}

func (w *Worker) buildFFmpegArgs(job *TranscodeJob, profile config.ABRProfile) []string {
	output := transcodedOutputPath(job, profile)
	videoFilter := fmt.Sprintf("scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2,fps=%d", profile.Width, profile.Height, profile.Width, profile.Height, profile.FrameRate)
	bitrate := strconv.Itoa(profile.Bitrate)
	bufsize := strconv.FormatInt(int64(profile.Bitrate)*2, 10)
	args := []string{
		"-hide_banner", "-loglevel", "warning", "-nostdin", "-nostats", "-progress", "pipe:1", "-y",
		"-i", job.InputPath,
		"-map", "0:v:0", "-map", "0:a?",
		"-vf", videoFilter,
		"-c:v", w.encoder,
	}
	if w.encoder == "libx264" {
		args = append(args, "-preset", "fast")
	}
	args = append(args,
		"-b:v", bitrate,
		"-maxrate", bitrate,
		"-bufsize", bufsize,
		"-g", strconv.Itoa(profile.FrameRate*2),
		"-sc_threshold", "0",
		"-c:a", "aac",
		"-b:a", strconv.Itoa(profile.AudioBitrate),
		"-f", "hls",
		"-hls_time", "2",
		"-hls_list_size", "6",
		"-hls_flags", "delete_segments+independent_segments",
		output,
	)
	return args
}

func transcodedOutputPath(job *TranscodeJob, profile config.ABRProfile) string {
	return filepath.Join(job.OutputDir, fmt.Sprintf("%s_%s.m3u8", job.StreamID, profile.Name))
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

func (w *Worker) recordProgress(job *TranscodeJob, progress string) {
	frames, bytesProcessed := parseProgress(progress)
	if bytesProcessed == 0 {
		if info, err := os.Stat(job.InputPath); err == nil && info.Mode().IsRegular() {
			bytesProcessed = info.Size()
		}
	}
	w.mu.Lock()
	w.stats.BytesProcessed += bytesProcessed
	w.stats.FramesEncoded += frames
	w.mu.Unlock()
}

func (w *Worker) callCompletion(job *TranscodeJob, err error) {
	if job == nil || job.OnComplete == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			w.logger.Error("transcoding completion callback panicked", zap.Any("panic", recovered), zap.String("job_id", job.ID))
		}
	}()
	job.OnComplete(err)
}

func validateJob(job *TranscodeJob) error {
	if job == nil || !validIdentifier(job.ID) {
		return ErrInvalidJob
	}
	if job.InputPath == "" || strings.TrimSpace(job.InputPath) != job.InputPath || strings.IndexFunc(job.InputPath, func(r rune) bool { return r == '\x00' || r == '\r' || r == '\n' || r == '\t' }) >= 0 {
		return ErrInvalidJob
	}
	if job.OutputDir == "" || strings.TrimSpace(job.OutputDir) != job.OutputDir || strings.IndexFunc(job.OutputDir, func(r rune) bool { return r == '\x00' || r == '\r' || r == '\n' || r == '\t' }) >= 0 {
		return ErrInvalidJob
	}
	if err := core.ValidateStreamID(job.StreamID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidJob, err)
	}
	return nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for index, character := range value {
		letter := (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z')
		digit := character >= '0' && character <= '9'
		if index == 0 && !letter && !digit {
			return false
		}
		if !letter && !digit && character != '-' && character != '_' && character != '.' {
			return false
		}
	}
	return true
}

func validateProfile(profile config.ABRProfile) error {
	if !validIdentifier(profile.Name) {
		return fmt.Errorf("invalid ABR profile %q", profile.Name)
	}
	if profile.Width <= 0 || profile.Width > config.MaxTranscoderDimension || profile.Height <= 0 || profile.Height > config.MaxTranscoderDimension || profile.Bitrate <= 0 || profile.Bitrate > config.MaxTranscoderBitrate || profile.FrameRate <= 0 || profile.FrameRate > config.MaxTranscoderFrameRate || profile.AudioBitrate <= 0 || profile.AudioBitrate > config.MaxTranscoderAudioBitrate {
		return fmt.Errorf("invalid ABR profile %q", profile.Name)
	}
	return nil
}

func validateProfiles(profiles []config.ABRProfile) error {
	seen := make(map[string]struct{}, len(profiles))
	for _, profile := range profiles {
		if _, exists := seen[profile.Name]; exists {
			return fmt.Errorf("duplicate ABR profile %q", profile.Name)
		}
		seen[profile.Name] = struct{}{}
	}
	return nil
}

func prepareOutputDir(path string) (string, error) {
	if err := validateOutputDirPath(path); err != nil {
		return "", err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if filepath.Clean(abs) == string(filepath.Separator) {
		return "", fmt.Errorf("output directory is unsafe")
	}
	if err := os.MkdirAll(abs, 0o750); err != nil {
		return "", err
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("output directory must be a regular directory")
	}
	return filepath.EvalSymlinks(abs)
}

func validateOutputDirPath(path string) error {
	pathSlash := filepath.ToSlash(path)
	if strings.Contains(pathSlash, "/../") || strings.HasPrefix(pathSlash, "../") || strings.HasSuffix(pathSlash, "/..") || pathSlash == ".." {
		return fmt.Errorf("output directory contains a traversal segment")
	}
	return nil
}

func validateTranscodedOutput(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("transcoding output was not created: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("transcoding output is not a regular file")
	}
	if info.Size() == 0 {
		return fmt.Errorf("transcoding output is empty")
	}
	return nil
}

func parseProgress(progress string) (frames, bytesProcessed int64) {
	for _, line := range strings.Split(progress, "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || parsed < 0 {
			continue
		}
		switch key {
		case "frame":
			frames = parsed
		case "total_size":
			bytesProcessed = parsed
		}
	}
	return frames, bytesProcessed
}

type boundedBuffer struct {
	mu    sync.Mutex
	data  []byte
	limit int
}

func (b *boundedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	originalLength := len(data)
	remaining := b.limit - len(b.data)
	if remaining > 0 {
		if len(data) > remaining {
			data = data[:remaining]
		}
		b.data = append(b.data, data...)
	}
	return originalLength, nil
}

func (b *boundedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(append([]byte(nil), b.data...))
}

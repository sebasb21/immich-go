package backup

import (
	"context"
	"fmt"
	"io"
	"time"
)

// RateLimitedReader wraps an io.Reader and limits the read rate
type RateLimitedReader struct {
	reader       io.Reader
	ctx          context.Context
	limiter      *rateLimiter
	bytesRead    int64
	startTime    time.Time
	lastLogTime  time.Time
	lastLogBytes int64
}

// rateLimiter implements a token bucket algorithm for rate limiting
type rateLimiter struct {
	rate       int64         // bytes per second
	bucketSize int64         // maximum burst size
	tokens     int64         // current tokens available
	lastUpdate time.Time     // last time tokens were added
}

// newRateLimiter creates a new rate limiter
func newRateLimiter(bytesPerSecond int64) *rateLimiter {
	// Allow bursts up to 2 seconds worth of data
	bucketSize := bytesPerSecond * 2
	return &rateLimiter{
		rate:       bytesPerSecond,
		bucketSize: bucketSize,
		tokens:     bucketSize,
		lastUpdate: time.Now(),
	}
}

// wait waits until the specified number of bytes can be sent
func (rl *rateLimiter) wait(ctx context.Context, bytes int64) error {
	for {
		// Add tokens based on elapsed time
		now := time.Now()
		elapsed := now.Sub(rl.lastUpdate)
		rl.tokens += int64(float64(rl.rate) * elapsed.Seconds())
		if rl.tokens > rl.bucketSize {
			rl.tokens = rl.bucketSize
		}
		rl.lastUpdate = now

		// If we have enough tokens, consume them and return
		if rl.tokens >= bytes {
			rl.tokens -= bytes
			return nil
		}

		// Calculate how long to wait for enough tokens
		tokensNeeded := bytes - rl.tokens
		waitTime := time.Duration(float64(tokensNeeded) / float64(rl.rate) * float64(time.Second))

		// Don't wait more than 100ms at a time to allow for context cancellation
		if waitTime > 100*time.Millisecond {
			waitTime = 100 * time.Millisecond
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
			// Continue loop to recalculate tokens
		}
	}
}

// NewRateLimitedReader creates a reader that limits the read rate
func NewRateLimitedReader(ctx context.Context, reader io.Reader, bytesPerSecond int64) *RateLimitedReader {
	now := time.Now()
	return &RateLimitedReader{
		reader:       reader,
		ctx:          ctx,
		limiter:      newRateLimiter(bytesPerSecond),
		startTime:    now,
		lastLogTime:  now,
		lastLogBytes: 0,
	}
}

// Read implements io.Reader with rate limiting
func (r *RateLimitedReader) Read(p []byte) (int, error) {
	// Wait for rate limiter to allow this read
	if err := r.limiter.wait(r.ctx, int64(len(p))); err != nil {
		return 0, err
	}

	// Perform the actual read
	n, err := r.reader.Read(p)
	r.bytesRead += int64(n)
	return n, err
}

// BytesRead returns the total number of bytes read
func (r *RateLimitedReader) BytesRead() int64 {
	return r.bytesRead
}

// CurrentSpeed returns the current upload speed in bytes per second
// Calculated from the last log checkpoint
func (r *RateLimitedReader) CurrentSpeed() float64 {
	elapsed := time.Since(r.lastLogTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	bytesSinceLastLog := r.bytesRead - r.lastLogBytes
	return float64(bytesSinceLastLog) / elapsed
}

// AverageSpeed returns the average upload speed in bytes per second since start
func (r *RateLimitedReader) AverageSpeed() float64 {
	elapsed := time.Since(r.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(r.bytesRead) / elapsed
}

// ResetLogCheckpoint resets the checkpoint for speed calculation
func (r *RateLimitedReader) ResetLogCheckpoint() {
	r.lastLogTime = time.Now()
	r.lastLogBytes = r.bytesRead
}

// ProgressReader wraps an io.Reader to track upload progress (without rate limiting)
type ProgressReader struct {
	reader       io.Reader
	bytesRead    int64
	totalSize    int64
	startTime    time.Time
	lastLogTime  time.Time
	lastLogBytes int64
	filename     string
}

// NewProgressReader creates a reader that tracks progress
func NewProgressReader(reader io.Reader, totalSize int64, filename string) *ProgressReader {
	now := time.Now()
	return &ProgressReader{
		reader:       reader,
		totalSize:    totalSize,
		filename:     filename,
		startTime:    now,
		lastLogTime:  now,
		lastLogBytes: 0,
	}
}

// Read implements io.Reader with progress tracking
func (p *ProgressReader) Read(b []byte) (int, error) {
	n, err := p.reader.Read(b)
	p.bytesRead += int64(n)
	return n, err
}

// BytesRead returns the total number of bytes read
func (p *ProgressReader) BytesRead() int64 {
	return p.bytesRead
}

// CurrentSpeed returns the current upload speed in bytes per second
func (p *ProgressReader) CurrentSpeed() float64 {
	elapsed := time.Since(p.lastLogTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	bytesSinceLastLog := p.bytesRead - p.lastLogBytes
	return float64(bytesSinceLastLog) / elapsed
}

// AverageSpeed returns the average upload speed in bytes per second since start
func (p *ProgressReader) AverageSpeed() float64 {
	elapsed := time.Since(p.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(p.bytesRead) / elapsed
}

// ResetLogCheckpoint resets the checkpoint for speed calculation
func (p *ProgressReader) ResetLogCheckpoint() {
	p.lastLogTime = time.Now()
	p.lastLogBytes = p.bytesRead
}

// PercentComplete returns the upload progress percentage
func (p *ProgressReader) PercentComplete() float64 {
	if p.totalSize == 0 {
		return 0
	}
	return float64(p.bytesRead) / float64(p.totalSize) * 100.0
}

// FormatProgress returns a formatted progress string
func (p *ProgressReader) FormatProgress() string {
	speed := p.CurrentSpeed()
	avgSpeed := p.AverageSpeed()
	percent := p.PercentComplete()

	return fmt.Sprintf("Uploading %s: %.1f%% (%s / %s) - Speed: %s/s (avg: %s/s)",
		p.filename,
		percent,
		formatBytesLocal(p.bytesRead),
		formatBytesLocal(p.totalSize),
		formatBytesLocal(int64(speed)),
		formatBytesLocal(int64(avgSpeed)),
	)
}

// formatBytesLocal formats bytes for display (local helper)
func formatBytesLocal(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

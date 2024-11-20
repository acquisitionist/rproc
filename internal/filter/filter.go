package filter

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	c "github.com/acquisitionist/rproc/internal/common"
	u "github.com/acquisitionist/rproc/internal/utils"
	"github.com/dustin/go-humanize"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog/log"
	"github.com/valyala/bytebufferpool"
	"golang.org/x/sync/semaphore"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MaxLineSize     = 10 << 20 // 10MB max line size
	DefaultMapSize  = 16       // Default size for JSON object maps
	FlushInterval   = time.Second
	LogInterval     = 5 * time.Second
	OutputBatchSize = 10000 // Number of lines to buffer before writing
)

// Processor handles the processing of Reddit data files
type Processor struct {
	ctx        *c.Context
	deps       *c.Dependencies
	state      *State
	processed  atomic.Int64
	matched    atomic.Int64
	errors     atomic.Int64
	bytesRead  atomic.Int64
	startTime  time.Time
	bufferPool *bytebufferpool.Pool
	jsonPool   sync.Pool
	linePool   sync.Pool
	matchPool  sync.Pool
	regexCache sync.Map // Cache for compiled regexes
}

// New creates a new filter instance with optimized pools
func New(ctx *c.Context, deps *c.Dependencies) (*Processor, error) {
	return &Processor{
		ctx:        ctx,
		deps:       deps,
		startTime:  time.Now(),
		bufferPool: &bytebufferpool.Pool{},
		jsonPool: sync.Pool{
			New: func() interface{} {
				return make(map[string]interface{}, DefaultMapSize)
			},
		},
		linePool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, MaxLineSize)
			},
		},
		matchPool: sync.Pool{
			New: func() interface{} {
				return make([]string, 0, OutputBatchSize)
			},
		},
	}, nil
}

// sanitizeFilename removes or replaces invalid filename characters
func sanitizeFilename(name string) string {
	re := regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)
	return re.ReplaceAllString(name, "_")
}

// Run executes the processing pipeline
func (p *Processor) Run(ctx context.Context) error {
	files, err := p.getInputFiles()
	if err != nil {
		return fmt.Errorf("getting input files: %w", err)
	}

	// Load state
	state, err := p.loadState()
	if err != nil {
		return fmt.Errorf("loading state: %w", err)
	}
	p.state = state

	// Get config hash
	configHash := p.hashConfig()

	// Filter out already processed files
	var filesToProcess []string
	for _, file := range files {
		if !p.state.isProcessed(filepath.Base(file), configHash) {
			filesToProcess = append(filesToProcess, file)
		} else {
			log.Info().
				Str("file", filepath.Base(file)).
				Msg("Skipping already processed file")
		}
	}

	if len(filesToProcess) == 0 {
		log.Info().Msg("No new files to process")
		return nil
	}

	// Set up concurrency controls
	var wg sync.WaitGroup
	maxConcurrency := p.ctx.Config.Threads
	if maxConcurrency <= 0 {
		maxConcurrency = runtime.NumCPU()
	}
	sem := semaphore.NewWeighted(int64(maxConcurrency))

	log.Info().
		Int("threads", maxConcurrency).
		Int("new_files", len(filesToProcess)).
		Int("skipped_files", len(files)-len(filesToProcess)).
		Msg("Starting processing")

	// Process files concurrently
	for i, file := range filesToProcess {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := sem.Acquire(ctx, 1); err != nil {
			return err
		}

		wg.Add(1)
		go func(file string, index, total int) {
			defer sem.Release(1)
			defer wg.Done()

			if err := p.processFile(ctx, file, configHash); err != nil {
				if errors.Is(err, context.Canceled) {
					log.Error().Err(err).Str("file", file).Msg("Processing canceled")
					return
				}
				log.Error().Err(err).Str("file", file).Msg("Failed to process file")
				p.errors.Add(1)
			}
		}(file, i, len(filesToProcess))
	}

	wg.Wait()

	// Save final state
	if err := p.state.saveState(p.deps.Fs); err != nil {
		log.Error().Err(err).Msg("Failed to save state file")
	}

	p.logFinalStats()
	return nil
}

// processFile handles processing of a single input file
func (p *Processor) processFile(ctx context.Context, path string, configHash string) error {
	fileInfo, err := p.deps.Fs.Stat(path)
	if err != nil {
		return fmt.Errorf("getting file info: %w", err)
	}

	inputFile, err := p.deps.Fs.Open(path)
	if err != nil {
		return fmt.Errorf("opening input file: %w", err)
	}
	defer inputFile.Close()

	reader, err := zstd.NewReader(
		inputFile,
		zstd.WithDecoderMaxWindow(1<<31),
		zstd.WithDecoderMaxMemory(1<<32),
		zstd.WithDecoderLowmem(false),
	)
	if err != nil {
		return fmt.Errorf("creating zstd reader: %w", err)
	}
	defer reader.Close()

	scanner := bufio.NewScanner(reader)
	buf := p.linePool.Get().([]byte)
	defer p.linePool.Put(buf)
	scanner.Buffer(buf[:MaxLineSize], MaxLineSize)

	var fileProcessed, bytesProcessed, fileMatched int64
	totalSize := fileInfo.Size()
	lastLog := time.Now()

	// Get slice from pool for matched lines
	matchedLines := p.matchPool.Get().([]string)
	matchedLines = matchedLines[:0]
	defer p.matchPool.Put(matchedLines)

	// Get buffer for string operations
	strBuf := p.bufferPool.Get()
	defer p.bufferPool.Put(strBuf)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		p.processed.Add(1)
		fileProcessed++

		line := scanner.Bytes()
		bytesProcessed += int64(len(line) + 1)

		if len(line) == 0 {
			continue
		}

		// Get object from pool
		obj := p.jsonPool.Get().(map[string]interface{})
		clear(obj)

		if err := json.Unmarshal(line, &obj); err != nil {
			p.errors.Add(1)
			p.jsonPool.Put(obj)
			continue
		}

		if p.matchesSubreddit(obj) {
			p.matched.Add(1)
			fileMatched++
			matchedLines = append(matchedLines, string(line))
		}

		p.jsonPool.Put(obj)

		if time.Since(lastLog) >= LogInterval {
			p.logProgress(filepath.Base(path), int(fileProcessed), int(totalSize))
			lastLog = time.Now()
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning file: %w", err)
	}

	// Only write output if we found matches
	if fileMatched > 0 {
		if err := p.writeOutput(path, matchedLines); err != nil {
			return fmt.Errorf("writing output: %w", err)
		}

		p.state.markProcessed(filepath.Base(path), configHash, fileMatched, fileProcessed)
		if err := p.state.saveState(p.deps.Fs); err != nil {
			log.Warn().Err(err).Msg("Failed to save state")
		}

		log.Info().
			Str("file", filepath.Base(path)).
			Int64("matches", fileMatched).
			Int64("total_lines", fileProcessed).
			Msg("File processing complete")
	}

	return nil
}

// matchesSubreddit checks if the data matches the configured subreddit
func (p *Processor) matchesSubreddit(data map[string]interface{}) bool {
	value, ok := data[p.ctx.Config.Field]
	if !ok {
		return false
	}

	strValue, ok := value.(string)
	if !ok {
		return false
	}

	switch {
	case p.ctx.Config.RegexMatch:
		// Get cached regex or compile new one
		var re *regexp.Regexp
		if cached, ok := p.regexCache.Load(p.ctx.Config.Value); ok {
			re = cached.(*regexp.Regexp)
		} else {
			compiled, err := regexp.Compile(p.ctx.Config.Value)
			if err != nil {
				log.Error().Err(err).Str("pattern", p.ctx.Config.Value).Msg("Invalid regex pattern")
				return false
			}
			p.regexCache.Store(p.ctx.Config.Value, compiled)
			re = compiled
		}
		return re.MatchString(strValue)

	case p.ctx.Config.PartialMatch:
		return strings.Contains(
			strings.ToLower(strValue),
			strings.ToLower(p.ctx.Config.Value),
		)

	default:
		return strings.EqualFold(strValue, p.ctx.Config.Value)
	}
}

// writeOutput handles writing matched lines to output file
func (p *Processor) writeOutput(inputPath string, matchedLines []string) error {
	if len(matchedLines) == 0 {
		return nil
	}

	inputFileName := filepath.Base(inputPath)
	fileExt := filepath.Ext(inputFileName)
	fileNameWithoutExt := strings.TrimSuffix(inputFileName, fileExt)
	outFileName := fmt.Sprintf("%s_%s%s",
		fileNameWithoutExt,
		sanitizeFilename(p.ctx.Config.Value),
		fileExt,
	)
	outPath := filepath.Join(p.ctx.Config.OutputDir, outFileName)

	if err := p.deps.Fs.MkdirAll(p.ctx.Config.OutputDir, 0755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}

	outFile, err := p.deps.Fs.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("creating output file: %w", err)
	}
	defer outFile.Close()

	writer, err := zstd.NewWriter(outFile)
	if err != nil {
		return fmt.Errorf("creating zstd writer: %w", err)
	}
	defer writer.Close()

	buf := p.bufferPool.Get()
	defer p.bufferPool.Put(buf)

	lastFlush := time.Now()

	for _, line := range matchedLines {
		if _, err := buf.WriteString(line); err != nil {
			return fmt.Errorf("writing line to buffer: %w", err)
		}
		if _, err := buf.WriteString("\n"); err != nil {
			return fmt.Errorf("writing newline to buffer: %w", err)
		}

		// Flush buffer when it gets large or enough time has passed
		if buf.Len() >= MaxLineSize || time.Since(lastFlush) >= FlushInterval {
			if _, err := writer.Write(buf.Bytes()); err != nil {
				return fmt.Errorf("writing batch: %w", err)
			}
			buf.Reset()
			lastFlush = time.Now()
		}
	}

	// Write any remaining data
	if buf.Len() > 0 {
		if _, err := writer.Write(buf.Bytes()); err != nil {
			return fmt.Errorf("writing final batch: %w", err)
		}
	}

	return nil
}

// getInputFiles returns a list of files to process based on configuration
func (p *Processor) getInputFiles() ([]string, error) {
	return u.GetInputFiles(p.deps, p.ctx.Config.InputDir, p.ctx.Config.FileFilter, p.ctx.Config.Debug)
}

// logProgress logs current processing statistics
func (p *Processor) logProgress(currentFile string, filesDone, totalFiles int) {
	now := time.Now()
	elapsed := now.Sub(p.startTime)
	speed := float64(p.processed.Load()) / elapsed.Seconds()

	log.Info().
		Str("file", currentFile).
		Str("progress", fmt.Sprintf("%d/%d", filesDone, totalFiles)).
		Int64("processed", p.processed.Load()).
		Int64("matched", p.matched.Load()).
		Int64("errors", p.errors.Load()).
		Str("speed", fmt.Sprintf("%s/s", humanize.Comma(int64(speed)))).
		Str("elapsed", humanize.RelTime(p.startTime, now, "ago", "")).
		Msg("Processing progress")
}

// logFinalStats logs final processing statistics
func (p *Processor) logFinalStats() {
	elapsed := time.Since(p.startTime)
	avgSpeed := float64(p.processed.Load()) / elapsed.Seconds()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	log.Info().
		Int64("total_processed", p.processed.Load()).
		Int64("total_matched", p.matched.Load()).
		Int64("total_errors", p.errors.Load()).
		Str("avg_speed", fmt.Sprintf("%s/s", humanize.Comma(int64(avgSpeed)))).
		Str("total_time", elapsed.Round(time.Second).String()).
		Str("heap_alloc", humanize.Bytes(m.HeapAlloc)).
		Str("total_alloc", humanize.Bytes(m.TotalAlloc)).
		Uint32("goroutines", uint32(runtime.NumGoroutine())).
		Float64("gc_pause_ms", float64(m.PauseTotalNs)/float64(time.Millisecond)).
		Uint32("gc_cycles", m.NumGC).
		Msg("Processing complete")
}

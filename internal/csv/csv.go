package csv

import (
	"bufio"
	"context"
	"encoding/csv"
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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MaxLineSize    = 10 << 20 // 10MB max line size
	DefaultMapSize = 16       // Default size for JSON object maps
	FlushInterval  = time.Second
	LogInterval    = 100000 // Log every 100k lines
)

var (
	ErrNoInput = errors.New("no input files found")
)

// Processor handles the conversion of Reddit data to CSV
type Processor struct {
	ctx        *c.Context
	deps       *c.Dependencies
	processed  atomic.Int64
	errors     atomic.Int64
	totalLines atomic.Int64
	badLines   atomic.Int64
	startTime  time.Time
	bufferPool *bytebufferpool.Pool
	jsonPool   sync.Pool
	outputPool sync.Pool
}

// ProcessStats holds processing statistics
type ProcessStats struct {
	LinesProcessed int64
	BadLines       int64
	BytesProcessed int64
	FileSize       int64
	Created        time.Time
}

// New creates a new CSV filter instance
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
		outputPool: sync.Pool{
			New: func() interface{} {
				return make([]string, 0, 10) // Most Reddit fields < 10
			},
		},
	}, nil
}

// Run executes the processing pipeline
func (p *Processor) Run(ctx context.Context) error {
	files, err := p.getInputFiles()
	if err != nil {
		return fmt.Errorf("getting input files: %w", err)
	}

	if len(files) == 0 {
		log.Info().Msg("No files to process")
		return nil
	}

	outputDir := p.ctx.Config.OutputDir
	if err := p.deps.Fs.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}

	outputFilePath := filepath.Join(outputDir, "output.csv")
	outputFile, err := p.deps.Fs.OpenFile(outputFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("creating csv file: %w", err)
	}
	defer outputFile.Close()

	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	fields := getFields(files[0])
	if err := writer.Write(fields); err != nil {
		return fmt.Errorf("writing header: %w", err)
	}

	log.Info().
		Int("threads", p.ctx.Config.Threads).
		Int("files", len(files)).
		Str("type", filepath.Base(files[0])[0:3]).
		Strs("fields", fields).
		Msg("Starting CSV conversion")

	// Concurrency setup
	maxConcurrency := p.ctx.Config.Threads
	if maxConcurrency <= 0 {
		maxConcurrency = runtime.NumCPU()
	}
	sem := semaphore.NewWeighted(int64(maxConcurrency))
	rowChan := make(chan []string, 10000)

	// Start writer goroutine
	writerDone := make(chan error, 1)
	go p.writeRows(writer, rowChan, writerDone)

	// Process files concurrently
	var wg sync.WaitGroup
	for i, file := range files {
		select {
		case <-ctx.Done():
			close(rowChan)
			return ctx.Err()
		default:
		}

		if err := sem.Acquire(ctx, 1); err != nil {
			close(rowChan)
			return err
		}

		wg.Add(1)
		go func(file string, index, total int) {
			defer sem.Release(1)
			defer wg.Done()

			if err := p.processFile(ctx, file, fields, rowChan); err != nil {
				if errors.Is(err, context.Canceled) {
					log.Error().Err(err).Str("file", file).Msg("Processing canceled")
					return
				}
				log.Error().Err(err).Str("file", file).Msg("Failed to process file")
				p.errors.Add(1)
			}
		}(file, i, len(files))
	}

	wg.Wait()
	close(rowChan)

	if err := <-writerDone; err != nil {
		return err
	}

	p.logFinalStats()
	return nil
}

// writeRows handles writing rows to the CSV file
func (p *Processor) writeRows(writer *csv.Writer, rowChan <-chan []string, done chan<- error) {
	defer close(done)
	var lastFlush time.Time

	for row := range rowChan {
		if err := writer.Write(row); err != nil {
			done <- fmt.Errorf("writing csv row: %w", err)
			return
		}

		// Return row slice to pool
		p.outputPool.Put(row)

		if time.Since(lastFlush) >= FlushInterval {
			writer.Flush()
			lastFlush = time.Now()
		}
	}
	writer.Flush()
}

// processFile handles processing of a single input file
func (p *Processor) processFile(ctx context.Context, path string, fields []string, rowChan chan<- []string) error {
	fileInfo, err := p.deps.Fs.Stat(path)
	if err != nil {
		return fmt.Errorf("getting file info: %w", err)
	}

	inputFile, err := p.deps.Fs.Open(path)
	if err != nil {
		return fmt.Errorf("opening input file: %w", err)
	}
	defer inputFile.Close()

	reader, err := zstd.NewReader(inputFile)
	if err != nil {
		return fmt.Errorf("creating zstd reader: %w", err)
	}
	defer reader.Close()

	stats := &ProcessStats{
		FileSize: fileInfo.Size(),
		Created:  time.Now(),
	}

	scanner := bufio.NewScanner(reader)
	buf := make([]byte, MaxLineSize)
	scanner.Buffer(buf, MaxLineSize)

	log.Info().
		Str("file", filepath.Base(path)).
		Int64("size", stats.FileSize).
		Msg("Starting file processing")

	strBuf := p.bufferPool.Get()
	defer p.bufferPool.Put(strBuf)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Bytes()
		stats.BytesProcessed += int64(len(line) + 1)

		if len(line) == 0 {
			continue
		}

		stats.LinesProcessed++

		// Get object from pool
		obj := p.jsonPool.Get().(map[string]interface{})
		clear(obj)

		if err := json.Unmarshal(line, &obj); err != nil {
			stats.BadLines++
			p.errors.Add(1)
			p.jsonPool.Put(obj)
			continue
		}

		// Get output slice from pool
		output := p.outputPool.Get().([]string)
		output = output[:0] // Reset slice
		if cap(output) < len(fields) {
			output = make([]string, 0, len(fields))
		}

		if err := p.processFields(obj, fields, &output, strBuf); err != nil {
			stats.BadLines++
			p.errors.Add(1)
			p.jsonPool.Put(obj)
			p.outputPool.Put(output)
			continue
		}

		rowChan <- output
		p.processed.Add(1)
		p.jsonPool.Put(obj)

		if stats.LinesProcessed%LogInterval == 0 {
			p.logProgress(path, stats)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning file: %w", err)
	}

	log.Info().
		Str("file", filepath.Base(path)).
		Int64("lines_processed", stats.LinesProcessed).
		Int64("bad_lines", stats.BadLines).
		Msg("Completed file")

	return nil
}

// processFields processes fields for a single row
func (p *Processor) processFields(obj map[string]interface{}, fields []string, output *[]string, buf *bytebufferpool.ByteBuffer) error {
	for _, field := range fields {
		buf.Reset()

		switch field {
		case "created":
			if createdUTC, exists := obj["created_utc"]; exists {
				unixTime := p.parseCreatedTime(createdUTC)
				buf.B = time.Unix(unixTime, 0).UTC().AppendFormat(buf.B, "2006-01-02 15:04")
			} else {
				if _, err := buf.WriteString("None"); err != nil {
					return fmt.Errorf("writing None for created: %w", err)
				}
			}

		case "link":
			if permalink, ok := obj["permalink"].(string); ok {
				if _, err := buf.WriteString("https://www.reddit.com"); err != nil {
					return fmt.Errorf("writing domain: %w", err)
				}
				if _, err := buf.WriteString(permalink); err != nil {
					return fmt.Errorf("writing permalink: %w", err)
				}
			} else {
				if err := p.buildRedditLink(obj, buf); err != nil {
					return fmt.Errorf("building reddit link: %w", err)
				}
			}

		case "author":
			if author, ok := obj["author"].(string); ok {
				if _, err := buf.WriteString("u/"); err != nil {
					return fmt.Errorf("writing author prefix: %w", err)
				}
				if _, err := buf.WriteString(author); err != nil {
					return fmt.Errorf("writing author: %w", err)
				}
			}

		case "text":
			if text, ok := obj["selftext"].(string); ok {
				if _, err := buf.WriteString(text); err != nil {
					return fmt.Errorf("writing text: %w", err)
				}
			}

		case "url":
			if url, ok := obj["url"].(string); ok && url != "" {
				if _, err := buf.WriteString(url); err != nil {
					return fmt.Errorf("writing url: %w", err)
				}
			}

		default:
			if v, ok := obj[field]; ok {
				// fmt.Fprintf internally uses WriteString, so we need to handle its error too
				if _, err := fmt.Fprintf(buf, "%v", v); err != nil {
					return fmt.Errorf("writing default field %s: %w", field, err)
				}
			}
		}

		*output = append(*output, string(buf.B))
	}

	return nil
}

// parseCreatedTime parses the created_utc field
func (p *Processor) parseCreatedTime(v interface{}) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int64:
		return t
	case int:
		return int64(t)
	case string:
		if i, err := strconv.ParseInt(t, 10, 64); err == nil {
			return i
		}
	}
	return 0
}

// getInputFiles returns a list of files to process based on configuration
func (p *Processor) getInputFiles() ([]string, error) {
	return u.GetInputFiles(p.deps, p.ctx.Config.InputDir, p.ctx.Config.FileFilter, p.ctx.Config.Debug)
}

// getFields determines the CSV fields based on file type
func getFields(filename string) []string {
	base := filepath.Base(filename)
	if strings.HasPrefix(base, "RS_") {
		return []string{"author", "title", "score", "created", "link", "text", "url"}
	} else if strings.HasPrefix(base, "RC_") {
		return []string{"author", "score", "created", "link", "body"}
	}
	log.Warn().
		Str("filename", base).
		Msg("Unable to determine file type from prefix, defaulting to submission fields")
	return []string{"author", "title", "score", "created", "link", "text", "url"}
}

// logProgress logs current processing statistics
func (p *Processor) logProgress(currentFile string, stats *ProcessStats) {
	log.Info().
		Str("file", filepath.Base(currentFile)).
		Int64("lines_processed", stats.LinesProcessed).
		Int64("bad_lines", stats.BadLines).
		Str("speed", fmt.Sprintf("%s/s", humanize.Comma(int64(float64(stats.LinesProcessed)/time.Since(stats.Created).Seconds())))).
		Msg("Processing progress")
}
func (p *Processor) logFinalStats() {
	elapsed := time.Since(p.startTime)
	avgSpeed := float64(p.processed.Load()) / elapsed.Seconds()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	log.Info().
		Int64("total_lines", p.totalLines.Load()).
		Int64("total_bad_lines", p.badLines.Load()).
		Int64("total_errors", p.errors.Load()).
		Str("avg_speed", fmt.Sprintf("%s/s", humanize.Comma(int64(avgSpeed)))).
		Str("total_time", elapsed.Round(time.Second).String()).
		Str("memory_alloc", humanize.Bytes(m.Alloc)).
		Str("total_alloc", humanize.Bytes(m.TotalAlloc)).
		Uint32("goroutines", uint32(runtime.NumGoroutine())).
		Msg("Processing complete")
}

// buildRedditLink builds a Reddit link from components
func (p *Processor) buildRedditLink(obj map[string]interface{}, buf *bytebufferpool.ByteBuffer) error {
	if _, err := buf.WriteString("https://www.reddit.com/r/"); err != nil {
		return fmt.Errorf("writing base url: %w", err)
	}

	if subreddit, ok := obj["subreddit"].(string); ok {
		if _, err := buf.WriteString(subreddit); err != nil {
			return fmt.Errorf("writing subreddit: %w", err)
		}
	}

	if _, err := buf.WriteString("/comments/"); err != nil {
		return fmt.Errorf("writing comments path: %w", err)
	}

	if linkID, ok := obj["link_id"].(string); ok {
		if _, err := buf.WriteString(strings.TrimPrefix(linkID, "t3_")); err != nil {
			return fmt.Errorf("writing link id: %w", err)
		}
	}

	if _, err := buf.WriteString("/_/"); err != nil {
		return fmt.Errorf("writing separator: %w", err)
	}

	if id, ok := obj["id"].(string); ok {
		if _, err := buf.WriteString(id); err != nil {
			return fmt.Errorf("writing comment id: %w", err)
		}
	}

	if _, err := buf.WriteString("/"); err != nil {
		return fmt.Errorf("writing final slash: %w", err)
	}

	return nil
}

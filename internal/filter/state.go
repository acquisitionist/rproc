package filter

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/spf13/afero"
	"path/filepath"
	"sync"
	"time"
)

type ProcessedFile struct {
	Filename     string    `json:"filename"`
	ProcessedAt  time.Time `json:"processed_at"`
	MatchCount   int64     `json:"match_count"`
	TotalLines   int64     `json:"total_lines"`
	HashedConfig string    `json:"hashed_config"` // To detect config changes
}

type State struct {
	ProcessedFiles map[string]ProcessedFile `json:"processed_files"`
	mu             sync.RWMutex
	stateFile      string
}

// getStateFilePath returns the path to the state file based on the config
func (p *Processor) getStateFilePath() string {
	// Create state file in output directory
	return filepath.Join(p.ctx.Config.OutputDir, "rproc_state.json")
}

// loadState loads the filter state from disk
func (p *Processor) loadState() (*State, error) {
	state := &State{
		ProcessedFiles: make(map[string]ProcessedFile),
		stateFile:      p.getStateFilePath(),
	}

	// Try to read existing state file
	exists, err := afero.Exists(p.deps.Fs, state.stateFile)
	if err != nil {
		return nil, fmt.Errorf("checking state file: %w", err)
	}

	if !exists {
		return state, nil // Return empty state if file doesn't exist
	}

	file, err := p.deps.Fs.Open(state.stateFile)
	if err != nil {
		return nil, fmt.Errorf("opening state file: %w", err)
	}
	defer file.Close()

	if err := json.NewDecoder(file).Decode(&state.ProcessedFiles); err != nil {
		return nil, fmt.Errorf("parsing state file: %w", err)
	}

	return state, nil
}

// saveState saves the current state to disk
func (p *State) saveState(fs afero.Fs) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := json.MarshalIndent(p.ProcessedFiles, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling state: %w", err)
	}

	if err := afero.WriteFile(fs, p.stateFile, data, 0644); err != nil {
		return fmt.Errorf("writing state file: %w", err)
	}

	return nil
}

// hashConfig creates a hash of the current configuration
func (p *Processor) hashConfig() string {
	config := fmt.Sprintf("%s:%s:%v:%v:%s",
		p.ctx.Config.Field,
		p.ctx.Config.Value,
		p.ctx.Config.RegexMatch,
		p.ctx.Config.PartialMatch,
		p.ctx.Config.FileFilter,
	)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(config)))
}

// isProcessed checks if a file has already been processed with current config
func (p *State) isProcessed(filename, configHash string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if processed, exists := p.ProcessedFiles[filename]; exists {
		return processed.HashedConfig == configHash
	}
	return false
}

// markProcessed marks a file as processed
func (p *State) markProcessed(filename string, configHash string, matchCount, totalLines int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ProcessedFiles[filename] = ProcessedFile{
		Filename:     filename,
		ProcessedAt:  time.Now(),
		MatchCount:   matchCount,
		TotalLines:   totalLines,
		HashedConfig: configHash,
	}
}

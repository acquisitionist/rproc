package cli

import (
	"errors"
	"fmt"
	c "github.com/acquisitionist/rproc/internal/common"
	"github.com/acquisitionist/rproc/internal/env"
	"github.com/adrg/xdg"
	"github.com/spf13/afero"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Config related constants
const (
	envPrefix      = "RPROC"
	configFileName = ".rproc"
	configFileType = "yaml"
	// MinRequiredDiskSpace = 10 * 1024 * 1024 * 1024 // 10GB
	// MinRequiredMemory    = 1 * 1024 * 1024 * 1024  // 1GB
)

// Common validation errors
var (
	ErrInvalidNumProcesses = errors.New("number of processes must be greater than 0")
	ErrInvalidErrorRate    = errors.New("error rate must be between 0 and 100")
	ErrInvalidValueList    = errors.New("value list file does not exist")
	ErrInvalidInput        = errors.New("input must be a directory or a .zst file with RC_ or RS_ prefix")
)

// Options holds CLI flags and configuration options.
type Options struct {
	ConfigPath string // Path to configuration file

	// Processing options
	Field     string
	Value     string
	ValueList string
	Threads   int

	// Filter options
	FileFilter        string
	SplitIntermediate bool
	ErrorRate         int
	PartialMatch      bool
	RegexMatch        bool

	Debug bool
}

// ConfigLoader handles loading and merging configuration from different sources
type ConfigLoader struct {
	fs  afero.Fs
	env *env.Getter
}

// NewConfigLoader creates a new configuration loader
func NewConfigLoader(fs afero.Fs) *ConfigLoader {
	return &ConfigLoader{
		fs:  fs,
		env: env.New(envPrefix),
	}
}

// LoadConfig loads the configuration from all sources and returns the merged result.
func LoadConfig(deps c.Dependencies, configPath string, opts Options) (c.Config, error) {
	loader := NewConfigLoader(deps.Fs)

	path, err := loader.findConfig(configPath)
	if err != nil {
		return c.Config{}, fmt.Errorf("finding config file: %w", err)
	}

	config, err := loader.readConfig(path)
	if err != nil {
		return c.Config{}, fmt.Errorf("loading config file: %w", err)
	}

	return loader.mergeConfig(config, opts), nil
}

// findConfigFile searches for the config file in standard locations.
func (cl *ConfigLoader) findConfig(explicitPath string) (string, error) {
	if explicitPath != "" {
		return explicitPath, nil
	}

	v := viper.New()
	v.SetFs(cl.fs)
	v.SetConfigType(configFileType)
	v.SetConfigName(configFileName)

	searchPaths := []string{
		".",                                    // Current directory
		filepath.Join(xdg.ConfigHome, "rproc"), // XDG config directory
		xdg.ConfigHome,                         // XDG config home
	}

	if home, err := os.UserHomeDir(); err == nil {
		searchPaths = append(searchPaths, home)
	}

	for _, path := range searchPaths {
		v.AddConfigPath(path)
	}

	if err := v.ReadInConfig(); err != nil {
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if errors.As(err, &configFileNotFoundError) {
			return "", nil // No config file found is acceptable
		}
		return "", fmt.Errorf("reading config file: %w", err)
	}

	return v.ConfigFileUsed(), nil
}

// loadConfigFile reads and parses the YAML config file.
func (cl *ConfigLoader) readConfig(path string) (c.Config, error) {
	if path == "" {
		return c.Config{}, nil
	}

	f, err := cl.fs.Open(path)
	if err != nil {
		return c.Config{}, fmt.Errorf("opening config file: %w", err)
	}
	defer f.Close()

	var config c.Config
	if err := yaml.NewDecoder(f).Decode(&config); err != nil {
		return c.Config{}, fmt.Errorf("parsing config file: %w", err)
	}

	return config, nil
}

// mergeConfig combines configuration from all sources in order of precedence.
func (cl *ConfigLoader) mergeConfig(config c.Config, opts Options) c.Config {
	// CLI options take precedence over config file
	if opts.Field != "" {
		config.Field = opts.Field
	}
	if opts.Value != "" {
		config.Value = opts.Value
	}
	if opts.ValueList != "" {
		config.ValueList = opts.ValueList
	}
	if opts.Threads != 0 {
		config.Threads = opts.Threads
	}
	if opts.FileFilter != "" {
		config.FileFilter = opts.FileFilter
	}
	if opts.ErrorRate != 0 {
		config.ErrorRate = opts.ErrorRate
	}

	// Boolean flags use OR semantics
	config.SplitIntermediate = config.SplitIntermediate || opts.SplitIntermediate
	config.PartialMatch = config.PartialMatch || opts.PartialMatch
	config.RegexMatch = config.RegexMatch || opts.RegexMatch
	config.Debug = config.Debug || opts.Debug

	// Environment variables override both config file and CLI options
	config.Field = cl.env.GetString("FIELD", config.Field)
	config.Value = cl.env.GetString("VALUE", config.Value)
	config.ValueList = cl.env.GetString("VALUE_LIST", config.ValueList)
	config.Threads = cl.env.GetInt("NUM_PROCESSES", config.Threads)
	config.FileFilter = cl.env.GetString("FILE_FILTER", config.FileFilter)
	config.SplitIntermediate = cl.env.GetBool("SPLIT_INTERMEDIATE", config.SplitIntermediate)
	config.ErrorRate = cl.env.GetInt("ERROR_RATE", config.ErrorRate)
	config.PartialMatch = cl.env.GetBool("PARTIAL_MATCH", config.PartialMatch)
	config.RegexMatch = cl.env.GetBool("REGEX_MATCH", config.RegexMatch)
	config.Debug = cl.env.GetBool("DEBUG", config.Debug)
	config.StateFile = cl.env.GetString("STATE_FILE", config.StateFile)

	return config
}
func ValidateCommon(config *c.Config, deps *c.Dependencies) error {
	var errs []error

	if config.Threads < 1 {
		errs = append(errs, ErrInvalidNumProcesses)
	}

	if config.ErrorRate < 0 || config.ErrorRate > 100 {
		errs = append(errs, ErrInvalidErrorRate)
	}

	// Check file filter regex validity
	if config.FileFilter != "" {
		if _, err := regexp.Compile(config.FileFilter); err != nil {
			errs = append(errs, fmt.Errorf("invalid file filter pattern: %w", err))
		}
	}

	// Input/Output path checks
	if err := validateInput(deps.Fs, config.InputDir); err != nil {
		errs = append(errs, fmt.Errorf("validating input path: %w", err))
	}

	for _, dir := range []string{config.OutputDir} {
		if err := validateOutputDirectory(deps.Fs, dir); err != nil {
			errs = append(errs, fmt.Errorf("validating directory %s: %w", dir, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("configuration validation failed: %w", errors.Join(errs...))
	}

	return nil
}
func ValidateCSVCommand(config *c.Config, deps *c.Dependencies) error {
	if err := ValidateCommon(config, deps); err != nil {
		return err
	}
	// future validations if any
	return nil
}

func ValidateFilterCommand(config *c.Config, deps *c.Dependencies) error {
	if err := ValidateCommon(config, deps); err != nil {
		return err
	}

	var errs []error

	if config.Field == "" {
		errs = append(errs, fmt.Errorf("field must be set for filter command"))
	}
	if config.Value == "" && config.ValueList == "" {
		errs = append(errs, fmt.Errorf("either value or value-list must be set for filter command"))
	}

	validFields := map[string]bool{
		"subreddit": true,
		"author":    true,
		"title":     true,
		"selftext":  true,
		"body":      true,
		"domain":    true,
	}
	if !validFields[config.Field] {
		errs = append(errs, fmt.Errorf("invalid field name: %s", config.Field))
	}

	// Check regex validity if regex matching is enabled
	if config.RegexMatch && config.Value != "" {
		if _, err := regexp.Compile(config.Value); err != nil {
			errs = append(errs, fmt.Errorf("invalid regex pattern: %w", err))
		}
	}
	// Check that regex and partial match aren't both enabled
	if config.RegexMatch && config.PartialMatch {
		errs = append(errs, fmt.Errorf("cannot use both regex and partial matching"))
	}

	// Existing value list check
	if config.ValueList != "" {
		exists, err := afero.Exists(deps.Fs, config.ValueList)
		if err != nil {
			errs = append(errs, fmt.Errorf("checking value list file: %w", err))
		} else if !exists {
			errs = append(errs, ErrInvalidValueList)
		} else {
			if file, err := deps.Fs.Open(config.ValueList); err != nil {
				errs = append(errs, fmt.Errorf("opening value list: %w", err))
			} else {
				defer file.Close()
				info, err := file.Stat()
				if err != nil {
					errs = append(errs, fmt.Errorf("stating value list: %w", err))
				} else if info.Size() == 0 {
					errs = append(errs, fmt.Errorf("value list is empty"))
				}
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("filter command validation failed: %w", errors.Join(errs...))
	}

	return nil
}

// validateInput checks if a path is a valid input (directory or Reddit dump file).
func validateInput(fs afero.Fs, path string) error {
	fi, err := fs.Stat(path)
	if err != nil {
		return fmt.Errorf("accessing input: %w", err)
	}

	if fi.IsDir() {
		return nil
	}

	if filepath.Ext(path) != ".zst" {
		return fmt.Errorf("%w: not a .zst file", ErrInvalidInput)
	}

	baseName := filepath.Base(path)
	if !strings.HasPrefix(baseName, "RC_") && !strings.HasPrefix(baseName, "RS_") {
		return fmt.Errorf("%w: file must start with RC_ or RS_", ErrInvalidInput)
	}

	return nil
}

// validateOutputDirectory ensures a directory exists or can be created.
func validateOutputDirectory(fs afero.Fs, path string) error {
	cleanPath := filepath.Clean(path)
	if cleanPath == "/" || filepath.VolumeName(cleanPath) == cleanPath {
		return fmt.Errorf("cannot use root path as directory: %s", path)
	}

	return fs.MkdirAll(cleanPath, 0755)
}

// GetDependencies creates and returns the application dependencies.
// This function can be expanded to include additional dependencies as needed.
func GetDependencies() (c.Dependencies, error) {
	deps := c.Dependencies{
		Fs: afero.NewOsFs(),
	}

	// Any additional initialization or validation of dependencies can be done here
	// For example, checking if required external services are available

	return deps, nil
}

// GetContext creates a new application context using the provided dependencies and configuration.
func GetContext(deps c.Dependencies, config c.Config) (c.Context, error) {
	// Initialize an empty reference - this can be expanded based on needs
	ref := c.Reference{}

	ctx := c.Context{
		Reference: ref,
		Config:    config,
	}

	return ctx, nil
}

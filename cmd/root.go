package cmd

import (
	"context"
	"errors"
	"fmt"
	"github.com/acquisitionist/rproc/internal/cli"
	c "github.com/acquisitionist/rproc/internal/common"
	"github.com/acquisitionist/rproc/internal/csv"
	"github.com/acquisitionist/rproc/internal/filter"
	"github.com/acquisitionist/rproc/internal/vcs"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"syscall"
)

var (
	rootCtx    context.Context
	rootCancel context.CancelFunc
	// Version information
	version = vcs.Get().String()

	// Core dependencies
	dep    c.Dependencies
	ctx    c.Context
	config c.Config

	// Configuration
	configPath string
	options    cli.Options
)

type spawn interface {
	Run(context.Context) error
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Version: version,
	Use:     "rproc",
	Short:   "Process Reddit data dumps",
	Long: `Process Reddit data dumps in zst compressed ndjson format.
Supports various operations including filtering and conversion to CSV.`,
	PersistentPreRunE: preRun,
	SilenceErrors:     true,
	SilenceUsage:      false,
}

// filterCmd represents the filter command
var filterCmd = &cobra.Command{
	Use:   "filter [input directory] [output directory]",
	Short: "Filter Reddit data dumps",
	Long: `Filter Reddit ZST.
Supports filtering by field values with regex and partial matching.`,
	Args: cobra.ExactArgs(2),
	RunE: runFilter,
}

// csvCmd represents the CSV conversion command
var csvCmd = &cobra.Command{
	Use:   "csv [input directory] [output file]",
	Short: "Convert Reddit ZST dumps to CSV",
	Long: `Convert Reddit data dumps from ZST compressed NDJSON format to CSV.
Supports both submissions and comments data.`,
	Args: cobra.ExactArgs(2),
	RunE: runCSV,
}

func init() {
	rootCtx, rootCancel = context.WithCancel(context.Background())

	// Add commands to root
	rootCmd.AddCommand(filterCmd)
	rootCmd.AddCommand(csvCmd)

	// Persistent flags (shared across all commands)
	pFlags := rootCmd.PersistentFlags()
	pFlags.StringVar(&configPath, "config", "", "config file (default is $HOME/.rproc.yaml)")
	pFlags.BoolVar(&options.Debug, "debug", false, "enable debug logging")
	pFlags.IntVar(&options.Threads, "threads", 1, "number of parallel processes")
	pFlags.StringVar(&options.FileFilter, "file-filter", ".*", "regex for matching input filenames")
	// pFlags.DurationVar(&timeout, "timeout", 0, "operation timeout (e.g. 1h, 2h30m)")

	// Filter flags
	filterFlags := filterCmd.Flags()
	filterFlags.StringVar(&options.Field, "field", "", "field to filter on")
	filterFlags.StringVar(&options.Value, "value", "", "value to match against the field")
	filterFlags.StringVar(&options.ValueList, "value-list", "", "file containing newline-separated values to match")
	filterFlags.BoolVar(&options.SplitIntermediate, "split-intermediate", false, "split intermediate files by first letter")
	filterFlags.IntVar(&options.ErrorRate, "error-rate", 0, "acceptable percentage of errors (0-100)")
	filterFlags.BoolVar(&options.PartialMatch, "partial", false, "use partial string matching")
	filterFlags.BoolVar(&options.RegexMatch, "regex", false, "use regex matching")

	// Mark both field and value as required
	if err := filterCmd.MarkFlagRequired("field"); err != nil {
		panic(fmt.Sprintf("failed to mark 'field' flag as required: %v", err))
	}
	if err := filterCmd.MarkFlagRequired("value"); err != nil {
		panic(fmt.Sprintf("failed to mark 'value' flag as required: %v", err))
	}
}

// preRun initializes all dependencies before command execution
func preRun(cmd *cobra.Command, args []string) error {
	if len(args) < 2 {
		return errors.New("input and output directories are required")
	}

	var err error

	// Initialize dependencies
	if dep, err = cli.GetDependencies(); err != nil {
		return fmt.Errorf("initializing dependencies: %w", err)
	}

	// Load configuration
	if config, err = cli.LoadConfig(dep, configPath, options); err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}

	// Set directories from arguments AFTER loading config
	config.InputDir = args[0]
	config.OutputDir = args[1]

	// Command-specific validation
	switch cmd {
	case filterCmd:
		if err = cli.ValidateFilterCommand(&config, &dep); err != nil {
			return fmt.Errorf("validating filter command: %w", err)
		}
	case csvCmd:
		if err = cli.ValidateCSVCommand(&config, &dep); err != nil {
			return fmt.Errorf("validating csv command: %w", err)
		}
	default:
		return fmt.Errorf("unknown command")
	}

	// Initialize context
	if ctx, err = cli.GetContext(dep, config); err != nil {
		return fmt.Errorf("initializing context: %w", err)
	}

	// Log startup information
	log.Info().
		Str("version", version).
		Bool("debug", config.Debug).
		Str("input", config.InputDir).
		Str("output", config.OutputDir).
		Int("threads", config.Threads).
		Str("field", config.Field).
		Str("value", config.Value).
		Msg("Starting rproc")
	return nil
}

func runFilter(_ *cobra.Command, _ []string) error {
	proc, err := filter.New(&ctx, &dep)
	if err != nil {
		return fmt.Errorf("failed to create filter: %w", err)
	}
	return runWithSignalHandling(proc)
}

func runCSV(_ *cobra.Command, _ []string) error {
	proc, err := csv.New(&ctx, &dep)
	if err != nil {
		return fmt.Errorf("failed to create filter: %w", err)
	}
	return runWithSignalHandling(proc)
}

// Execute starts the application
func Execute() error {
	// Make sure we clean up the context when we're done
	defer func() {
		rootCancel()
		log.Info().Msg("Shutdown complete")
	}()

	if err := rootCmd.Execute(); err != nil {
		if err.Error() != "processing cancelled" {
			return fmt.Errorf("executing command: %w", err)
		}
		log.Info().Msg("Processing was cancelled")
		return nil
	}
	return nil
}

func runWithSignalHandling(proc spawn) error {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	done := make(chan error, 1)

	go func() {
		<-sigChan
		log.Info().Msg("Received interrupt signal, initiating shutdown...")
		rootCancel()
	}()

	go func() {
		done <- proc.Run(rootCtx)
	}()

	err := <-done
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("filter error: %w", err)
	}
	return nil
}

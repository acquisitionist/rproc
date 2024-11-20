package main

import (
	"github.com/acquisitionist/rproc/cmd"
	"github.com/acquisitionist/rproc/internal/vcs"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"runtime/debug"
	"time"
)

func init() {
	// Configure
	zerolog.TimeFieldFormat = time.RFC3339

	// Set up console writer for nice formatting
	consoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}

	// Create logs directory if it doesn't exist
	if err := os.MkdirAll("logs", 0755); err != nil {
		log.Fatal().Err(err).Msg("Failed to create logs directory")
	}

	// Open log file
	logFile, err := os.OpenFile(
		"logs/rproc.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to open log file")
	}

	// Use multiple writers to write to both console and file
	multi := zerolog.MultiLevelWriter(consoleWriter, logFile)

	// Configure global logger
	log.Logger = zerolog.New(multi).
		With().
		Timestamp().
		Caller().
		Logger()
}

func main() {
	// Set up panic recovery
	defer func() {
		if r := recover(); r != nil {
			log.Fatal().
				Interface("panic", r).
				Str("stack", string(debug.Stack())).
				Msg("Recovered from panic")
			os.Exit(1)
		}
	}()

	log.Info().
		Str("version", vcs.Get().String()).
		Msg("Welcome to RProc!")

	// Execute the root command
	if err := cmd.Execute(); err != nil {
		// Log based on error type
		switch err.Error() {
		case "processing cancelled":
			// Ensure final messages are written
			log.Info().Msg("Processing cancelled by user, shutting down cleanly")
			os.Exit(0)
		default:
			// Log the error with stack trace for debugging
			log.Error().
				Err(err).
				Str("stack", string(debug.Stack())).
				Msg("Application error")
			os.Exit(1)
		}
	}

	// Log successful completion
	log.Info().Msg("Processing completed successfully")
}
